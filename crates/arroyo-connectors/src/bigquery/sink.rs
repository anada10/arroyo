use arrow::array::{Array, AsArray, LargeStringArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use google_cloud_auth::project::Config as AuthConfig;
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_token::TokenSourceProvider;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::BigQueryTable;

#[derive(Clone)]
struct FlushConfig {
    max_bytes: usize,
    max_rows: usize,
    max_age: Duration,
}

impl FlushConfig {
    fn new(table: &BigQueryTable) -> Self {
        Self {
            max_bytes: table.max_batch_bytes.unwrap_or(8_000_000) as usize, // under 10MB
            max_rows: table.max_batch_rows.unwrap_or(5_000) as usize,
            max_age: Duration::from_millis(table.flush_interval_millis.unwrap_or(500) as u64),
        }
    }
}

struct BatchBuffer {
    rows: Vec<(Option<String>, Vec<u8>)>,
    size: usize,
    last_flush: Instant,
}

impl BatchBuffer {
    fn new() -> Self { Self { rows: Vec::new(), size: 0, last_flush: Instant::now() } }
    fn add(&mut self, insert_id: Option<String>, row: Vec<u8>) { self.size += row.len() + insert_id.as_ref().map(|s| s.len()).unwrap_or(0); self.rows.push((insert_id, row)); }
    fn should_flush(&self, cfg: &FlushConfig) -> bool { self.rows.len() >= cfg.max_rows || self.size >= cfg.max_bytes || self.last_flush.elapsed() >= cfg.max_age }
    fn mark_flushed(&mut self) { self.rows.clear(); self.size = 0; self.last_flush = Instant::now(); }
}

pub struct BigQuerySinkFunc {
    project_id: String,
    dataset: String,
    table: String,
    insert_id_field: Option<String>,
    client: Client,
    token_provider: Option<DefaultTokenSourceProvider>,
    endpoint: Option<String>,
    serializer: ArrowSerializer,
    buffer: BatchBuffer,
    flush_config: FlushConfig,
}

impl BigQuerySinkFunc {
    pub fn new(cfg: super::BigQueryConfig, table: super::BigQueryTable, op: arroyo_rpc::OperatorConfig) -> Self {
        let flush_config = FlushConfig::new(&table);
        Self {
            project_id: cfg.project_id,
            dataset: table.dataset,
            table: table.table,
            insert_id_field: table.insert_id_field.clone(),
            client: Client::new(),
            token_provider: None,
            endpoint: cfg.endpoint,
            serializer: ArrowSerializer::new(op.format.expect("format required")),
            buffer: BatchBuffer::new(),
            flush_config,
        }
    }

    fn make_url(&self) -> String {
        let base = self.endpoint.clone().unwrap_or_else(|| "https://bigquery.googleapis.com".to_string());
        format!("{}/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll", base, self.project_id, self.dataset, self.table)
    }

    async fn flush_with_retries(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if self.buffer.rows.is_empty() {
            return Ok(());
        }
        let url = self.make_url();
        #[derive(Serialize)]
        struct Row<'a> {
            #[serde(rename = "insertId", skip_serializing_if = "Option::is_none")]
            insert_id: Option<&'a str>,
            json: serde_json::Value,
        }
        #[derive(Serialize)]
        struct Payload<'a> { kind: &'static str, rows: Vec<Row<'a>> }

        // Serialize rows to JSON objects expected by BigQuery insertAll.
        let mut payload_rows = Vec::with_capacity(self.buffer.rows.len());
        for (insert_id, v) in &self.buffer.rows {
            // v is bytes of a single record in selected format; for BigQuery streaming, JSON is expected.
            // We assume JSON output format was chosen.
            let value: serde_json::Value = match serde_json::from_slice(v) {
                Ok(j) => j,
                Err(e) => {
                    ctx.report_error("BigQuery JSON error", format!("{e:?}")).await;
                    continue;
                }
            };
            payload_rows.push(Row {
                insert_id: insert_id.as_deref(),
                json: value,
            });
        }
        let payload = Payload { kind: "bigquery#tableDataInsertAllRequest", rows: payload_rows };
        if payload.rows.is_empty() {
            self.buffer.mark_flushed();
            return Ok(());
        }

        let mut attempts = 0;
        loop {
            let provider = self.token_provider.as_ref().ok_or_else(|| {
                connector_err!(Internal, NoRetry, "BigQuery token provider is not initialized")
            })?;
            let token = provider
                .token_source()
                .token()
                .await
                .map_err(|e| connector_err!(External, WithBackoff, "failed to fetch BigQuery token: {e}"))?;
            let resp = self.client.post(&url)
                .header(CONTENT_TYPE, "application/json")
                .header(AUTHORIZATION, token)
                .body(serde_json::to_vec(&payload).unwrap())
                .send().await;

            match resp {
                Ok(r) => {
                    if r.status().is_success() {
                        self.buffer.mark_flushed();
                        return Ok(());
                    } else {
                        let status = r.status();
                        let text = r.text().await.unwrap_or_default();
                        ctx.report_error("BigQuery insertAll error", format!("status={} body={}", status, text)).await;
                    }
                }
                Err(e) => {
                    ctx.report_error("BigQuery request error", format!("{e:?}")).await;
                }
            }

            attempts += 1;
            if attempts >= 20 {
                return Err(connector_err!(External, WithBackoff, "exhausted retries writing to BigQuery '{}.{}'", self.dataset, self.table));
            }
            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(2_000))).await;
        }
    }
}

enum InsertIdColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

#[async_trait]
impl ArrowOperator for BigQuerySinkFunc {
    fn name(&self) -> String { format!("bigquery-sink-{}.{}", self.dataset, self.table) }

    fn tables(&self) -> HashMap<String, TableConfig> { HashMap::new() }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) -> DataflowResult<()> {
        let auth_cfg = AuthConfig {
            audience: None,
            scopes: Some(&["https://www.googleapis.com/auth/bigquery"][..]),
            ..Default::default()
        };
        self.token_provider = Some(DefaultTokenSourceProvider::new(auth_cfg).await.map_err(|e| {
            connector_err!(External, WithBackoff, "failed to initialize BigQuery auth: {e}")
        })?);
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // For high throughput, buffer and flush by size/time/row count.
        let values = self.serializer.serialize(&batch);
        let insert_idx = self
            .insert_id_field
            .as_ref()
            .and_then(|name| ctx.in_schemas[0].schema.fields.iter().position(|f| f.name() == name));
        let insert_col = if let Some(idx) = insert_idx {
            match batch.column(idx).data_type() {
                DataType::Utf8 => Some(InsertIdColumn::Utf8(batch.column(idx).as_string::<i32>())),
                DataType::LargeUtf8 => Some(InsertIdColumn::LargeUtf8(batch.column(idx).as_string::<i64>())),
                _ => {
                    ctx.report_error(
                        "BigQuery insert_id_field must be string",
                        format!("field '{}' has unsupported type {:?}", self.insert_id_field.as_deref().unwrap_or(""), batch.column(idx).data_type()),
                    )
                    .await;
                    None
                }
            }
        } else {
            None
        };

        for (i, row) in values.enumerate() {
            let insert_id = match &insert_col {
                Some(InsertIdColumn::Utf8(col)) if !col.is_null(i) => Some(col.value(i).to_string()),
                Some(InsertIdColumn::LargeUtf8(col)) if !col.is_null(i) => {
                    Some(col.value(i).to_string())
                }
                _ => None,
            };
            self.buffer.add(insert_id, row);
            if self.buffer.should_flush(&self.flush_config) {
                self.flush_with_retries(ctx).await?;
            }
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush_with_retries(ctx).await
    }
}


