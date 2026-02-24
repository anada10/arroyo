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
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_auth::project::Config as AuthConfig;
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_token::TokenSourceProvider;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
    dataset: Option<String>,
    table: Option<String>,
    dataset_field: Option<String>,
    table_field: Option<String>,
    insert_id_field: Option<String>,
    client: Client,
    token_provider: Option<DefaultTokenSourceProvider>,
    service_account_json: Option<String>,
    endpoint: Option<String>,
    serializer: ArrowSerializer,
    buffers: HashMap<Destination, BatchBuffer>,
    flush_config: FlushConfig,
}

impl BigQuerySinkFunc {
    pub fn new(cfg: super::BigQueryConfig, table: super::BigQueryTable, op: arroyo_rpc::OperatorConfig) -> Self {
        let flush_config = FlushConfig::new(&table);
        Self {
            project_id: cfg.project_id,
            dataset: table.dataset,
            table: table.table,
            dataset_field: table.dataset_field,
            table_field: table.table_field,
            insert_id_field: table.insert_id_field.clone(),
            client: Client::new(),
            token_provider: None,
            service_account_json: cfg.service_account_json,
            endpoint: cfg.endpoint,
            serializer: ArrowSerializer::new(op.format.expect("format required")),
            buffers: HashMap::new(),
            flush_config,
        }
    }

    fn make_url(&self, destination: &Destination) -> String {
        let base = self.endpoint.clone().unwrap_or_else(|| "https://bigquery.googleapis.com".to_string());
        format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll",
            base, self.project_id, destination.dataset, destination.table
        )
    }

    async fn flush_destination_with_retries(
        &self,
        destination: &Destination,
        buffer: &mut BatchBuffer,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        if buffer.rows.is_empty() {
            return Ok(());
        }
        let url = self.make_url(destination);
        #[derive(Serialize)]
        struct Row<'a> {
            #[serde(rename = "insertId", skip_serializing_if = "Option::is_none")]
            insert_id: Option<&'a str>,
            json: serde_json::Value,
        }
        #[derive(Serialize)]
        struct Payload<'a> { kind: &'static str, rows: Vec<Row<'a>> }

        // Serialize rows to JSON objects expected by BigQuery insertAll.
        let mut payload_rows = Vec::with_capacity(buffer.rows.len());
        for (insert_id, v) in &buffer.rows {
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
            buffer.mark_flushed();
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
                        buffer.mark_flushed();
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
                return Err(connector_err!(
                    External,
                    WithBackoff,
                    "exhausted retries writing to BigQuery '{}.{}'",
                    destination.dataset,
                    destination.table
                ));
            }
            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(2_000))).await;
        }
    }

    fn resolve_destination(&self, row: &serde_json::Value) -> Result<Destination, String> {
        let row_obj = row
            .as_object()
            .ok_or_else(|| "record must serialize to a JSON object for BigQuery sink".to_string())?;

        let dataset = if let Some(field) = &self.dataset_field {
            match row_obj.get(field).and_then(|v| v.as_str()) {
                Some(v) if !v.trim().is_empty() => v.to_string(),
                _ => {
                    return Err(format!(
                        "dataset_field '{}' is missing or not a non-empty string",
                        field
                    ))
                }
            }
        } else {
            self.dataset
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "dataset must be configured (static or dataset_field)".to_string())?
                .to_string()
        };

        let table = if let Some(field) = &self.table_field {
            match row_obj.get(field).and_then(|v| v.as_str()) {
                Some(v) if !v.trim().is_empty() => v.to_string(),
                _ => return Err(format!("table_field '{}' is missing or not a non-empty string", field)),
            }
        } else {
            self.table
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "table must be configured (static or table_field)".to_string())?
                .to_string()
        };

        Ok(Destination { dataset, table })
    }

    async fn flush_destination(
        &mut self,
        destination: Destination,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        if let Some(mut buffer) = self.buffers.remove(&destination) {
            self.flush_destination_with_retries(&destination, &mut buffer, ctx)
                .await?;
            if !buffer.rows.is_empty() {
                self.buffers.insert(destination, buffer);
            }
        }
        Ok(())
    }
}

enum InsertIdColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Destination {
    dataset: String,
    table: String,
}

#[async_trait]
impl ArrowOperator for BigQuerySinkFunc {
    fn name(&self) -> String {
        format!(
            "bigquery-sink-{}.{}",
            self.dataset.as_deref().unwrap_or("<dynamic>"),
            self.table.as_deref().unwrap_or("<dynamic>")
        )
    }

    fn tables(&self) -> HashMap<String, TableConfig> { HashMap::new() }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) -> DataflowResult<()> {
        let dataset_static = self
            .dataset
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        let table_static = self
            .table
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        if self.dataset_field.is_none() && !dataset_static {
            return Err(connector_err!(
                User,
                NoRetry,
                "BigQuery sink requires either 'dataset' or 'dataset_field'"
            ));
        }
        if self.table_field.is_none() && !table_static {
            return Err(connector_err!(
                User,
                NoRetry,
                "BigQuery sink requires either 'table' or 'table_field'"
            ));
        }

        let auth_cfg = AuthConfig {
            audience: None,
            scopes: Some(&["https://www.googleapis.com/auth/bigquery"][..]),
            ..Default::default()
        };

        let provider = if let Some(service_account_json) = self.service_account_json.as_deref() {
            if !service_account_json.trim().is_empty() {
                let credentials = CredentialsFile::new_from_str(service_account_json)
                    .await
                    .map_err(|e| {
                        connector_err!(
                            User,
                            NoRetry,
                            "invalid BigQuery service account JSON: {e}"
                        )
                    })?;
                DefaultTokenSourceProvider::new_with_credentials(auth_cfg, Box::new(credentials))
                    .await
                    .map_err(|e| {
                        connector_err!(
                            External,
                            WithBackoff,
                            "failed to initialize BigQuery auth with service account JSON: {e}"
                        )
                    })?
            } else {
                DefaultTokenSourceProvider::new(auth_cfg).await.map_err(|e| {
                    connector_err!(
                        External,
                        WithBackoff,
                        "failed to initialize BigQuery auth via ADC: {e}"
                    )
                })?
            }
        } else {
            DefaultTokenSourceProvider::new(auth_cfg).await.map_err(|e| {
                connector_err!(
                    External,
                    WithBackoff,
                    "failed to initialize BigQuery auth via ADC: {e}"
                )
            })?
        };
        self.token_provider = Some(provider);
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
        let mut destinations_to_flush: HashSet<Destination> = HashSet::new();
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
            let row_value: serde_json::Value = match serde_json::from_slice(&row) {
                Ok(v) => v,
                Err(e) => {
                    ctx.report_error("BigQuery JSON error", format!("{e:?}")).await;
                    continue;
                }
            };
            let destination = match self.resolve_destination(&row_value) {
                Ok(d) => d,
                Err(e) => {
                    ctx.report_error("BigQuery destination error", e).await;
                    continue;
                }
            };
            let insert_id = match &insert_col {
                Some(InsertIdColumn::Utf8(col)) if !col.is_null(i) => Some(col.value(i).to_string()),
                Some(InsertIdColumn::LargeUtf8(col)) if !col.is_null(i) => {
                    Some(col.value(i).to_string())
                }
                _ => None,
            };
            let buffer = self
                .buffers
                .entry(destination.clone())
                .or_insert_with(BatchBuffer::new);
            buffer.add(insert_id, row);
            if buffer.should_flush(&self.flush_config) {
                destinations_to_flush.insert(destination);
            }
        }

        for destination in destinations_to_flush {
            self.flush_destination(destination, ctx).await?;
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let destinations: Vec<Destination> = self.buffers.keys().cloned().collect();
        for destination in destinations {
            self.flush_destination(destination, ctx).await?;
        }
        Ok(())
    }
}


