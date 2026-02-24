use anyhow::{anyhow};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_auth::project::Config as AuthConfig;
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_token::TokenSourceProvider;
use reqwest::header::AUTHORIZATION;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

mod sink;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./bigquery.svg");

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BigQueryConfig {
    pub project_id: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub service_account_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BigQueryTable {
    pub dataset: String,
    pub table: String,
    #[serde(default)]
    pub use_storage_write: Option<bool>,
    #[serde(default)]
    pub insert_id_field: Option<String>,
    #[serde(default)]
    pub max_batch_bytes: Option<i64>,
    #[serde(default)]
    pub max_batch_rows: Option<i64>,
    #[serde(default)]
    pub flush_interval_millis: Option<i64>,
}

pub struct BigQueryConnector {}

impl Connector for BigQueryConnector {
    type ProfileT = BigQueryConfig;
    type TableT = BigQueryTable;

    fn name(&self) -> &'static str { "bigquery" }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "bigquery".to_string(),
            name: "Google BigQuery".to_string(),
            icon: ICON.to_string(),
            description: "Write to Google BigQuery via streaming inserts".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _config: Self::ProfileT, _table: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn test(
        &self,
        _name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            let msg = match test_connection(&config, &table).await {
                Ok(message) => TestSourceMessage {
                    error: false,
                    done: true,
                    message,
                },
                Err(e) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("BigQuery validation failed: {e}"),
                },
            };
            let _ = tx.send(msg).await;
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema.cloned().ok_or_else(|| anyhow!("No schema defined for BigQuery connection"))?;
        let format = schema.format.as_ref().cloned().ok_or_else(|| anyhow!("'format' must be set for BigQuery"))?;
        let oc = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };
        Ok(Connection::new(id, self.name(), name.to_string(), ConnectionType::Sink, schema, &oc, "BigQuerySink".into()))
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(ConstructedOperator::from_operator(Box::new(sink::BigQuerySinkFunc::new(profile, table, config))))
    }
}

impl BigQueryConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<BigQueryConfig> {
        let project_id = options.pull_str("project_id")?;
        let endpoint = options.pull_opt_str("endpoint")?;
        let service_account_json = options.pull_opt_str("service_account_json")?;
        Ok(BigQueryConfig {
            project_id,
            endpoint,
            service_account_json,
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<BigQueryTable> {
        let dataset = options.pull_str("dataset")?;
        let table = options.pull_str("table")?;
        let use_storage_write = options.pull_opt_bool("use_storage_write")?;
        let insert_id_field = options.pull_opt_field("insert_id_field")?;
        let max_batch_bytes = options.pull_opt_i64("max_batch_bytes")?;
        let max_batch_rows = options.pull_opt_i64("max_batch_rows")?;
        let flush_interval_millis = options.pull_opt_i64("flush_interval_millis")?;
        Ok(BigQueryTable { dataset, table, use_storage_write, insert_id_field, max_batch_bytes, max_batch_rows, flush_interval_millis })
    }
}

async fn create_token_provider(
    config: &BigQueryConfig,
) -> anyhow::Result<DefaultTokenSourceProvider> {
    let auth_cfg = AuthConfig {
        audience: None,
        scopes: Some(&["https://www.googleapis.com/auth/bigquery"][..]),
        ..Default::default()
    };

    if let Some(service_account_json) = config.service_account_json.as_deref() {
        if !service_account_json.trim().is_empty() {
            let credentials = CredentialsFile::new_from_str(service_account_json)
                .await
                .map_err(|e| anyhow!("invalid service account JSON: {e}"))?;
            return DefaultTokenSourceProvider::new_with_credentials(auth_cfg, Box::new(credentials))
                .await
                .map_err(|e| anyhow!("failed to initialize auth with service account JSON: {e}"));
        }
    }

    DefaultTokenSourceProvider::new(auth_cfg)
        .await
        .map_err(|e| anyhow!("failed to initialize ADC auth: {e}"))
}

async fn test_connection(config: &BigQueryConfig, table: &BigQueryTable) -> anyhow::Result<String> {
    let provider = create_token_provider(config).await?;
    let token = provider
        .token_source()
        .token()
        .await
        .map_err(|e| anyhow!("failed to fetch BigQuery access token: {e}"))?;

    // During profile-only validation, table fields may not be provided yet.
    if table.dataset.trim().is_empty() || table.table.trim().is_empty() {
        return Ok("Authenticated to BigQuery successfully".into());
    }

    let base = config
        .endpoint
        .clone()
        .unwrap_or_else(|| "https://bigquery.googleapis.com".to_string());
    let url = format!(
        "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}",
        base, config.project_id, table.dataset, table.table
    );

    let response = reqwest::Client::new()
        .get(url)
        .header(AUTHORIZATION, token)
        .send()
        .await
        .map_err(|e| anyhow!("failed to call BigQuery API: {e}"))?;

    if response.status().is_success() {
        Ok(format!(
            "Authenticated and verified table {}.{}",
            table.dataset, table.table
        ))
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!(
            "BigQuery API returned {} while checking table {}.{}: {}",
            status,
            table.dataset,
            table.table,
            body
        ))
    }
}


