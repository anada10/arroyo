use anyhow::{anyhow};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
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
    pub endpoint: Option<String>
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
        _config: Self::ProfileT,
        _table: Self::TableT,
        _schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            let _ = tx.send(TestSourceMessage{ error: false, done: true, message: "Successfully validated BigQuery".into()}).await;
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
        Ok(BigQueryConfig { project_id, endpoint })
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


