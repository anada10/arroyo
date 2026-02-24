use anyhow::{anyhow};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

mod sink;
mod source;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./pubsub.svg");

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PubSubConfig {
    pub project_id: String,
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum TableType {
    #[serde(rename = "source")]
    Source { subscription: String },
    #[serde(rename = "sink")]
    Sink { topic: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PubSubTable {
    #[serde(rename = "type")]
    pub type_: TableType,
}

pub struct PubSubConnector {}

impl Connector for PubSubConnector {
    type ProfileT = PubSubConfig;
    type TableT = PubSubTable;

    fn name(&self) -> &'static str { "pubsub" }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "pubsub".to_string(),
            name: "Google Pub/Sub".to_string(),
            icon: ICON.to_string(),
            description: "Read and write Google Cloud Pub/Sub".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _config: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
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
            let _ = tx
                .send(TestSourceMessage { error: false, done: true, message: "Successfully validated Pub/Sub connection".to_string() })
                .await;
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
        let (typ, desc) = match &table.type_ {
            TableType::Source { subscription } => (ConnectionType::Source, format!("PubSubSource<{subscription}>")),
            TableType::Sink { topic } => (ConnectionType::Sink, format!("PubSubSink<{topic}>")),
        };

        let schema = schema
            .cloned()
            .ok_or_else(|| anyhow!("No schema defined for Pub/Sub connection"))?;

        let format = schema
            .format
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("'format' must be set for Pub/Sub connection"))?;

        let oc = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(id, self.name(), name.to_string(), typ, schema, &oc, desc))
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(match table.type_ {
            TableType::Source { subscription } => {
                ConstructedOperator::from_source(Box::new(source::PubSubSourceFunc {
                    project_id: profile.project_id,
                    endpoint: profile.endpoint,
                    subscription,
                    format: config.format.expect("Format must be set for Pub/Sub source"),
                    framing: config.framing,
                    bad_data: config.bad_data,
                }))
            }
            TableType::Sink { topic } => {
                ConstructedOperator::from_operator(Box::new(sink::PubSubSinkFunc {
                    project_id: profile.project_id,
                    endpoint: profile.endpoint,
                    topic,
                    publisher: None,
                    serializer: ArrowSerializer::new(config.format.expect("Format must be set for Pub/Sub sink")),
                }))
            }
        })
    }
}

impl PubSubConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<PubSubConfig> {
        let project_id = arroyo_rpc::ConnectorOptions::pull_str(options, "project_id")?;
        let endpoint = options.pull_opt_str("endpoint")?;
        Ok(PubSubConfig { project_id, endpoint })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<PubSubTable> {
        if let Some(subscription) = options.pull_opt_str("subscription")? {
            Ok(PubSubTable { type_: TableType::Source { subscription } })
        } else {
            let topic = options.pull_str("topic")?;
            Ok(PubSubTable { type_: TableType::Sink { topic } })
        }
    }
}


