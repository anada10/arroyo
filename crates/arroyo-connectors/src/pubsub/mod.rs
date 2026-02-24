use anyhow::{anyhow};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::{Client, ClientConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
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
    #[serde(default)]
    pub service_account_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum TableType {
    Source { subscription: String },
    Sink { topic: String },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PubSubTable {
    #[serde(rename = "type")]
    pub type_: TableType,
}

impl<'de> Deserialize<'de> for PubSubTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Nested {
                #[serde(rename = "type")]
                type_: TableType,
            },
            LegacySource {
                #[serde(rename = "type")]
                type_: String,
                subscription: String,
            },
            LegacySink {
                #[serde(rename = "type")]
                type_: String,
                topic: String,
            },
        }

        match Repr::deserialize(deserializer)? {
            Repr::Nested { type_ } => Ok(Self { type_ }),
            Repr::LegacySource {
                type_,
                subscription,
            } if type_.eq_ignore_ascii_case("source") => Ok(Self {
                type_: TableType::Source { subscription },
            }),
            Repr::LegacySink { type_, topic } if type_.eq_ignore_ascii_case("sink") => Ok(Self {
                type_: TableType::Sink { topic },
            }),
            _ => Err(serde::de::Error::custom(
                "invalid Pub/Sub table config; expected source/subscription or sink/topic",
            )),
        }
    }
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
                    message: format!("Pub/Sub validation failed: {e}"),
                },
            };
            let _ = tx
                .send(msg)
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
                    service_account_json: profile.service_account_json,
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
                    service_account_json: profile.service_account_json,
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
        let service_account_json = options.pull_opt_str("service_account_json")?;
        Ok(PubSubConfig {
            project_id,
            endpoint,
            service_account_json,
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<PubSubTable> {
        if let Some(subscription) = options.pull_opt_str("subscription")? {
            return Ok(PubSubTable {
                type_: TableType::Source { subscription },
            });
        }
        if let Some(subscription) = options.pull_opt_str("type.subscription")? {
            return Ok(PubSubTable {
                type_: TableType::Source { subscription },
            });
        }
        if let Some(subscription) = options.pull_opt_str("source.subscription")? {
            return Ok(PubSubTable {
                type_: TableType::Source { subscription },
            });
        }

        if let Some(topic) = options.pull_opt_str("topic")? {
            return Ok(PubSubTable {
                type_: TableType::Sink { topic },
            });
        }
        if let Some(topic) = options.pull_opt_str("type.topic")? {
            return Ok(PubSubTable {
                type_: TableType::Sink { topic },
            });
        }
        if let Some(topic) = options.pull_opt_str("sink.topic")? {
            return Ok(PubSubTable {
                type_: TableType::Sink { topic },
            });
        }

        Err(anyhow!(
            "Pub/Sub table config must include either subscription (source) or topic (sink)"
        ))
    }
}

async fn pubsub_client(config: &PubSubConfig) -> anyhow::Result<Client> {
    let mut cfg = ClientConfig::default();
    if let Some(endpoint) = &config.endpoint {
        cfg.endpoint = endpoint.clone();
    }
    cfg.project_id = Some(config.project_id.clone());

    let cfg = if let Some(service_account_json) = config.service_account_json.as_deref() {
        if !service_account_json.trim().is_empty() {
            let credentials = CredentialsFile::new_from_str(service_account_json)
                .await
                .map_err(|e| anyhow!("invalid service account JSON: {e}"))?;
            cfg.with_credentials(credentials)
                .await
                .map_err(|e| anyhow!("failed to configure Pub/Sub auth with service account JSON: {e}"))?
        } else {
            cfg.with_auth()
                .await
                .map_err(|e| anyhow!("failed to configure Pub/Sub auth via ADC: {e}"))?
        }
    } else {
        cfg.with_auth()
            .await
            .map_err(|e| anyhow!("failed to configure Pub/Sub auth via ADC: {e}"))?
    };

    Client::new(cfg).await.map_err(|e| anyhow!("failed to initialize Pub/Sub client: {e}"))
}

async fn test_connection(config: &PubSubConfig, table: &PubSubTable) -> anyhow::Result<String> {
    let client = pubsub_client(config).await?;
    match &table.type_ {
        TableType::Source { subscription } => {
            let sub = client.subscription(subscription);
            let exists = sub
                .exists(None)
                .await
                .map_err(|e| anyhow!("failed to check subscription '{}': {e}", subscription))?;
            if !exists {
                return Err(anyhow!("subscription '{}' does not exist", subscription));
            }
            Ok(format!("Authenticated and verified subscription '{subscription}'"))
        }
        TableType::Sink { topic } => {
            let topic_ref = client.topic(topic);
            let exists = topic_ref
                .exists(None)
                .await
                .map_err(|e| anyhow!("failed to check topic '{}': {e}", topic))?;
            if !exists {
                return Err(anyhow!("topic '{}' does not exist", topic));
            }
            Ok(format!("Authenticated and verified topic '{topic}'"))
        }
    }
}


