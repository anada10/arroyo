use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::TableConfig;
use async_trait::async_trait;
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use std::collections::HashMap;

pub struct PubSubSinkFunc {
    pub project_id: String,
    pub endpoint: Option<String>,
    pub service_account_json: Option<String>,
    pub topic: String,
    pub publisher: Option<Publisher>,
    pub serializer: ArrowSerializer,
}

impl PubSubSinkFunc {
    async fn publisher(&mut self) -> DataflowResult<&mut Publisher> {
        if self.publisher.is_none() {
            let mut cfg = ClientConfig::default();
            if let Some(e) = &self.endpoint {
                cfg.endpoint = e.clone();
            }
            cfg.project_id = Some(self.project_id.clone());
            let cfg = if let Some(service_account_json) = self.service_account_json.as_deref() {
                if !service_account_json.trim().is_empty() {
                    let credentials = CredentialsFile::new_from_str(service_account_json)
                        .await
                        .map_err(|e| connector_err!(User, NoRetry, "invalid Pub/Sub service account JSON: {e}"))?;
                    cfg.with_credentials(credentials).await.map_err(|e| {
                        connector_err!(
                            External,
                            WithBackoff,
                            "failed to configure Pub/Sub auth with service account JSON: {e}"
                        )
                    })?
                } else {
                    cfg.with_auth().await.map_err(|e| {
                        connector_err!(External, WithBackoff, "failed to configure Pub/Sub auth: {e}")
                    })?
                }
            } else {
                cfg.with_auth().await.map_err(|e| {
                    connector_err!(External, WithBackoff, "failed to configure Pub/Sub auth: {e}")
                })?
            };
            let client = Client::new(cfg)
                .await
                .map_err(|e| connector_err!(External, WithBackoff, "failed to initialize Pub/Sub client: {e}"))?;
            let topic = client.topic(&self.topic);
            self.publisher = Some(topic.new_publisher(None));
        }
        Ok(self.publisher.as_mut().expect("publisher initialized above"))
    }
}

#[async_trait]
impl ArrowOperator for PubSubSinkFunc {
    fn name(&self) -> String { format!("pubsub-publisher-{}", self.topic) }

    fn tables(&self) -> HashMap<String, TableConfig> { HashMap::new() }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) -> DataflowResult<()> {
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let payloads: Vec<Vec<u8>> = self.serializer.serialize(&batch).collect();
        for v in payloads {
            let msg = PubsubMessage { data: v.into(), ..Default::default() };
            let awaiter = self.publisher().await?.publish(msg).await;
            if let Err(e) = awaiter.get().await {
                ctx.report_error("Could not write to Pub/Sub", format!("{e:?}")).await;
                return Err(connector_err!(External, WithBackoff, "failed to write to Pub/Sub topic '{}': {e}", self.topic));
            }
        }
        Ok(())
    }
}


