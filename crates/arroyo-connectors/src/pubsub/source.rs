use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::StopMode;
use async_trait::async_trait;
use futures::StreamExt;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::MessageStream;
use std::time::SystemTime;
use tokio::select;

pub struct PubSubSourceFunc {
    pub project_id: String,
    pub endpoint: Option<String>,
    pub subscription: String,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

impl PubSubSourceFunc {
    async fn new_stream(&self) -> DataflowResult<MessageStream> {
        let mut cfg = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|e| connector_err!(External, WithBackoff, "failed to configure Pub/Sub auth: {e}"))?;
        if let Some(e) = &self.endpoint {
            cfg.endpoint = e.clone();
        }
        cfg.project_id = Some(self.project_id.clone());
        let client = Client::new(cfg)
            .await
            .map_err(|e| connector_err!(External, WithBackoff, "failed to initialize Pub/Sub client: {e}"))?;
        let sub = client.subscription(&self.subscription);
        sub.subscribe(None)
            .await
            .map_err(|e| connector_err!(External, WithBackoff, "failed to subscribe to '{}': {e}", self.subscription))
    }
}

#[async_trait]
impl SourceOperator for PubSubSourceFunc {
    fn name(&self) -> String {
        format!("pubsub-source-{}", self.subscription)
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );
        let mut stream = self.new_stream().await?;
        loop {
            select! {
                maybe_msg = stream.next() => {
                    match maybe_msg {
                        Some(msg) => {
                            let payload = msg.message.data.as_slice();
                            let ts = SystemTime::now();
                            // best effort ack after successful deserialization
                            if let Err(e) = collector.deserialize_slice(payload, ts, None).await {
                                ctx.report_nonfatal_error(e).await;
                                // nack
                                msg.nack().await.ok();
                            } else {
                                if collector.should_flush() {
                                    if let Err(e) = collector.flush_buffer().await {
                                        ctx.report_nonfatal_error(e).await;
                                    }
                                }
                                msg.ack().await.ok();
                            }
                        }
                        None => {
                            return Ok(SourceFinishType::Graceful);
                        }
                    }
                }
                control = ctx.control_rx.recv() => {
                    match control {
                        Some(arroyo_rpc::ControlMessage::Checkpoint(c)) => {
                            if self.start_checkpoint(c, ctx, collector).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                        Some(arroyo_rpc::ControlMessage::Stop { mode }) => {
                            return Ok(match mode {
                                StopMode::Graceful => SourceFinishType::Graceful,
                                StopMode::Immediate => SourceFinishType::Immediate
                            });
                        }
                        Some(arroyo_rpc::ControlMessage::Commit { .. }) => {}
                        Some(arroyo_rpc::ControlMessage::LoadCompacted { compacted }) => { ctx.load_compacted(compacted).await; }
                        Some(arroyo_rpc::ControlMessage::NoOp) => {}
                        None => {}
                    }
                }
            }
        }
    }
}


