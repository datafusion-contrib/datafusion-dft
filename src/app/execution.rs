use color_eyre::eyre::Result;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use super::config::DataFusionConfig;

pub struct ExecutionContext {
    pub session_ctx: SessionContext,
    pub config: DataFusionConfig,
    /// Sends WSMessages to DataFusion.  We have a separate sender for this, rather than piggy
    /// backing on `app_event_tx` because we are only concerned about `WSMessage` and not other
    /// `AppEvent`'s.
    pub cancellation_token: CancellationToken,
}

impl ExecutionContext {
    pub fn new(config: DataFusionConfig) -> Self {
        let cfg = SessionConfig::default()
            .with_batch_size(1)
            .with_information_schema(true);

        let session_ctx = SessionContext::new_with_config(cfg);
        let cancellation_token = CancellationToken::new();

        Self {
            config,
            session_ctx,
            cancellation_token,
        }
    }

    pub fn create_tables(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn execute_stream_sql(&mut self, query: &str) -> Result<()> {
        let df = self.session_ctx.sql(query).await.unwrap();
        let physical_plan = df.create_physical_plan().await.unwrap();
        // We use small batch size because web socket stream comes in small increments (each
        // message usually only has at most a few records).
        let stream_cfg = SessionConfig::default().with_batch_size(self.config.stream_batch_size);
        let stream_task_ctx = TaskContext::default().with_session_config(stream_cfg);
        let mut stream = execute_stream(physical_plan, stream_task_ctx.into()).unwrap();

        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch.unwrap();
            let d = pretty_format_batches(&[batch]).unwrap();
            println!("{}", d);
        }
        Ok(())
    }

    pub async fn show_catalog(&self) -> Result<()> {
        let tables = self.session_ctx.sql("SHOW tables").await?.collect().await?;
        let formatted = pretty_format_batches(&tables).unwrap();
        println!("{}", formatted);
        Ok(())
    }
}
