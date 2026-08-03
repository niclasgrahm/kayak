use anyhow::Result;
use std::sync::Arc;

use crate::BuildCtx;

pub mod dummy;
pub mod nats;
pub mod streamer;

pub trait BuildInput {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn InputSource>>;
}

// pub type MessageBatch = Vec<Arc<serde_json::Value>>;
pub use streamer_core::MessageBatch;

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Arc<MessageBatch>>;
}

pub enum BufferKind {
    Static { size: usize },
    Tumbling { window_seconds: usize },
    // Sliding {
    //     window_seconds: usize,
    //     step_seconds: usize,
    // },
}

pub struct Buffered {
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<Arc<MessageBatch>>>,
    kind: BufferKind,
}

impl Buffered {
    #[must_use] 
    pub fn new(inner: Box<dyn InputSource>, kind: BufferKind) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut inner = inner;
            loop {
                let res = inner.next().await;
                let failed = res.is_err();
                if tx.send(res).await.is_err() || failed {
                    break;
                }
            }
        });
        Self { rx, kind }
    }
}

#[async_trait::async_trait]
impl InputSource for Buffered {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        match self.kind {
            BufferKind::Static { size } => {
                let mut out = Vec::new();
                for _ in 0..size {
                    match self.rx.recv().await {
                        Some(Ok(batch)) => out.extend(batch.iter().cloned()),
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                }
                Ok(Arc::new(out))
            }
            BufferKind::Tumbling { window_seconds } => {
                let mut out = Vec::new();
                let deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(window_seconds as u64);
                let mut window_closed = std::pin::pin!(tokio::time::sleep_until(deadline));

                loop {
                    tokio::select! {
                        maybe = self.rx.recv() => match maybe {
                            Some(Ok(batch)) => out.extend(batch.iter().cloned()),
                            Some(Err(e)) => return Err(e),
                            None => return Ok(Arc::new(out)),
                        },
                        () = &mut window_closed => return Ok(Arc::new(out)),
                    }
                }
            } // BufferKind::Tumbling { window_seconds } => {
              //     let start = std::time::Instant::now();
              //     while start.elapsed().as_secs() < window_seconds as u64 {
              //         let inner_batch = self.inner.next().await?;
              //         batch.extend(inner_batch.iter().cloned());
              //     }
              // }
              // BufferKind::Sliding {
              //     window_seconds,
              //     step_seconds,
              // } => {
              //     let mut last_step = std::time::Instant::now();
              //     let start = std::time::Instant::now();
              //     while start.elapsed().as_secs() < window_seconds as u64 {
              //         let inner_batch = self.inner.next().await?;
              //         batch.extend(inner_batch.iter().cloned());
              //         if last_step.elapsed().as_secs() >= step_seconds as u64 {
              //             last_step = std::time::Instant::now();
              //             batch.clear();
              //         }
              //     }
              // }
        }
    }
}
