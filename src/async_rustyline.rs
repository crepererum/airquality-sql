use std::sync::Arc;

use anyhow::{Context, Result};
use rustyline::{error::ReadlineError, history::MemHistory, Config, Editor};
use tokio::sync::Mutex;

type MyEditor = Editor<(), MemHistory>;

pub(crate) struct AsyncRustyline {
    inner: Arc<Mutex<MyEditor>>,
}

impl AsyncRustyline {
    pub(crate) async fn new() -> Result<Self> {
        let inner = tokio::task::spawn_blocking(|| {
            let cfg = Config::builder().build();
            let history = MemHistory::new();
            MyEditor::with_history(cfg, history)
        })
        .await
        .context("tokio spawn")?
        .context("setup rustyline")?;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) async fn read_line(&self) -> Result<Option<String>> {
        let mut inner = Arc::clone(&self.inner).lock_owned().await;

        let rl_res = tokio::task::spawn_blocking(move || {
            let s = inner.readline(">> ")?;
            inner.add_history_entry(&s)?;
            Ok(s)
        })
        .await
        .context("tokio spawn")?;

        match rl_res {
            Ok(s) => Ok(Some(s)),
            Err(ReadlineError::Eof | ReadlineError::Interrupted) => Ok(None),
            Err(e) => Err(e).context("read line"),
        }
    }
}
