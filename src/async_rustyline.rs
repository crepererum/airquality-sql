use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use rustyline::{error::ReadlineError, history::FileHistory, Config, Editor};
use tokio::sync::Mutex;

type MyEditor = Editor<(), FileHistory>;

pub(crate) struct AsyncRustyline {
    inner: Arc<Mutex<MyEditor>>,
    path: Option<PathBuf>,
}

impl AsyncRustyline {
    pub(crate) async fn new() -> Result<Self> {
        let (inner, path) = tokio::task::spawn_blocking(|| {
            let cfg = Config::builder().build();
            let history = FileHistory::new();
            let mut inner = MyEditor::with_history(cfg, history).context("setup rustyline")?;

            let cache_dir =
                ProjectDirs::from("net.crepererum", "crepererum.net", env!("CARGO_PKG_NAME"))
                    .map(|dirs| dirs.cache_dir().to_owned());
            let path = match cache_dir {
                Some(cache_dir) => {
                    std::fs::create_dir_all(&cache_dir).context("create cache dir")?;

                    let path = cache_dir.join("history.txt");
                    if std::fs::exists(&path).ok().unwrap_or_default() {
                        inner.load_history(&path).context("load history")?;
                    }
                    Some(path)
                }
                None => None,
            };

            Ok((inner, path)) as Result<_, anyhow::Error>
        })
        .await
        .context("tokio spawn")??;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            path,
        })
    }

    pub(crate) async fn read_line(&self) -> Result<Option<String>> {
        let mut inner = Arc::clone(&self.inner).lock_owned().await;
        let path = self.path.clone();

        let rl_res = tokio::task::spawn_blocking(move || {
            let s = inner.readline(">> ")?;
            inner.add_history_entry(&s)?;
            if let Some(path) = path {
                inner.save_history(&path)?;
            }
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
