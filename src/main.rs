mod async_rustyline;
mod rest_api;
mod schema;
mod stream;
mod time;

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{array::RecordBatch, datatypes::SchemaRef, util::pretty::pretty_format_batches};
use async_rustyline::AsyncRustyline;
use datafusion::{
    catalog_common::{CatalogProvider, MemoryCatalogProvider},
    prelude::{SessionConfig, SessionContext},
};
use schema::schema_provider;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("airquality", "airquality");
    let ctx = SessionContext::new_with_config(cfg);

    let catalog = MemoryCatalogProvider::new();
    catalog
        .register_schema("airquality", schema_provider())
        .context("register schema")?;
    ctx.register_catalog("airquality", Arc::new(catalog));

    let rl = AsyncRustyline::new()
        .await
        .context("init async rustyline")?;

    loop {
        let Some(sql) = rl.read_line().await.context("read line")? else {
            break;
        };

        match process_sql(&sql, &ctx).await {
            Ok(res) => {
                println!("{}", res);
            }
            Err(e) => {
                println!("{e:?}");
            }
        }
    }

    Ok(())
}

async fn process_sql(sql: &str, ctx: &SessionContext) -> Result<String> {
    let df = ctx.sql(sql).await.context("execute query")?;
    let schema = SchemaRef::from(df.schema().clone());
    let mut batches = df.collect().await.context("collect results")?;

    // add empty batch for headers
    batches.push(RecordBatch::new_empty(schema));

    let s = pretty_format_batches(&batches).context("fmt")?.to_string();
    Ok(s)
}
