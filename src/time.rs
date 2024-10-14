use std::sync::{Arc, LazyLock};

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use jiff::{civil::DateTime, tz::TimeZone};

const TZ: &str = "CET";

pub(crate) static ARROW_TZ: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from(TZ));
static JIFF_TZ: LazyLock<TimeZone> = LazyLock::new(|| TimeZone::get(TZ).expect("valid timezone"));

pub(crate) fn ts_datatype() -> DataType {
    DataType::Timestamp(TimeUnit::Second, Some(Arc::clone(&ARROW_TZ)))
}

pub(crate) fn dt_to_seconds(dt: DateTime) -> Result<i64> {
    let zoned = dt.to_zoned(JIFF_TZ.clone()).context("invalid timestamp")?;
    Ok(zoned.timestamp().as_second())
}
