use std::sync::{Arc, LazyLock};

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use jiff::{civil::DateTime, tz::TimeZone, Timestamp};

const TZ: &str = "CET";

pub(crate) static ARROW_TZ: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from(TZ));
pub(crate) static JIFF_TZ: LazyLock<TimeZone> =
    LazyLock::new(|| TimeZone::get(TZ).expect("valid timezone"));

pub(crate) fn ts_datatype() -> DataType {
    DataType::Timestamp(TimeUnit::Second, Some(Arc::clone(&ARROW_TZ)))
}

pub(crate) fn dt_to_seconds(dt: DateTime) -> Result<i64> {
    let zoned = dt.to_zoned(JIFF_TZ.clone()).context("invalid timestamp")?;
    Ok(zoned.timestamp().as_second())
}

pub(crate) fn extract_from_scalar(scalar: &ScalarValue) -> Option<Timestamp> {
    let datafusion::scalar::ScalarValue::TimestampSecond(Some(ts), Some(tz)) = scalar else {
        return None;
    };
    if tz.as_ref() != TZ {
        return None;
    }
    Timestamp::from_second(*ts).ok()
}
