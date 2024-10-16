use std::sync::LazyLock;

use jiff::{civil::DateTime, ToSpan};
use regex::Regex;
use serde::{de::Error, Deserializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IntBool(bool);

impl<'de> serde::Deserialize<'de> for IntBool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = i128::deserialize(deserializer)?;
        match i {
            0 => Ok(Self(false)),
            1 => Ok(Self(true)),
            _ => Err(D::Error::custom(format!("invalid bool: {i}"))),
        }
    }
}

impl From<IntBool> for bool {
    fn from(value: IntBool) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct StringU64(u64);

impl<'de> serde::Deserialize<'de> for StringU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let n = s
            .parse()
            .map_err(|e| D::Error::custom(format!("invalid number: {e}")))?;
        Ok(Self(n))
    }
}

impl From<StringU64> for u64 {
    fn from(value: StringU64) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StringF64(f64);

impl<'de> serde::Deserialize<'de> for StringF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let n = s
            .parse()
            .map_err(|e| D::Error::custom(format!("invalid number: {e}")))?;
        Ok(Self(n))
    }
}

impl From<StringF64> for f64 {
    fn from(value: StringF64) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OptionalString(Option<String>);

impl<'de> serde::Deserialize<'de> for OptionalString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self((!s.is_empty()).then_some(s)))
    }
}

impl From<OptionalString> for Option<String> {
    fn from(value: OptionalString) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ZipCode(Option<String>);

impl<'de> serde::Deserialize<'de> for ZipCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self((!s.is_empty() && s != "00000").then_some(s)))
    }
}

impl From<ZipCode> for Option<String> {
    fn from(value: ZipCode) -> Self {
        value.0
    }
}

static TS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^(?<year>[0-9]{4})-(?<month>[0-9]{2})-(?<day>[0-9]{2}) (?<hour>[0-9]{2}):(?<minute>[0-9]{2}):(?<second>[0-9]{2})$"#).unwrap()
});

/// Whoever encodes "midnight" as `24:00:00`...
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct WeirdTimestamp(DateTime);

impl WeirdTimestamp {
    fn parse_number<'de, D>(cap: &regex::Captures<'_>, what: &'static str) -> Result<u16, D::Error>
    where
        D: Deserializer<'de>,
    {
        let n: u16 = cap
            .name(what)
            .expect("group should exist")
            .as_str()
            .parse()
            .map_err(|e| D::Error::custom(format!("invalid {what}: {e}")))?;
        Ok(n)
    }
}

impl<'de> serde::Deserialize<'de> for WeirdTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let m = TS_REGEX
            .captures(&s)
            .ok_or_else(|| D::Error::custom(format!("invalid timestamp: {s}")))?;
        let year = Self::parse_number::<D>(&m, "year")?;
        let month = Self::parse_number::<D>(&m, "month")?;
        let day = Self::parse_number::<D>(&m, "day")?;
        let hour = Self::parse_number::<D>(&m, "hour")?;
        let minute = Self::parse_number::<D>(&m, "minute")?;
        let second = Self::parse_number::<D>(&m, "second")?;

        let (hour, h24) = if hour == 24 {
            (23, true)
        } else {
            (hour, false)
        };
        let s = format!("{year}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
        let dt: DateTime = s
            .parse()
            .map_err(|e| D::Error::custom(format!("invalid timestamp: {e}")))?;
        let dt = if h24 { dt.saturating_add(1.hour()) } else { dt };
        Ok(Self(dt))
    }
}

impl From<WeirdTimestamp> for DateTime {
    fn from(value: WeirdTimestamp) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intbool() {
        assert_de_ok(r#"0"#, IntBool(false));
        assert_de_ok(r#"1"#, IntBool(true));
        assert_de_err::<IntBool>(r#"3"#, "invalid bool: 3");
        assert_de_err::<IntBool>(r#"false"#, "invalid number at line 1 column 1");
    }

    #[test]
    fn test_string_u64() {
        assert_de_ok(r#""0""#, StringU64(0));
        assert_de_ok(r#""42""#, StringU64(42));
        assert_de_err::<StringU64>(r#""foo""#, "invalid number: invalid digit found in string");
        assert_de_err::<StringU64>(
            r#"0"#,
            "invalid type: integer `0`, expected a string at line 1 column 1",
        );
    }

    #[test]
    fn test_string_f64() {
        assert_de_ok(r#""0""#, StringF64(0.0));
        assert_de_ok(r#""4.2""#, StringF64(4.2));
        assert_de_err::<StringF64>(r#""foo""#, "invalid number: invalid float literal");
        assert_de_err::<StringF64>(
            r#"0"#,
            "invalid type: integer `0`, expected a string at line 1 column 1",
        );
    }

    #[test]
    fn test_optional_string() {
        assert_de_ok(r#""""#, OptionalString(None));
        assert_de_ok(r#""foo""#, OptionalString(Some("foo".to_owned())));
        assert_de_err::<OptionalString>(
            r#"0"#,
            "invalid type: integer `0`, expected a string at line 1 column 1",
        );
    }

    #[test]
    fn test_zip_code() {
        assert_de_ok(r#""""#, ZipCode(None));
        assert_de_ok(r#""00000""#, ZipCode(None));
        assert_de_ok(r#""01234""#, ZipCode(Some("01234".to_owned())));
        assert_de_err::<ZipCode>(
            r#"0"#,
            "invalid type: integer `0`, expected a string at line 1 column 1",
        );
    }

    #[test]
    fn test_weird_timestamp() {
        assert_de_ok(
            r#""2024-01-02 03:04:05""#,
            WeirdTimestamp("2024-01-02 03:04:05".parse().unwrap()),
        );
        assert_de_ok(
            r#""2024-01-02 24:04:05""#,
            WeirdTimestamp("2024-01-03 00:04:05".parse().unwrap()),
        );
        assert_de_err::<WeirdTimestamp>(
            r#"0"#,
            "invalid type: integer `0`, expected a string at line 1 column 1",
        );
        assert_de_err::<WeirdTimestamp>(r#""foo""#, "invalid timestamp: foo");
        assert_de_err::<WeirdTimestamp>(
            r#""2024-40-02 03:04:05""#,
            "invalid timestamp: failed to parse month in date \"2024-40-02 03:04:05\": month is not valid: parameter 'month' with value 40 is not in the required range of 1..=12",
        );
    }

    #[track_caller]
    fn assert_de_ok<T>(s: &'static str, expected: T)
    where
        T: PartialEq + std::fmt::Debug + serde::de::Deserialize<'static>,
    {
        let actual = serde_json::from_str::<T>(s).unwrap();
        assert_eq!(actual, expected);
    }

    #[track_caller]
    fn assert_de_err<T>(s: &'static str, expected: &'static str)
    where
        T: std::fmt::Debug + serde::de::Deserialize<'static>,
    {
        let actual = serde_json::from_str::<T>(s).unwrap_err().to_string();
        assert_eq!(actual, expected);
    }
}
