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
