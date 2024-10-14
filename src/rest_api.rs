//! See <https://www.umweltbundesamt.de/daten/luft/luftdaten/doc>.

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use reqwest::Client;
use serde::{de::Error, Deserialize, Deserializer};
use serde_tuple::Deserialize_tuple;

const BASE_URL: &str = "https://www.umweltbundesamt.de/api/air_data/v3";

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct Component {
    pub(crate) id: StringU64,
    pub(crate) code: String,
    pub(crate) symbol: String,
    pub(crate) unit: String,
    pub(crate) _translated_name: String,
}

impl Component {
    pub(crate) async fn list(client: &Client) -> Result<Vec<Component>> {
        Ok(client
            .get(format!("{BASE_URL}/components/json"))
            .send()
            .await
            .context("send request")?
            .error_for_status()
            .context("HTTP error")?
            .json::<ComponentResponse>()
            .await
            .context("get JSON")?
            .data
            .into_values()
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct ComponentResponse {
    #[serde(rename = "count")]
    _count: u64,

    #[serde(rename = "indices")]
    _indices: Vec<String>,

    #[serde(flatten)]
    data: HashMap<StringU64, Component>,
}

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct Network {
    pub(crate) id: StringU64,
    pub(crate) code: String,
    pub(crate) translated_name: String,
}

impl Network {
    pub(crate) async fn list(client: &Client) -> Result<Vec<Network>> {
        Ok(client
            .get(format!("{BASE_URL}/networks/json"))
            .send()
            .await
            .context("send request")?
            .error_for_status()
            .context("HTTP error")?
            .json::<NetworkResponse>()
            .await
            .context("get JSON")?
            .data
            .into_values()
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct NetworkResponse {
    data: HashMap<StringU64, Network>,
}

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct Station {
    pub(crate) id: StringU64,
    pub(crate) code: String,
    pub(crate) name: String,
    pub(crate) city: OptionalString,
    pub(crate) synonym: OptionalString,
    pub(crate) date_of_first_activity: NaiveDate,
    pub(crate) date_of_last_activity: Option<NaiveDate>,
    pub(crate) longitude: StringF64,
    pub(crate) latitude: StringF64,
    pub(crate) id_of_network: StringU64,
    pub(crate) id_of_station_setting: StringU64,
    pub(crate) _id_of_station_type: StringU64,
    pub(crate) _code_of_network: String,
    pub(crate) _translated_name_of_network: String,
    pub(crate) _translated_name_of_station_setting: String,
    pub(crate) _translated_short_name_of_station_setting: String,
    pub(crate) _translated_name_of_station_type: String,
    pub(crate) street: OptionalString,
    pub(crate) street_number: OptionalString,
    pub(crate) zip_code: ZipCode,
}

impl Station {
    pub(crate) async fn list(client: &Client) -> Result<Vec<Station>> {
        Ok(client
            .get(format!("{BASE_URL}/stations/json"))
            .send()
            .await
            .context("send request")?
            .error_for_status()
            .context("HTTP error")?
            .json::<StationsResponse>()
            .await
            .context("get JSON")?
            .data
            .into_values()
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct StationsResponse {
    data: HashMap<StringU64, Station>,
}

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct StationSetting {
    pub(crate) id: StringU64,
    pub(crate) translated_name: String,
    pub(crate) translated_short_name: String,
}

impl StationSetting {
    pub(crate) async fn list(client: &Client) -> Result<Vec<StationSetting>> {
        Ok(client
            .get(format!("{BASE_URL}/stationsettings/json"))
            .send()
            .await
            .context("send request")?
            .error_for_status()
            .context("HTTP error")?
            .json::<StationSettingResponse>()
            .await
            .context("get JSON")?
            .data
            .into_values()
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct StationSettingResponse {
    #[serde(rename = "count")]
    _count: u64,

    #[serde(rename = "indices")]
    _indices: Vec<String>,

    #[serde(flatten)]
    data: HashMap<StringU64, StationSetting>,
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
