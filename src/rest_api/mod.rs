//! See <https://www.umweltbundesamt.de/daten/luft/luftdaten/doc>.

pub(crate) mod utils;

use std::{collections::HashMap, marker::PhantomData};

use anyhow::{Context, Result};
use jiff::civil::DateTime;
use reqwest::Client;
use serde::{
    de::{Error, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_tuple::Deserialize_tuple;

use utils::{IntBool, OptionalString, StringF64, StringU64, ZipCode};

const BASE_URL: &str = "https://www.umweltbundesamt.de/api/air_data/v3";

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct AirQualityComponent {
    pub(crate) id: u64,
    pub(crate) value: u64,
    pub(crate) index: u64,
    pub(crate) y_value: StringF64,
}

#[derive(Debug)]
pub(crate) struct AirQualityRow {
    pub(crate) date_end: DateTime,
    pub(crate) index: u64,
    pub(crate) incomplete: IntBool,
    pub(crate) components: Vec<AirQualityComponent>,
}

impl<'de> serde::Deserialize<'de> for AirQualityRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// See <https://github.com/serde-rs/serde/issues/1337#issuecomment-404239049>.
        struct PrefixVisitor(PhantomData<(DateTime, u64, IntBool, AirQualityComponent)>);

        impl<'de> Visitor<'de> for PrefixVisitor {
            type Value = AirQualityRow;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let date_end = seq
                    .next_element()?
                    .ok_or_else(|| Error::invalid_length(0, &self))?;
                let index = seq
                    .next_element()?
                    .ok_or_else(|| Error::invalid_length(1, &self))?;
                let incomplete = seq
                    .next_element()?
                    .ok_or_else(|| Error::invalid_length(2, &self))?;
                let components = Vec::<AirQualityComponent>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?;
                Ok(AirQualityRow {
                    date_end,
                    incomplete,
                    index,
                    components,
                })
            }
        }

        deserializer.deserialize_seq(PrefixVisitor(PhantomData))
    }
}

pub(crate) type AirQuality = HashMap<StringU64, HashMap<DateTime, AirQualityRow>>;

pub(crate) async fn list_air_quality(client: &Client) -> Result<AirQuality> {
    Ok(client
        .get(format!("{BASE_URL}/airquality/json"))
        .query(&[
            ("date_from", "2019-01-01"),
            ("time_from", "9"),
            ("date_to", "2019-01-01"),
            ("time_to", "9"),
        ])
        .send()
        .await
        .context("send request")?
        .error_for_status()
        .context("HTTP error")?
        .json::<AirQualityResponse>()
        .await
        .context("get JSON")?
        .data)
}

#[derive(Debug, Deserialize)]
struct AirQualityResponse {
    data: AirQuality,
}

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct Component {
    pub(crate) id: StringU64,
    pub(crate) code: String,
    pub(crate) symbol: String,
    pub(crate) unit: String,
    pub(crate) translated_name: String,
}

impl Component {
    pub(crate) async fn list(client: &Client) -> Result<Vec<Self>> {
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
    pub(crate) async fn list(client: &Client) -> Result<Vec<Self>> {
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
    pub(crate) activity_from: DateTime,
    pub(crate) activity_to: Option<DateTime>,
    pub(crate) longitude: StringF64,
    pub(crate) latitude: StringF64,
    pub(crate) id_of_network: StringU64,
    pub(crate) id_of_station_setting: StringU64,
    pub(crate) id_of_station_type: StringU64,
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
    pub(crate) async fn list(client: &Client) -> Result<Vec<Self>> {
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
    pub(crate) async fn list(client: &Client) -> Result<Vec<Self>> {
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

#[derive(Debug, Deserialize_tuple)]
pub(crate) struct StationType {
    pub(crate) id: StringU64,
    pub(crate) translated_name: String,
}

impl StationType {
    pub(crate) async fn list(client: &Client) -> Result<Vec<Self>> {
        Ok(client
            .get(format!("{BASE_URL}/stationtypes/json"))
            .send()
            .await
            .context("send request")?
            .error_for_status()
            .context("HTTP error")?
            .json::<StationTypeResponse>()
            .await
            .context("get JSON")?
            .data
            .into_values()
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct StationTypeResponse {
    #[serde(rename = "count")]
    _count: u64,

    #[serde(rename = "indices")]
    _indices: Vec<String>,

    #[serde(flatten)]
    data: HashMap<StringU64, StationType>,
}
