use std::{any::Any, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{
        BooleanBuilder, Float64Builder, RecordBatch, StringBuilder, TimestampSecondBuilder,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, Session, TableProvider},
    catalog_common::MemorySchemaProvider,
    datasource::TableType,
    error::{DataFusionError, Result},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    prelude::Expr,
};
use reqwest::Client;

use crate::{
    rest_api::{
        list_air_quality, AirQualityRow, Component, Network, Station, StationSetting, StationType,
    },
    time::{dt_to_seconds, ts_datatype, ARROW_TZ},
};

pub(crate) fn schema_provider() -> Arc<dyn SchemaProvider> {
    let client = Client::new();

    let provider = MemorySchemaProvider::new();
    provider
        .register_table(
            "air_quality".to_owned(),
            Arc::new(AirQualityTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    provider
        .register_table(
            "components".to_owned(),
            Arc::new(ComponentsTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    provider
        .register_table(
            "networks".to_owned(),
            Arc::new(NetworksTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    provider
        .register_table(
            "stations".to_owned(),
            Arc::new(StationsTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    provider
        .register_table(
            "station_settings".to_owned(),
            Arc::new(StationSettingsTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    provider
        .register_table(
            "station_types".to_owned(),
            Arc::new(StationTypesTable {
                client: client.clone(),
            }),
        )
        .expect("should always for for mem provider");
    Arc::new(provider)
}

struct AirQualityTable {
    client: Client,
}

#[async_trait]
impl TableProvider for AirQualityTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("station_id", DataType::UInt64, false)),
            Arc::new(Field::new("date_start", ts_datatype(), false)),
            Arc::new(Field::new("date_end", ts_datatype(), false)),
            Arc::new(Field::new("index", DataType::UInt64, false)),
            Arc::new(Field::new("incomplete", DataType::Boolean, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let res = list_air_quality(&self.client)
            .await
            .context("list air quality")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut station_id_builder = UInt64Builder::new();
        let mut date_start_builder =
            TimestampSecondBuilder::new().with_timezone(Arc::clone(&ARROW_TZ));
        let mut date_end_builder =
            TimestampSecondBuilder::new().with_timezone(Arc::clone(&ARROW_TZ));
        let mut index_builder = UInt64Builder::new();
        let mut incomplete_builder = BooleanBuilder::new();
        for (station_id, sub) in res {
            for (date_start, row) in sub {
                let AirQualityRow {
                    date_end,
                    index,
                    incomplete,
                    ..
                } = row;

                station_id_builder.append_value(station_id.into());
                date_start_builder.append_value(
                    dt_to_seconds(date_start).map_err(|e| DataFusionError::External(e.into()))?,
                );
                date_end_builder.append_value(
                    dt_to_seconds(date_end).map_err(|e| DataFusionError::External(e.into()))?,
                );
                index_builder.append_value(index);
                incomplete_builder.append_value(incomplete.into());
            }
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(station_id_builder.finish()),
                Arc::new(date_start_builder.finish()),
                Arc::new(date_end_builder.finish()),
                Arc::new(index_builder.finish()),
                Arc::new(incomplete_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

struct ComponentsTable {
    client: Client,
}

#[async_trait]
impl TableProvider for ComponentsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("code", DataType::Utf8, false)),
            Arc::new(Field::new("symbol", DataType::Utf8, false)),
            Arc::new(Field::new("unit", DataType::Utf8, false)),
            Arc::new(Field::new("translated_name", DataType::Utf8, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let components = Component::list(&self.client)
            .await
            .context("list components")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut id_builder = UInt64Builder::with_capacity(components.len());
        let mut code_builder = StringBuilder::new();
        let mut symbol_builder = StringBuilder::new();
        let mut unit_builder = StringBuilder::new();
        let mut translated_name_builder = StringBuilder::new();
        for component in components {
            let Component {
                id,
                code,
                symbol,
                unit,
                translated_name,
            } = component;
            id_builder.append_value(id.into());
            code_builder.append_value(code);
            symbol_builder.append_value(symbol);
            unit_builder.append_value(unit);
            translated_name_builder.append_value(translated_name);
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(code_builder.finish()),
                Arc::new(symbol_builder.finish()),
                Arc::new(unit_builder.finish()),
                Arc::new(translated_name_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

struct NetworksTable {
    client: Client,
}

#[async_trait]
impl TableProvider for NetworksTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("code", DataType::Utf8, false)),
            Arc::new(Field::new("translated_name", DataType::Utf8, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let networks = Network::list(&self.client)
            .await
            .context("list networks")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut id_builder = UInt64Builder::with_capacity(networks.len());
        let mut code_builder = StringBuilder::new();
        let mut translated_name_builder = StringBuilder::new();
        for network in networks {
            let Network {
                id,
                code,
                translated_name,
            } = network;
            id_builder.append_value(id.into());
            code_builder.append_value(code);
            translated_name_builder.append_value(translated_name);
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(code_builder.finish()),
                Arc::new(translated_name_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

struct StationsTable {
    client: Client,
}

#[async_trait]
impl TableProvider for StationsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("code", DataType::Utf8, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("synonym", DataType::Utf8, true)),
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(Field::new("street", DataType::Utf8, true)),
            Arc::new(Field::new("street_number", DataType::Utf8, true)),
            Arc::new(Field::new("zip_code", DataType::Utf8, true)),
            Arc::new(Field::new("longitude", DataType::Float64, false)),
            Arc::new(Field::new("latitude", DataType::Float64, false)),
            Arc::new(Field::new("activity_from", ts_datatype(), false)),
            Arc::new(Field::new("activity_to", ts_datatype(), true)),
            Arc::new(Field::new("id_of_network", DataType::UInt64, false)),
            Arc::new(Field::new("id_of_station_setting", DataType::UInt64, false)),
            Arc::new(Field::new("id_of_station_type", DataType::UInt64, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stations = Station::list(&self.client)
            .await
            .context("list stations")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut id_builder = UInt64Builder::with_capacity(stations.len());
        let mut code_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut city_builder = StringBuilder::new();
        let mut synonym_builder = StringBuilder::new();
        let mut activity_from_builder = TimestampSecondBuilder::with_capacity(stations.len())
            .with_timezone(Arc::clone(&ARROW_TZ));
        let mut activity_to_builder = TimestampSecondBuilder::with_capacity(stations.len())
            .with_timezone(Arc::clone(&ARROW_TZ));
        let mut longitude_builder = Float64Builder::with_capacity(stations.len());
        let mut latitude_builder = Float64Builder::with_capacity(stations.len());
        let mut street_builder = StringBuilder::new();
        let mut street_number_builder = StringBuilder::new();
        let mut zip_code_builder = StringBuilder::new();
        let mut id_of_network_builder = UInt64Builder::with_capacity(stations.len());
        let mut id_of_station_setting_builder = UInt64Builder::with_capacity(stations.len());
        let mut id_of_station_type_builder = UInt64Builder::with_capacity(stations.len());
        for station in stations {
            let Station {
                id,
                code,
                name,
                city,
                synonym,
                activity_from,
                activity_to,
                longitude,
                latitude,
                street,
                street_number,
                zip_code,
                id_of_network,
                id_of_station_setting,
                id_of_station_type,
                ..
            } = station;
            id_builder.append_value(id.into());
            code_builder.append_value(code);
            name_builder.append_value(name);
            city_builder.append_option(Option::<String>::from(city));
            synonym_builder.append_option(Option::<String>::from(synonym));
            activity_from_builder.append_value(
                dt_to_seconds(activity_from).map_err(|e| DataFusionError::External(e.into()))?,
            );
            activity_to_builder.append_option(
                activity_to
                    .map(|dt| dt_to_seconds(dt).map_err(|e| DataFusionError::External(e.into())))
                    .transpose()?,
            );
            longitude_builder.append_value(longitude.into());
            latitude_builder.append_value(latitude.into());
            street_builder.append_option(Option::<String>::from(street));
            street_number_builder.append_option(Option::<String>::from(street_number));
            zip_code_builder.append_option(Option::<String>::from(zip_code));
            id_of_network_builder.append_value(id_of_network.into());
            id_of_station_setting_builder.append_value(id_of_station_setting.into());
            id_of_station_type_builder.append_value(id_of_station_type.into());
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(code_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(synonym_builder.finish()),
                Arc::new(city_builder.finish()),
                Arc::new(street_builder.finish()),
                Arc::new(street_number_builder.finish()),
                Arc::new(zip_code_builder.finish()),
                Arc::new(longitude_builder.finish()),
                Arc::new(latitude_builder.finish()),
                Arc::new(activity_from_builder.finish()),
                Arc::new(activity_to_builder.finish()),
                Arc::new(id_of_network_builder.finish()),
                Arc::new(id_of_station_setting_builder.finish()),
                Arc::new(id_of_station_type_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

struct StationSettingsTable {
    client: Client,
}

#[async_trait]
impl TableProvider for StationSettingsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("translated_name", DataType::Utf8, false)),
            Arc::new(Field::new("translated_short_name", DataType::Utf8, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let settings = StationSetting::list(&self.client)
            .await
            .context("list station settings")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut id_builder = UInt64Builder::with_capacity(settings.len());
        let mut translated_name_builder = StringBuilder::new();
        let mut translated_short_name_builder = StringBuilder::new();
        for setting in settings {
            let StationSetting {
                id,
                translated_name,
                translated_short_name,
            } = setting;
            id_builder.append_value(id.into());
            translated_name_builder.append_value(translated_name);
            translated_short_name_builder.append_value(translated_short_name);
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(translated_name_builder.finish()),
                Arc::new(translated_short_name_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

struct StationTypesTable {
    client: Client,
}

#[async_trait]
impl TableProvider for StationTypesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("translated_name", DataType::Utf8, false)),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stypes = StationType::list(&self.client)
            .await
            .context("list station types")
            .map_err(|e| DataFusionError::External(e.into()))?;

        let mut id_builder = UInt64Builder::with_capacity(stypes.len());
        let mut translated_name_builder = StringBuilder::new();
        for stype in stypes {
            let StationType {
                id,
                translated_name,
            } = stype;
            id_builder.append_value(id.into());
            translated_name_builder.append_value(translated_name);
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(translated_name_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}
