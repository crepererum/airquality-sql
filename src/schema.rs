use std::{any::Any, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{Date64Builder, Float64Builder, RecordBatch, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use datafusion::{
    catalog::{SchemaProvider, Session, TableProvider},
    catalog_common::MemorySchemaProvider,
    datasource::TableType,
    error::{DataFusionError, Result},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    prelude::Expr,
};
use reqwest::Client;

use crate::rest_api::{Component, Network, Station};

pub(crate) fn schema_provider() -> Arc<dyn SchemaProvider> {
    let client = Client::new();

    let provider = MemorySchemaProvider::new();
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
    Arc::new(provider)
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
        for component in components {
            let Component {
                id,
                code,
                symbol,
                unit,
                ..
            } = component;
            id_builder.append_value(id.into());
            code_builder.append_value(code);
            symbol_builder.append_value(symbol);
            unit_builder.append_value(unit);
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(code_builder.finish()),
                Arc::new(symbol_builder.finish()),
                Arc::new(unit_builder.finish()),
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
            .context("list components")
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
            Arc::new(Field::new(
                "date_of_first_activity",
                DataType::Date64,
                false,
            )),
            Arc::new(Field::new("date_of_last_activity", DataType::Date64, true)),
            Arc::new(Field::new("id_of_network", DataType::UInt64, false)),
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
        let mut date_of_first_activity_builder = Date64Builder::with_capacity(stations.len());
        let mut date_of_last_activity_builder = Date64Builder::with_capacity(stations.len());
        let mut longitude_builder = Float64Builder::with_capacity(stations.len());
        let mut latitude_builder = Float64Builder::with_capacity(stations.len());
        let mut street_builder = StringBuilder::new();
        let mut street_number_builder = StringBuilder::new();
        let mut zip_code_builder = StringBuilder::new();
        let mut id_of_network_builder = UInt64Builder::with_capacity(stations.len());
        for station in stations {
            let Station {
                id,
                code,
                name,
                city,
                synonym,
                date_of_first_activity,
                date_of_last_activity,
                longitude,
                latitude,
                street,
                street_number,
                zip_code,
                id_of_network,
                ..
            } = station;
            id_builder.append_value(id.into());
            code_builder.append_value(code);
            name_builder.append_value(name);
            city_builder.append_option(Option::<String>::from(city));
            synonym_builder.append_option(Option::<String>::from(synonym));
            date_of_first_activity_builder.append_value(
                NaiveDateTime::from(date_of_first_activity)
                    .and_utc()
                    .timestamp_millis(),
            );
            date_of_last_activity_builder.append_option(
                date_of_last_activity
                    .map(|day| NaiveDateTime::from(day).and_utc().timestamp_millis()),
            );
            longitude_builder.append_value(longitude.into());
            latitude_builder.append_value(latitude.into());
            street_builder.append_option(Option::<String>::from(street));
            street_number_builder.append_option(Option::<String>::from(street_number));
            zip_code_builder.append_option(Option::<String>::from(zip_code));
            id_of_network_builder.append_value(id_of_network.into());
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
                Arc::new(date_of_first_activity_builder.finish()),
                Arc::new(date_of_last_activity_builder.finish()),
                Arc::new(id_of_network_builder.finish()),
            ],
        )?;
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}
