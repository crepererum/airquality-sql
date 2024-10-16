use std::{any::Any, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{
        BooleanBuilder, Float64Builder, ListBuilder, RecordBatch, StringBuilder, StructBuilder,
        TimestampSecondBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, Session, TableProvider},
    catalog_common::MemorySchemaProvider,
    common::DFSchema,
    datasource::TableType,
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    logical_expr::{utils::conjunction, TableProviderFilterPushDown},
    physical_expr::ExprBoundaries,
    physical_plan::{
        memory::MemoryExec, stream::RecordBatchStreamAdapter, streaming::StreamingTableExec,
        ExecutionPlan,
    },
    prelude::Expr,
};
use futures::StreamExt;
use jiff::{civil::DateTime, Timestamp, ToSpan};
use reqwest::Client;

use crate::{
    rest_api::{
        list_air_quality, AirQuality, AirQualityComponent, AirQualityRow, Component, Network,
        Station, StationSetting, StationType,
    },
    stream::SimplePartitionStream,
    time::{dt_to_seconds, extract_from_scalar, ts_datatype, ARROW_TZ, JIFF_TZ},
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

impl AirQualityTable {
    const COL_DATE_START: &str = "date_start";
    const COL_DATE_END: &str = "date_end";

    fn component_struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::UInt64, false),
            Field::new("index", DataType::UInt64, false),
            Field::new("y_value", DataType::Float64, false),
        ])
    }

    fn component_field() -> Arc<Field> {
        Arc::new(Field::new(
            "component",
            DataType::Struct(Self::component_struct_fields()),
            false,
        ))
    }

    fn analyze_date_range(
        &self,
        state: &dyn Session,
        filters: &[Expr],
        schema: &SchemaRef,
    ) -> Result<(Timestamp, Timestamp)> {
        // set default range
        let default = "2019-01-01T09:00:00"
            .parse::<DateTime>()
            .expect("default should parse")
            .to_zoned(JIFF_TZ.clone())
            .expect("default should be in correct zone")
            .timestamp();
        let mut ts_start = default;
        let mut ts_end = default;

        let df_schema = DFSchema::try_from(Arc::clone(schema))?;

        let Some(expr) = conjunction(filters.iter().cloned()) else {
            return Ok((ts_start, ts_end));
        };
        let expr = state.create_physical_expr(expr, &df_schema)?;

        let boundaries = ExprBoundaries::try_new_unbounded(schema)?;
        let context = datafusion::physical_expr::AnalysisContext::new(boundaries);

        let context = datafusion::physical_expr::analyze(&expr, context, schema)?;
        if let Some(ts) = context
            .boundaries
            .iter()
            .find(|boundary| boundary.column.name() == Self::COL_DATE_START)
            .and_then(|boundaries| extract_from_scalar(boundaries.interval.lower()))
        {
            ts_start = ts;
        }
        if let Some(boundaries) = context
            .boundaries
            .iter()
            .find(|boundary| boundary.column.name() == Self::COL_DATE_END)
        {
            if let Some(ts) = extract_from_scalar(boundaries.interval.upper()) {
                ts_end = ts;
            }
        }

        Ok((ts_start, ts_end))
    }

    fn rest_to_arrow(data: AirQuality, schema: &SchemaRef) -> Result<RecordBatch> {
        let mut station_id_builder = UInt64Builder::new();
        let mut date_start_builder =
            TimestampSecondBuilder::new().with_timezone(Arc::clone(&ARROW_TZ));
        let mut date_end_builder =
            TimestampSecondBuilder::new().with_timezone(Arc::clone(&ARROW_TZ));
        let mut index_builder = UInt64Builder::new();
        let mut incomplete_builder = BooleanBuilder::new();
        let mut components_builder = ListBuilder::new(StructBuilder::from_fields(
            Self::component_struct_fields(),
            1024,
        ))
        .with_field(Self::component_field());
        for (station_id, sub) in data {
            for (date_start, row) in sub {
                let AirQualityRow {
                    date_end,
                    index,
                    incomplete,
                    components,
                } = row;

                station_id_builder.append_value(station_id.into());
                date_start_builder.append_value(
                    dt_to_seconds(date_start.into())
                        .map_err(|e| DataFusionError::External(e.into()))?,
                );
                date_end_builder.append_value(
                    dt_to_seconds(date_end.into())
                        .map_err(|e| DataFusionError::External(e.into()))?,
                );
                index_builder.append_value(index);
                incomplete_builder.append_value(incomplete.into());

                for component in components {
                    let AirQualityComponent {
                        id,
                        value,
                        index,
                        y_value,
                    } = component;

                    let struct_builder = components_builder.values();
                    struct_builder
                        .field_builder::<UInt64Builder>(0)
                        .expect("correct type")
                        .append_value(id);
                    struct_builder
                        .field_builder::<UInt64Builder>(1)
                        .expect("correct type")
                        .append_value(value);
                    struct_builder
                        .field_builder::<UInt64Builder>(2)
                        .expect("correct type")
                        .append_value(index);
                    struct_builder
                        .field_builder::<Float64Builder>(3)
                        .expect("correct type")
                        .append_value(y_value.into());
                    struct_builder.append(true);
                }
                components_builder.append(true);
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(station_id_builder.finish()),
                Arc::new(date_start_builder.finish()),
                Arc::new(date_end_builder.finish()),
                Arc::new(index_builder.finish()),
                Arc::new(incomplete_builder.finish()),
                Arc::new(components_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for AirQualityTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new([
            Arc::new(Field::new("station_id", DataType::UInt64, false)),
            Arc::new(Field::new(Self::COL_DATE_START, ts_datatype(), false)),
            Arc::new(Field::new(Self::COL_DATE_END, ts_datatype(), false)),
            Arc::new(Field::new("index", DataType::UInt64, false)),
            Arc::new(Field::new("incomplete", DataType::Boolean, false)),
            Arc::new(Field::new(
                "components",
                DataType::List(Self::component_field()),
                false,
            )),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let (ts_from, ts_to) = self.analyze_date_range(state, filters, &schema)?;

        let schema_captured = Arc::clone(&schema);
        let client = self.client.clone();
        let stream_fn = move |_| {
            let schema_captured_stream = Arc::clone(&schema_captured);
            let client_captured = client.clone();
            let stream = futures::stream::unfold(Some(ts_from), move |ts_from| async move {
                let ts_from = ts_from?;
                let ts_to_step = ts_from.saturating_add(1.hour()).min(ts_to);
                Some((
                    (ts_from, ts_to_step),
                    (ts_to_step < ts_to).then_some(ts_to_step),
                ))
            })
            .map(move |(ts_from, ts_to)| {
                let client = client_captured.clone();
                let schema = Arc::clone(&schema_captured_stream);
                async move {
                    let data = list_air_quality(&client, ts_from, ts_to)
                        .await
                        .context("list air quality")
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    let batch = Self::rest_to_arrow(data, &schema)?;
                    Ok(batch)
                }
            })
            .buffer_unordered(10);
            let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema_captured), stream);
            Box::pin(stream) as SendableRecordBatchStream
        };

        let exec = StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(SimplePartitionStream::new(schema, stream_fn))],
            projection,
            [],
            false,
            limit,
        )?;
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
