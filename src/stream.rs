use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::streaming::PartitionStream,
};

pub(crate) struct SimplePartitionStream {
    schema: SchemaRef,
    stream_fn: Box<dyn Fn(Arc<TaskContext>) -> SendableRecordBatchStream + Send + Sync>,
}

impl SimplePartitionStream {
    pub(crate) fn new<F>(schema: SchemaRef, stream_fn: F) -> Self
    where
        F: Fn(Arc<TaskContext>) -> SendableRecordBatchStream + Send + Sync + 'static,
    {
        Self {
            schema,
            stream_fn: Box::new(stream_fn),
        }
    }
}

impl PartitionStream for SimplePartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        (self.stream_fn)(ctx)
    }
}
