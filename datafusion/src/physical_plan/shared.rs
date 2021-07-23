// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `SharedExec` represents some data can be shared by others

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use super::{RecordBatchStream, SendableRecordBatchStream};
use async_trait::async_trait;

use futures::stream::Stream;
use futures::stream::StreamExt;

/// Execution Plan for a shared expr.
#[derive(Debug)]
pub struct SharedExec {
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,

    prepared_data: Option<RecordBatch>,
}
