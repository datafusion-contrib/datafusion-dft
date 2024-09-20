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

use datafusion::error::DataFusionError;
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use log::info;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    bytes_scanned: usize,
    // exec_metrics: Vec<ExecMetrics>,
}

impl ExecutionStats {
    pub fn bytes_scanned(&self) -> usize {
        self.bytes_scanned
    }
}

#[derive(Default)]
struct PlanVisitor {
    total_bytes_scanned: usize,
    // exec_metrics: Vec<ExecMetrics>,
}

impl From<PlanVisitor> for ExecutionStats {
    fn from(value: PlanVisitor) -> Self {
        Self {
            bytes_scanned: value.total_bytes_scanned,
        }
    }
}

impl ExecutionPlanVisitor for PlanVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> color_eyre::Result<bool, Self::Error> {
        match plan.metrics() {
            Some(metrics) => match metrics.sum_by_name("bytes_scanned") {
                Some(bytes_scanned) => {
                    info!("Adding {} to total_bytes_scanned", bytes_scanned.as_usize());
                    self.total_bytes_scanned += bytes_scanned.as_usize();
                }
                None => {
                    info!("No bytes_scanned for {}", plan.name())
                }
            },
            None => {
                info!("No MetricsSet for {}", plan.name())
            }
        }
        Ok(true)
    }
}

pub fn collect_plan_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionStats> {
    let mut visitor = PlanVisitor::default();
    if visit_execution_plan(plan.as_ref(), &mut visitor).is_ok() {
        Some(visitor.into())
    } else {
        None
    }
}
