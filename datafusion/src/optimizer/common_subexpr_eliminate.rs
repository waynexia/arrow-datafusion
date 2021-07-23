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

//! Eliminate common sub-expression.

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils::{self, ExprAddrToId, ExprSet};

pub struct CommonSubexprEliminate {}

impl OptimizerRule for CommonSubexprEliminate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        todo!()
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

fn gather_identifier(
    plan: &LogicalPlan,
    expr_set: &mut ExprSet,
    addr_map: &mut ExprAddrToId,
) -> Result<()> {
    match plan {
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => {
            for e in expr {
                // let mut desc = String::new();
                utils::expr_to_identifier(e, expr_set, addr_map)?;
                // println!("{:?}", desc);
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            // let mut desc = String::new();
            utils::expr_to_identifier(predicate, expr_set, addr_map)?;
            // println!("{:?}", desc);
        }
        LogicalPlan::Window {
            input,
            window_expr,
            schema,
        } => {
            for e in window_expr {
                // let mut desc = String::new();
                utils::expr_to_identifier(e, expr_set, addr_map)?;
                // println!("{:?}", desc);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        } => {
            for e in group_expr {
                // let mut desc = String::new();
                utils::expr_to_identifier(e, expr_set, addr_map)?;
                // println!("{:?}", desc);
            }

            for e in aggr_expr {
                // let mut desc = String::new();
                utils::expr_to_identifier(e, expr_set, addr_map)?;
                // println!("{:?}", desc);
            }
        }
        LogicalPlan::Sort { expr, input } => {}
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Repartition { .. }
        | LogicalPlan::Union { .. }
        | LogicalPlan::TableScan { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Extension { .. }
        | LogicalPlan::Shared { .. } => {}
    }

    let inputs = plan.inputs();
    for input in inputs {
        gather_identifier(input, expr_set, addr_map)?;
    }

    Ok(())
}

fn generate_shared_plan(expr_set: &mut ExprSet) {
    for (expr, _, count, plan) in expr_set.values_mut() {
        if *count <= 1 {
            continue;
        }

        let shared_plan = LogicalPlan::Shared{ inputs: (), expr: (), schema: () }
    }
}

// csp -> common sub-expression plan
fn place_csp() {
    todo!()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::{binary_expr, col, lit, sum, LogicalPlanBuilder, Operator};
    use crate::test::*;

    #[test]
    #[ignore]
    fn dev_driver_tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![],
                vec![
                    sum(binary_expr(
                        col("a"),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Minus, col("b")),
                    )),
                    sum(binary_expr(
                        binary_expr(
                            col("a"),
                            Operator::Multiply,
                            binary_expr(lit(1), Operator::Minus, col("b")),
                        ),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Plus, col("c")),
                    )),
                ],
            )?
            .build()?;

        let mut expr_set = ExprSet::new();
        let mut addr_map = ExprAddrToId::new();
        // gather_identifier(&plan, &mut expr_set)?;
        println!("{:#?}", expr_set);

        Ok(())
    }
}
