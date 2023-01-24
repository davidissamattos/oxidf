// oxidf computation of the pipeline
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use crate::input::*;
use crate::operations::*;
use crate::output::*;
use crate::steps::*;
use crate::merge::*;
use polars::prelude::*;

///Execute a pipeline
/// Arguments:
/// * a reference to a pipeline struct
/// * a boolean indicating if the messages will be shown
pub fn compute_pipeline(pipeline: &Pipeline, messages: bool) {
    println!(
        "Running pipeline: {}, version {}",
        pipeline.general.name, pipeline.general.version
    );
    //TODO: fix fake initialization
    let mut df: LazyFrame = df![
    "A"   => [1, 2],
    "B"   => ["AA", "BB"],
    ]
    .unwrap()
    .lazy();
    //Loop over all pipeline steps
    for (i, step) in pipeline.steps.iter().enumerate() {
        if messages {
            println!("Step {}: {}", i, step.operation);
        }
        if i == 0 {}
        let operation = step.operation.as_str();
        match operation {
            //Input
            "read_csv" => {
                let s = ReadCsvStep::from(step);
                df = s.execute().unwrap();
            }
            //Operations
            "rename" => {
                let s: RenameStep = RenameStep::from(step, df);
                df = s.execute().unwrap();
            }
            "select" => {
                let s: SelectColumnsStep = SelectColumnsStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_eq" => {
                let s: FilterEqualStep = FilterEqualStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_gt" => {
                let s: FilterGreaterStep = FilterGreaterStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_gt_eq" => {
                let s: FilterGreaterEqStep = FilterGreaterEqStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_lt" => {
                let s: FilterSmallerStep = FilterSmallerStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_lt_eq" => {
                let s: FilterSmallerEqStep = FilterSmallerEqStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_isin" => {
                let s: FilterIsInStep = FilterIsInStep::from(step, df);
                df = s.execute().unwrap();
            }
            "filter_contains" => {
                let s: FilterContainsStep = FilterContainsStep::from(step, df);
                df = s.execute().unwrap();
            }
            "remove_na" => {
                let s: RemoveNAStep = RemoveNAStep::from(step, df);
                df = s.execute().unwrap();
            }
            "recode" => {
                let s: RecodeStep = RecodeStep::from(step, df);
                df = s.execute().unwrap();
            }
            "cast" => {
                let s: CastStep = CastStep::from(step, df);
                df = s.execute().unwrap();
            }
            "anonymize" => {
                let s: AnonymizeStep = AnonymizeStep::from(step, df);
                df = s.execute().unwrap();
            }
            "pivot" => {
                let s: PivotStep = PivotStep::from(step, df);
                df = s.execute().unwrap();
            }
            "unique" => {
                let s: UniqueStep = UniqueStep::from(step, df);
                df = s.execute().unwrap();
            }
            //Output
            "preview" => {
                let s: PreviewStep = PreviewStep::from(step, df);
                df = s.execute().unwrap();
            }

            _ => {
                panic!("Error computing the steps. Step {} failed", operation);
            }
        }
    }
}
