use crate::input::*;
use crate::operations::*;
use crate::output::*;
use crate::steps::*;
use core::panic;
use std::fs;
use std::io;
use toml;

/// Parse a TOML file into a Pipeline
/// Read a TOML file and parse it according to a Pipeline struct
pub fn parse_toml(path: &str) -> Result<Pipeline, io::Error> {
    // let filename = "./data/test.toml";
    let file_content: String = fs::read_to_string(path)?;
    let toml_content: Pipeline = toml::from_str(&file_content)?;
    //return
    Ok(toml_content)
}

/// Validates the Pipeline
/// Validates the Pipeline struct according to the available set of operations
/// This is an import aspect since the steps properties do not have a fixed contract that can be verified by the serializer
pub fn validate_pipeline(pipeline: &Pipeline) {
    let output = Vec::from(OUTPUT_OP);
    let input = Vec::from(INPUT_OP);
    let operations = Vec::from(OPERATIONS_OP);
    // The general part is validated by the parser, but now we need to validate the operations
    let all = [input, operations, output].concat();

    // Validate if all operations exist
    for step in &pipeline.steps {
        assert!(
            all.contains(&step.operation.as_str()),
            "{} is not a valid operation",
            &step.operation.as_str()
        );
        match step.operation.as_str() {
            "read_csv" => ReadCsvStep::validate(step),
            "preview" => PreviewStep::validate(step),
            "filter_eq" => FilterEqualStep::validate(step),
            "filter_gt_eq" => FilterGreaterEqStep::validate(step),
            "filter_lt" => FilterSmallerStep::validate(step),
            "filter_lt_eq" => FilterSmallerEqStep::validate(step),
            "filter_gt" => FilterGreaterStep::validate(step),
            "filter_isin" => FilterIsInStep::validate(step),
            "filter_contains" => FilterContainsStep::validate(step),
            "remove_na" => RemoveNAStep::validate(step),
            "rename" => RenameStep::validate(step),
            "recode" => RecodeStep::validate(step),
            "select" => SelectColumnsStep::validate(step),
            "cast" => CastStep::validate(step),
            "anonymize" => AnonymizeStep::validate(step),
            "pivot" => PivotStep::validate(step),
            "unique" => UniqueStep::validate(step),
            _ => panic!("Step {} is not a valid operation", step.operation.as_str()),
        }
    }
}