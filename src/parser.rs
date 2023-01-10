use crate::input::*;
use crate::operations::*;
use crate::output::*;
use crate::steps::*;
use std::fs;
use std::io;
use toml;

/// Parse a TOML file into a Pipeline
/// Read a TOML file and parse it according to a Pipeline struct
/// The general structure of the TOML file is defined and explained in the README.md file
/// Arguments: a path to the TOML file
/// Returns: A Result containing the Pipeline or an reading error
/// ```
/// let cars = parse_toml("./tests/toml/cars.toml").unwrap();
/// assert_eq!(cars.general.name, "Cars");
/// ```
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
/// Arguments: a Pipeline struct
/// Returns: a boolean indicating if the Pipeline is valid and a message clarifying the identified issues
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
        if step.operation.as_str().eq("read_csv") {
            ReadCsvStep::validate(step);
        }
        if step.operation.as_str().eq("save_csv") {
            SaveCsvStep::validate(step);
        }
        if step.operation.as_str().eq("preview") {
            PreviewStep::validate(step);
        }
        if step.operation.as_str().eq("filter_eq") {
            FilterEqualStep::validate(step);
        }
        if step.operation.as_str().eq("filter_gt_eq") {
            FilterGreaterEqStep::validate(step);
        }
        if step.operation.as_str().eq("filter_lt") {
            FilterSmallerStep::validate(step);
        }
        if step.operation.as_str().eq("filter_lt_eq") {
            FilterSmallerEqStep::validate(step);
        }
        if step.operation.as_str().eq("filter_gt") {
            FilterGreaterStep::validate(step);
        }
        if step.operation.as_str().eq("filter_isin") {
            FilterIsInStep::validate(step);
        }
        if step.operation.as_str().eq("filter_contains") {
            FilterContainsStep::validate(step);
        }
        if step.operation.as_str().eq("remove_na") {
            RemoveNAStep::validate(step);
        }
        if step.operation.as_str().eq("rename") {
            RenameStep::validate(step);
        }
        if step.operation.as_str().eq("select") {
            SelectColumnsStep::validate(step);
        }
        if step.operation.as_str().eq("anonymize") {
            AnonymizeStep::validate(step);
        }
    }
}

#[cfg(test)]
mod tests_parser {
    use super::*;

    #[test]
    fn test_toml_specification() -> Result<(), io::Error> {
        // let cars = read_toml("./tests/data/cars.toml").unwrap();
        // let iris = read_toml("./tests/data/iris.toml").unwrap();
        // let titanic = read_toml("./tests/data/titanic.toml").unwrap();
        let cars = parse_toml("./tests/toml/cars.toml")?;
        let iris = parse_toml("./tests/toml/iris.toml")?;
        let titanic = parse_toml("./tests/toml/titanic.toml")?;
        assert_eq!(cars.general.name, "Cars");
        assert_eq!(iris.general.name, "Iris");
        assert_eq!(titanic.general.name, "Titanic");
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_wrong_toml_1() {
        let _ = parse_toml("./tests/toml/missing_steps_operation.toml").unwrap();
    }

    #[test]
    #[should_panic]
    fn test_wrong_toml_2() {
        let _ = parse_toml("./tests/toml/missing_steps_properties.toml").unwrap();
    }
    #[test]
    #[should_panic]
    fn test_wrong_toml_3() {
        let _ = parse_toml("./tests/toml/missing_steps.toml").unwrap();
    }
    #[test]
    #[should_panic]
    fn test_wrong_toml_4() {
        let _ = parse_toml("./tests/toml/missing_steps.toml").unwrap();
    }
}

#[cfg(test)]
mod tests_validate {
    use super::*;

    #[test]
    fn tests_validate_pipeline() {
        let m = parse_toml("./tests/toml/cars.toml").unwrap();
        validate_pipeline(&m);
        let m = parse_toml("./tests/toml/iris.toml").unwrap();
        validate_pipeline(&m);
        let m = parse_toml("./tests/toml/titanic.toml").unwrap();
        validate_pipeline(&m);
    }

    #[test]
    #[should_panic]
    fn tests_validate_pipeline_false_operations() {
        let m = parse_toml("./tests/toml/false_operation.toml").unwrap();
        validate_pipeline(&m);
    }
}
