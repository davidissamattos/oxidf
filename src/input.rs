// oxidf data input
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use crate::steps::{self, *};
use polars::prelude::*;

pub const INPUT_OP: &[&str] = &["read_csv"];

/// Read CSV
/// Read a csv file given a delimiter and optional header. Polars will read this as a lazy DataFrame to pass on to the rest of the pipeline
/// Arguments:
/// * a literal string path
/// * a delimiter: commonly b',' or b';' or b' ' or b'\t'
/// * a boolean indicating if there is a header or not
/// Return:
/// A LazyFrame encapsulated in Result
pub struct ReadCsvStep {
    delimiter: u8,
    header: bool,
    path: String,
}
impl ReadCsvStep {
    pub fn new(path: &str, delimiter: u8, header: bool) -> Self {
        ReadCsvStep {
            delimiter: delimiter,
            header: header,
            path: String::from(path),
        }
    }
    pub fn from(step: &Steps) -> Self {
        let path = step
            .properties
            .get("path")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let delimiter = step
            .properties
            .get("delimiter")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
            .as_bytes()
            .get(0)
            .unwrap()
            .clone();
        let header = step.properties.get("header").unwrap().as_bool().unwrap();
        ReadCsvStep {
            delimiter: delimiter,
            header: header,
            path: path,
        }
    }
}
impl Execute for ReadCsvStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let df = LazyCsvReader::new(self.path.clone())
            .with_delimiter(self.delimiter)
            .has_header(self.header)
            .with_ignore_parser_errors(true)
            .finish()?;
        Ok(df)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("path"));
        assert!(step.properties.contains_key("delimiter"));
        assert!(step.properties.contains_key("header"));
    }
}

mod tests_read_csv {
    use super::*;
    #[test]
    fn test_read_csv_step() {
        let cars_reader = ReadCsvStep::new("./tests/data/cars_semicolon.csv", b';', true);
        let cars = cars_reader.execute().unwrap();
        let cars_df = cars
            .select([col("dist").sum(), col("speed").sum()])
            .collect()
            .unwrap();
        let sumdist = cars_df
            .column("dist")
            .unwrap()
            .iter()
            .nth(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();
        let sumspeed = cars_df
            .column("speed")
            .unwrap()
            .iter()
            .nth(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();
        assert_eq!(sumdist, 2149);
        assert_eq!(sumspeed, 770);
    }
}
