// oxidf pipeline output
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use crate::steps::*;
use polars::prelude::*;

pub const OUTPUT_OP: &[&str] = &["save_csv", "preview"];

/// Save csv to file
/// Saves a DataFrame in a csv file Polars
/// The data frame represented in the lazy format (only operations and without occupying memory) is converted to an actual dataframe in memory.
/// Arguments:
/// * a reference to the lazydataframe to be saved
/// * a literal string path
/// * a delimiter: commonly b',' or b';' or b' ' or b'\t'
/// * a boolean indicating if there is a header or not
/// Return:
/// A LazyFrame encapsulated in Result
pub struct SaveCsvStep {
    lazydf: LazyFrame,
    delimiter: u8,
    header: bool,
    path: String,
}
impl SaveCsvStep {
    pub fn new(lazydf: LazyFrame, path: &str, delimiter: u8, header: bool) -> Self {
        SaveCsvStep {
            lazydf: lazydf,
            delimiter: delimiter,
            header: header,
            path: String::from(path),
        }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
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
        SaveCsvStep::new(lazydf, &path, delimiter, header)
    }
}
impl Execute for SaveCsvStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let mut df = self.lazydf.clone().collect()?;
        let mut file = std::fs::File::create(&self.path)?;
        CsvWriter::new(&mut file)
            .has_header(self.header)
            .with_delimiter(self.delimiter)
            .finish(&mut df)?;
        Ok(self.lazydf.clone())
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("path"));
        assert!(step.properties.contains_key("delimiter"));
        assert!(step.properties.contains_key("header"));
    }
}

/// Preview header
/// Pretty prints a DataFrame in the command line using the default format display printer of Polars
pub struct PreviewStep {
    lazydf: LazyFrame,
}
impl PreviewStep {
    pub fn new(lazydf: LazyFrame) -> Self {
        PreviewStep { lazydf: lazydf }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        PreviewStep::new(lazydf)
    }
}
impl Execute for PreviewStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let mut df = self.lazydf.clone().fetch(1000).unwrap();
        println!("{}", df);
        Ok(self.lazydf.clone())
    }
    fn validate(step: &Steps) {}
}
mod tests_preview {
    use super::*;
    //Just testing if it does not fail
    #[test]
    fn test_print() {
        let cars_reader =
            crate::input::ReadCsvStep::new("./tests/data/cars_semicolon.csv", b';', true);
        let cars = cars_reader.execute().unwrap();
        //sample true and false and different values of n
        let preview_step = PreviewStep::new(cars.clone());
        let _ = preview_step.execute();
    }
}
