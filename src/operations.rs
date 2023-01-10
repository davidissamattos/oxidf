// oxidf main set of operations
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use crate::steps::*;
use crate::utils::*;
use polars::lazy::dsl::Expr;
use polars::prelude::*;
use toml::Value;

pub const OPERATIONS_OP: &[&str] = &[
    "rename",
    "select",
    "filter_eq",
    "filter_gt",
    "filter_gt_eq",
    "filter_lt",
    "filter_lt_eq",
    "filter_isin",
    "filter_contains",
    "remove_na",
    "anonymize"
];

/// Rename columnt  
/// Renames individual columns in the data frame
pub struct RenameStep {
    lazydf: LazyFrame,
    col: String,
    name: String,
}
impl RenameStep {
    pub fn new(lazydf: LazyFrame, col: String, name: String) -> Self {
        RenameStep {
            lazydf: lazydf,
            col: col,
            name: name,
        }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let name = step
            .properties
            .get("name")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        RenameStep::new(lazydf, col, name)
    }
}
impl Execute for RenameStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let new_lazydf = self
            .lazydf
            .clone()
            .rename([self.col.as_str()], [self.name.as_str()]);
        // println!("{}",df);
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error rename does not contain property: col");
        assert!(step.properties.contains_key("name"));
    }
}
mod tests_rename {
    use super::*;
    //Just testing if it does not fail
    #[test]
    fn test_rename() {
        //Import a csv, rename a column and get the names of the columns
        let cars_reader =
            crate::input::ReadCsvStep::new("./tests/data/cars_semicolon.csv", b';', true);
        let cars = cars_reader.execute().unwrap();
        let rename = RenameStep::new(cars.clone(), String::from("dist"), String::from("Distance"));
        let cars = rename.execute().unwrap();
        let cars_df = cars.collect().unwrap();
        // the columns should contain the new name and not contain the old name
        let columns = cars_df.get_column_names();
        assert!(!columns.contains(&"dist"));
        assert!(columns.contains(&"Distance"));
    }
}

/// Select columns in the data
pub struct SelectColumnsStep {
    lazydf: LazyFrame,
    columns: Vec<String>,
}
impl SelectColumnsStep {
    pub fn new(lazydf: LazyFrame, columns: Vec<String>) -> Self {
        SelectColumnsStep {
            lazydf: lazydf,
            columns: columns,
        }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let columns_value = step
            .properties
            .get("columns")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        let columns: Vec<String> = get_string_array(columns_value);
        SelectColumnsStep::new(lazydf, columns)
    }
}
impl Execute for SelectColumnsStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let  select_cols: Vec<Expr> = get_array_columns(self.columns.clone());
        let new_lazydf = self.lazydf.clone().select(select_cols);
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("columns"), "Error rename does not contain property: columns");
    }
}

/// Filter greater than
pub struct FilterGreaterStep {
    lazydf: LazyFrame,
    col: String,
    value: f64,
}
impl FilterGreaterStep {
    pub fn new(lazydf: LazyFrame, col: String, value: f64) -> Self {
        FilterGreaterStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step.properties.get("value").unwrap().as_float().unwrap();
        FilterGreaterStep::new(lazydf, col, value)
    }
}
impl Execute for FilterGreaterStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.gt(lit(self.value)));
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"),"Error filter_gt does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}

///Filter smaller than
pub struct FilterSmallerStep {
    lazydf: LazyFrame,
    col: String,
    value: f64,
}
impl FilterSmallerStep {
    pub fn new(lazydf: LazyFrame, col: String, value: f64) -> Self {
        FilterSmallerStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step.properties.get("value").unwrap().as_float().unwrap();
        FilterSmallerStep::new(lazydf, col, value)
    }
}
impl Execute for FilterSmallerStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.lt(lit(self.value)));
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error filter_lt does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}

/// Filter greater or equal than
pub struct FilterGreaterEqStep {
    lazydf: LazyFrame,
    col: String,
    value: f64,
}
impl FilterGreaterEqStep {
    pub fn new(lazydf: LazyFrame, col: String, value: f64) -> Self {
        FilterGreaterEqStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step.properties.get("value").unwrap().as_float().unwrap();
        FilterGreaterEqStep::new(lazydf, col, value)
    }
}
impl Execute for FilterGreaterEqStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.gt_eq(lit(self.value)));
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"),"Error filter_gt_eq does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}

///filter_lt_eq
/// Filter smaller than
pub struct FilterSmallerEqStep {
    lazydf: LazyFrame,
    col: String,
    value: f64,
}
impl FilterSmallerEqStep {
    pub fn new(lazydf: LazyFrame, col: String, value: f64) -> Self {
        FilterSmallerEqStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step.properties.get("value").unwrap().as_float().unwrap();
        FilterSmallerEqStep::new(lazydf, col, value)
    }
}
impl Execute for FilterSmallerEqStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.lt_eq(lit(self.value)));
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error filter_lt_eq does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}

/// filter_eq
/// Filter data frame on equality of a column
pub struct FilterEqualStep {
    lazydf: LazyFrame,
    col: String,
    value: Value,
}
impl FilterEqualStep {
    pub fn new(lazydf: LazyFrame, col: String, value: Value) -> Self {
        FilterEqualStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step.properties.get("value").unwrap().clone();
        FilterEqualStep::new(lazydf, col, value)
    }
}
impl Execute for FilterEqualStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let mut new_lazydf: LazyFrame;
        if self.value.is_str() {
            let v = self.value.as_str().unwrap();
            new_lazydf = self.lazydf.clone().filter(expr.eq(lit(v)));
        } else if self.value.is_float(){
            let v = self.value.as_float().unwrap();
            new_lazydf = self.lazydf.clone().filter(expr.eq(lit(v)));
        } else if self.value.is_integer(){
            let v = self.value.as_integer().unwrap();
            new_lazydf = self.lazydf.clone().filter(expr.eq(lit(v)));
        } else {
            panic!("Error! The filter_eq operation can only parse strings or numbers");
        }
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error! filter_eq does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}

/// filter_isin
/// Filter values that are in a array
pub struct FilterIsInStep {
    lazydf: LazyFrame,
    col: String,
    value_list: Vec<Value>,
}
impl FilterIsInStep {
    pub fn new(lazydf: LazyFrame, col: String, value_list: Vec<Value>) -> Self {
        FilterIsInStep { lazydf, col, value_list }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value_list = step.properties.get("value_list").unwrap().as_array().unwrap().clone();
        FilterIsInStep::new(lazydf, col, value_list)
    }
}
impl Execute for FilterIsInStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str());
        let first_item = self.value_list.get(0).unwrap().clone();
        let mut new_lazydf: LazyFrame;
        if first_item.is_str() {
            let v = get_string_array(self.value_list.clone());
            let s = lit(Series::new("_",v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else if first_item.is_float(){
            let v = get_float_array(self.value_list.clone());
            let s = lit(Series::new("_",v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else if first_item.is_integer(){
            let v = get_int_array(self.value_list.clone());
            let s = lit(Series::new("_",v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else {
            panic!("Error, the filter_isin can only parse strings or numbers");
        }
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error! filter_isin does not contain property: col");
        assert!(step.properties.contains_key("value_list"),"Error! filter_isin does not contain property: value_list");
    }
}

/// anonymize
/// Anonymize a string column
pub struct AnonymizeStep {
    lazydf: LazyFrame,
    col: String,
}
impl AnonymizeStep {
    pub fn new(lazydf: LazyFrame, col: String) -> Self {
        AnonymizeStep {
            lazydf: lazydf,
            col: col,
        }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
            AnonymizeStep::new(lazydf, col)
    }
}
impl Execute for AnonymizeStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let mut df = self
            .lazydf.clone()
            .collect().unwrap().apply(self.col.as_str(), hash_polars).unwrap().clone();
        let new_lazydf = df.lazy();
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error! anonymize does not contain property: col");
    }
}

/// filter_contains
/// filter a string column based on a regex expression
pub struct FilterContainsStep {
    lazydf: LazyFrame,
    col: String,
    value: String,
}
impl FilterContainsStep {
    pub fn new(lazydf: LazyFrame, col: String, value: String) -> Self {
        FilterContainsStep { lazydf, col, value }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let value = step
        .properties
        .get("value")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
        FilterContainsStep::new(lazydf, col, value)
    }
}
impl Execute for FilterContainsStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr: Expr = col(self.col.as_str()).str().contains(self.value.clone());
        let new_lazydf = self.lazydf.clone().filter(expr);
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"),"Error filter_contains does not contain property: col");
        assert!(step.properties.contains_key("value"));
    }
}


///remove_na
/// Remove rows that are NA in a specific column
pub struct RemoveNAStep {
    lazydf: LazyFrame,
    col: String,
}
impl RemoveNAStep {
    pub fn new(lazydf: LazyFrame, col: String) -> Self {
        RemoveNAStep { lazydf, col }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        RemoveNAStep::new(lazydf, col)
    }
}
impl Execute for RemoveNAStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let expr =  col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.is_not_null());
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(step.properties.contains_key("col"), "Error! remove_na does not contain property: col");
    }
}


// Recode values
//
//
//

// Pivot table
//
//



// Group By
//
//

// Summarize
//
//
//

// Mean center column
//
//

// Normalize column
//
//
//


