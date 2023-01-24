// oxidf main set of operations
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use crate::steps::*;
use crate::utils::*;
use polars::prelude::*;
use polars_ops::pivot::{pivot, PivotAgg};
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
    "recode",
    "cast",
    "anonymize",
    "pivot",
    "unique"
];

/// Rename column
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
        assert!(
            step.properties.contains_key("col"),
            "Error rename does not contain property: col"
        );
        assert!(step.properties.contains_key("name"));
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
        let select_cols: Vec<Expr> = get_array_columns(self.columns.clone());
        let new_lazydf = self.lazydf.clone().select(select_cols);
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("columns"),
            "Error rename does not contain property: columns"
        );
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
        assert!(
            step.properties.contains_key("col"),
            "Error filter_gt does not contain property: col"
        );
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
        assert!(
            step.properties.contains_key("col"),
            "Error filter_lt does not contain property: col"
        );
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
        assert!(
            step.properties.contains_key("col"),
            "Error filter_gt_eq does not contain property: col"
        );
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
        assert!(
            step.properties.contains_key("col"),
            "Error filter_lt_eq does not contain property: col"
        );
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
        } else if self.value.is_float() {
            let v = self.value.as_float().unwrap();
            new_lazydf = self.lazydf.clone().filter(expr.eq(lit(v)));
        } else if self.value.is_integer() {
            let v = self.value.as_integer().unwrap();
            new_lazydf = self.lazydf.clone().filter(expr.eq(lit(v)));
        } else {
            panic!("Error! The filter_eq operation can only parse strings or numbers");
        }
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! filter_eq does not contain property: col"
        );
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
        FilterIsInStep {
            lazydf,
            col,
            value_list,
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
        let value_list = step
            .properties
            .get("value_list")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
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
            let s = lit(Series::new("_", v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else if first_item.is_float() {
            let v = get_float_array(self.value_list.clone());
            let s = lit(Series::new("_", v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else if first_item.is_integer() {
            let v = get_int_array(self.value_list.clone());
            let s = lit(Series::new("_", v));
            new_lazydf = self.lazydf.clone().filter(expr.is_in(s));
        } else {
            panic!("Error, the filter_isin can only parse strings or numbers");
        }
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! filter_isin does not contain property: col"
        );
        assert!(
            step.properties.contains_key("value_list"),
            "Error! filter_isin does not contain property: value_list"
        );
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
            .lazydf
            .clone()
            .collect()
            .unwrap()
            .apply(self.col.as_str(), hash_polars)
            .unwrap()
            .clone();
        let new_lazydf = df.lazy();
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! anonymize does not contain property: col"
        );
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
        assert!(
            step.properties.contains_key("col"),
            "Error filter_contains does not contain property: col"
        );
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
        let expr = col(self.col.as_str());
        let new_lazydf = self.lazydf.clone().filter(expr.is_not_null());
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! remove_na does not contain property: col"
        );
    }
}

/// recode
/// recode items of a column
pub struct RecodeStep {
    lazydf: LazyFrame,
    col: String,
    from: Vec<Value>,
    to: Vec<Value>,
}
impl RecodeStep {
    pub fn new(lazydf: LazyFrame, col: String, from: Vec<Value>, to: Vec<Value>) -> Self {
        RecodeStep {
            lazydf,
            col,
            from,
            to,
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

        let from = step
            .properties
            .get("from")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();

        let to = step
            .properties
            .get("to")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        RecodeStep::new(lazydf, col, from, to)
    }
}
impl Execute for RecodeStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let first_to = self.to.get(0).unwrap().clone();
        let first_from = self.from.get(0).unwrap().clone();
        let mut lazydf = self.lazydf.clone();

        if first_from.is_str() & first_to.is_str() {
            let from = get_string_array(self.from.clone());
            let to = get_string_array(self.to.clone());
            // println!("{:?}",from);
            for (pos, v_from) in from.iter().enumerate() {
                let v_to = to.get(pos).unwrap().as_str();
                let v_from = v_from.as_str();
                lazydf = lazydf.with_column(
                    when(col(self.col.as_str()).eq(lit(v_from)))
                        .then(lit(v_to))
                        .otherwise(col(self.col.as_str()))
                        .alias(self.col.as_str()),
                );
            }
        } else if first_from.is_float() & first_to.is_float() {
            let from = get_float_array(self.from.clone());
            let to = get_float_array(self.from.clone());
            for (pos, v_from) in from.iter().enumerate() {
                let v_to = to.get(pos).unwrap().clone();
                let v_from = v_from.clone();
                lazydf = lazydf.with_column(
                    when(col(self.col.as_str()).eq(lit(v_from)))
                        .then(lit(v_to))
                        .otherwise(col(self.col.as_str()))
                        .alias(self.col.as_str()),
                );
            }
        } else if first_from.is_integer() & first_to.is_integer() {
            let from = get_int_array(self.from.clone());
            let to = get_int_array(self.from.clone());
            for (pos, v_from) in from.iter().enumerate() {
                let v_to = to.get(pos).unwrap().clone();
                let v_from = v_from.clone();
                lazydf = lazydf.with_column(
                    when(col(self.col.as_str()).eq(lit(v_from)))
                        .then(lit(v_to))
                        .otherwise(col(self.col.as_str()))
                        .alias(self.col.as_str()),
                );
            }
        } else if first_from.is_integer() & first_to.is_str() {
            let from = get_int_array(self.from.clone());
            let to = get_string_array(self.to.clone());
            for (pos, v_from) in from.iter().enumerate() {
                let v_to = to.get(pos).unwrap().as_str();
                let v_from = v_from.clone(); //.to_string().as_str(); //convert int to string
                lazydf = lazydf.with_column(
                    when(col(self.col.as_str()).eq(lit(v_from)))
                        .then(lit(v_to))
                        .otherwise(col(self.col.as_str()))
                        .alias(self.col.as_str()),
                );
                //we cast after replacing
                lazydf = lazydf.with_column(
                    col(self.col.as_str())
                        .cast(DataType::Utf8)
                        .alias(self.col.as_str()),
                );
            }
        } else if first_from.is_str() & first_to.is_integer() {
            let from = get_string_array(self.from.clone());
            let to = get_int_array(self.to.clone());
            for (pos, v_from) in from.iter().enumerate() {
                let v_to = to.get(pos).unwrap().clone();
                let v_from = v_from.clone(); //.to_string().as_str(); //convert int to string
                lazydf = lazydf.with_column(
                    when(col(self.col.as_str()).eq(lit(v_from.as_str())))
                        .then(lit(v_to))
                        .otherwise(col(self.col.as_str()))
                        .alias(self.col.as_str()),
                );
            }
            //we cast after replacing
            lazydf = lazydf.with_column(
                col(self.col.as_str())
                    .cast(DataType::Int64)
                    .alias(self.col.as_str()),
            );
        } else {
            panic!("Error! In recode step. The lists to and from do not match correct types")
        }
        let new_lazydf = lazydf.clone();
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! recode does not contain property: col"
        );
        assert!(
            step.properties.contains_key("to"),
            "Error! recode does not contain property: to"
        );
        assert!(
            step.properties.contains_key("from"),
            "Error! recode does not contain property: from"
        );
        let to = step
            .properties
            .get("to")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        let from = step
            .properties
            .get("from")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        let first_to = to.get(0).unwrap().clone();
        let first_from = from.get(0).unwrap().clone();
        if first_from.is_integer() {
            let _ = get_int_array(to.clone());
            let _ = get_int_array(from.clone());
        }
        if first_from.is_float() {
            let _ = get_float_array(to.clone());
            let _ = get_float_array(from.clone());
        }
        if first_from.is_float() {
            let _ = get_float_array(to.clone());
            let _ = get_float_array(from.clone());
        }
    }
}

/// cast
/// cast a column into string, int or float
pub struct CastStep {
    lazydf: LazyFrame,
    col: String,
    to: String,
}
impl CastStep {
    pub fn new(lazydf: LazyFrame, col: String, to: String) -> Self {
        CastStep { lazydf, col, to }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let col = step
            .properties
            .get("col")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let to = step
            .properties
            .get("to")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        CastStep::new(lazydf, col, to)
    }
}
impl Execute for CastStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let dtype = match self.to.as_str() {
            "string" => DataType::Utf8,
            "int" => DataType::Int64,
            "float" => DataType::Float64,
            _ => panic!("Error! {} is not a valid data type", self.to.as_str()),
        };
        let new_lazydf = self
            .lazydf
            .clone()
            .with_column(col(self.col.as_str()).cast(dtype).alias(self.col.as_str()));
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("col"),
            "Error! cast does not contain property: col"
        );
        assert!(
            step.properties.contains_key("to"),
            "Error! cast does not contain property: to"
        );
    }
}

/// Pivot table
/// pivot a table from long to wide format
pub struct PivotStep {
    lazydf: LazyFrame,
    values: Vec<String>,
    index: Vec<String>,
    columns: Vec<String>,
    aggregation: String,
    sort_columns: bool,
}
impl PivotStep {
    pub fn new(
        lazydf: LazyFrame,
        values: Vec<String>,
        index: Vec<String>,
        columns: Vec<String>,
        aggregation: String,
        sort_columns: bool,
    ) -> Self {
        PivotStep {
            lazydf,
            values,
            index,
            columns,
            aggregation,
            sort_columns,
        }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let values = get_string_array(
            step.properties
                .get("values")
                .unwrap()
                .as_array()
                .unwrap()
                .clone(),
        );
        let index = get_string_array(
            step.properties
                .get("index")
                .unwrap()
                .as_array()
                .unwrap()
                .clone(),
        );
        let columns = get_string_array(
            step.properties
                .get("columns")
                .unwrap()
                .as_array()
                .unwrap()
                .clone(),
        );
        let aggregation = step
            .properties
            .get("aggregation")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let sort_columns = step
            .properties
            .get("sort_columns")
            .unwrap()
            .as_bool()
            .unwrap();
        PivotStep::new(lazydf, values, index, columns, aggregation, sort_columns)
    }
}
impl Execute for PivotStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let df = self
            .lazydf
            .clone()
            .collect()
            .expect("Error collecting the dataframe in the pivot step");
        let agg_fn = match self.aggregation.as_str() {
            "first" => PivotAgg::First,
            "last" => PivotAgg::Last,
            "max" => PivotAgg::Max,
            "mean" => PivotAgg::Mean,
            "median" => PivotAgg::Median,
            "min" => PivotAgg::Min,
            "sum" => PivotAgg::Sum,
            "count" => PivotAgg::Count,
            _ => panic!("Error! The provided aggregation in the pivot step is not valid"),
        };
        let out = pivot(
            &df,
            self.values.clone(),
            self.index.clone(),
            self.columns.clone(),
            agg_fn,
            self.sort_columns.clone(),
        )
        .unwrap();
        let new_lazydf = out.lazy();
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
        assert!(
            step.properties.contains_key("values"),
            "Error! pivot does not contain property: values"
        );
        assert!(
            step.properties.contains_key("index"),
            "Error! pivot does not contain property: index"
        );
        assert!(
            step.properties.contains_key("columns"),
            "Error! pivot does not contain property: columns"
        );
        assert!(
            step.properties.contains_key("aggregation"),
            "Error! pivot does not contain property: aggregation"
        );
        assert!(
            step.properties.contains_key("sort_columns"),
            "Error! pivot does not contain property: sort_columns"
        );
    }
}

// keep only unique
pub struct UniqueStep {
    lazydf: LazyFrame,
    cols: Option<Vec<String>>,

}
impl UniqueStep {
    pub fn new(lazydf: LazyFrame, cols: Option<Vec<String>>) -> Self {
        UniqueStep { lazydf, cols }
    }
    pub fn from(step: &Steps, lazydf: LazyFrame) -> Self {
        let all_col = match step.properties.get("cols") {
            None => true,
            _ => false
        };
        let cols:Option<Vec<String>> = match all_col {
            true => None,
            false => {
                Some(get_string_array(
                    step.properties
                        .get("cols")
                        .unwrap()
                        .as_array()
                        .unwrap()
                        .clone()
                ))
            }
            
        };
        UniqueStep::new(lazydf, cols)
    }
}
impl Execute for UniqueStep {
    fn execute(&self) -> Result<LazyFrame, PolarsError> {
        let new_lazydf = self.lazydf
                .clone()
                .unique_stable(self.cols.clone(), UniqueKeepStrategy::First);
        Ok(new_lazydf)
    }
    fn validate(step: &Steps) {
    }
}


// Melt data frame
//
//

// Filter or
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