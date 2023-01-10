// oxidf common utilities for other files
// Author: David Issa Mattos
// Mantainer: David Issa Mattos

use polars::{prelude::*, lazy::dsl::Expr};
use toml::Value;
use sha2::{Sha256, Digest};

pub fn get_int_array(value_array: Vec<Value>) -> Vec<i64> {
    let mut columns: Vec<i64> = vec![];
    for v in value_array {
        let i = v.as_integer().unwrap();
        columns.push(i);
    }
    columns
}

pub fn get_string_array(value_array: Vec<Value>) -> Vec<String> {
    let mut columns: Vec<String> = vec![];
    for v in value_array {
        let i = v.as_str().unwrap().to_string();
        columns.push(i);
    }
    columns
}

pub fn get_float_array(value_array: Vec<Value>) -> Vec<f64> {
    let mut columns: Vec<f64> = vec![];
    for v in value_array {
        let i = v.as_float().unwrap();
        columns.push(i);
    }
    columns
}

pub fn get_array_columns(value_array: Vec<String>) -> Vec<Expr>{
    let mut select_cols: Vec<Expr> = vec![];
        for v in value_array {
            select_cols.push(col(&v));
        }
    select_cols
}


fn hash(vin:String) -> String{
    //Hash a string using Sha256
    let mut hasher = Sha256::new();
    hasher.update(vin.into_bytes());
    let result = format!("{:X}", hasher.finalize());
    result
}

pub fn hash_polars(vin_series: &Series)->Series{
    //Function that we can apply to a element wise in polars
    //https://pola-rs.github.io/polars/polars/frame/struct.DataFrame.html#method.apply
    // Receives a series, 
    //convert it to Uf8 chunk,
    // convert to iterator
    // Realizes two maps (do not know why...)
    // Converts back to a series after collecting

    vin_series.utf8()
        .unwrap()
        .into_iter()
        .map(|vin|{
            vin.map(|s| hash(s.to_string()))
        })
        .collect::<Utf8Chunked>()
        .into_series()
        
        //use as
        // df.apply("CarID",hash_polars);
}