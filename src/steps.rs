use polars::prelude::*;
use serde_derive::Deserialize;
use toml;
use toml::value::Table;

#[derive(Deserialize)]
pub struct Pipeline {
    pub general: General,
    pub steps: Vec<Steps>,
}
#[derive(Deserialize)]
pub struct General {
    pub name: String,
    pub version: String,
    pub mantainer: String,
    pub description: String,
}
#[derive(Deserialize)]
pub struct Steps {
    pub operation: String,
    pub properties: Table,
}

pub trait Execute {
    fn execute(&self) -> Result<LazyFrame, PolarsError>;
    fn validate(step: &Steps);
}
