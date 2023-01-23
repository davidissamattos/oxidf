# oxidf

## Intro

oxidf is a rust-based tabular data processor with a TOML specification. 
Instead of coding how a data frame will be processed using a programming language, we specify this process utilizing a TOML file. The TOML file is read by the oxidf software (rust-based) and the data file is then processed. We focus on fast performance.

We utilize the [Polars](https://docs.rs/polars/latest/polars/) library as the core data processor focusing as much as possible on optimized lazy evaluations 

Advantages: 
+ Simplified syntax. Everything is written as a TOML file. Syntax is kept to the essential and multiple examples are given.
+ All essential operations are included (in progress)
+ No need to maintain an environment and manage libraries and dependencies. Single binary file
+ Performance. Fast performant code that runs in multiple platforms
+ We can run complex data cleaning tasks without learning a programming language. The output can them be imported to the preferred analysis software
+ Reproducible workflows that are easy to source control. No need to manage virtual environments, programming language versions, docker images etc.

Disadvantages:
 - Limited operations that can be done. Utilizing a general programming language like Python we can manipulate data frames in an extremely flexible way (of course given that you know how). The set of operations available in oxidf is limited altough covering the most common scenarios
 - Limited input format. Right now we only accept CSV format. We aim to expand to other formats like (Parquet, SPSS sav etc..)


## TOML specification and operations

The toml file should be created as sequence of consecutive steps. The first step should be of input type, followed by the operations and output.
While it is desirable to have an output step as the latest one, the output can be an intermediate step (e.g. saving intermediate csv files or printing intermediate dataframes)

Most of the examples below are performed in the iris csv file or in the titanic csv file.

### Input 

#### Read csv file 

```toml
[[steps]]
    operation = "read_csv"
    [steps.properties]
    path = "./tests/data/iris.csv"
    delimiter = ","
    header = true
```

### Operations

#### Rename a single column
```toml
[[steps]]
    operation = "rename"
    [steps.properties]
    col = "Sepal.Width"
    name= "Width"
```


#### Select multiple columns
```toml
[[steps]]
    operation = "select"
    [steps.properties]
    columns = ["Sepal.Length", "Sepal.Width"]
```

#### Filter greater than
```toml
[[steps]]
    operation = "filter_gt"
    [steps.properties]
    col = "Sepal.Width"
    value= 3
```

#### Filter greater or equal to
```toml
[[steps]]
    operation = "filter_gt_eq"
    [steps.properties]
    col = "Sepal.Width"
    value= 3
```

#### Filter lesser than
```toml
[[steps]]
    operation = "filter_lt"
    [steps.properties]
    col = "Sepal.Width"
    value= 3
```

#### Filter lesser or equal to
```toml
[[steps]]
    operation = "filter_lt_eq"
    [steps.properties]
    col = "Sepal.Width"
    value= 3
```


#### Filter value equal to
```toml
[[steps]]
    operation = "filter_eq"
    [steps.properties]
    col = "Sepal.Width"
    value= 3
```
or for strings
```toml
[[steps]]
    operation = "filter_eq"
    [steps.properties]
    col = "Species"
    value= "versicolor"
```

#### Filter value is in list. 
The list should only contain items of the same type, not be empty (string or numbers, either all integers or all float)
```toml
[[steps]]
    operation = "filter_isin"
    [steps.properties]
    col = "Species"
    value_list= ["setosa", "versicolor"]
```


#### Filter contains. 
Filter values of one column based on a regex pattern
```toml
[[steps]]
    operation = "filter_contains"
    [steps.properties]
    col = "Sepal.Width"
    value= "^set.*"
```

#### Recode. 
Recode values of a specific column. As much as possible we cast the column to the type of the to array. It is important to remember that the all elements in the to array (and separately the from array) should be of the same type. 
This function accepts the following types of recoding:
string -> string
float -> float
integer -> integer
integer -> string
string -> integer
```toml
[[steps]]
    operation = "recode"
    [steps.properties]
    col = "Survived"
    from = ["Yes", "No"]
    to = [1, 0]
```

#### Cast.
Change the type of the column to either string, float or int.
From float to int, the number is rounded
```toml
[[steps]]
    operation = "cast"
    [steps.properties]
    col = "Sepal.Length"
    to = "int"
```

#### Remove na. 
Remove the rows of a column containing NA. We created a new csv file from the iris dataset with some NA to test this function
```toml
[[steps]]
    operation = "remove_na"
    [steps.properties]
    col = "Species"
```
If one needs to remove from the whole dataframe, instead of specifying a step for every column one can write
```toml
[[steps]]
    operation = "remove_na"
    [steps.properties]
    col = "*"
```


#### Anonymize. 
Take a column of strings and hash it to Sha:256. For this operation all the steps are collected, the whole input is read and all previous operations are performed. The collected results are passed to the next step.
```toml
[[steps]]
    operation = "anonymize"
    [steps.properties]
        col = "Species"
```

#### Pivot. 
Pivot a dataframe from long to wide. This operation requires values to be collected before running
Here we specify three lists of columns. 
* columns. Which column we will get the name for the new columns based on the rows
* values: Which column containing the values that will fill these new columns
* index: which columns we will use to create unique rows. If based on the index only some of the rows are repeated, we need to use an aggregation function on the values.
* aggregation: aggregation function for the repeated rows based on the index. The valid aggregations are:
  * "first" 
  * "last"
  * "max" 
  * "mean" 
  * "median" 
  * "min" 
  * "sum" 
  * "count"

This example is run on the pivot_example.csv dataset
```toml
[[steps]]
operation = "pivot"
[steps.properties]
    values = ["parvalue"]
    index = ["CarID","ts", "request"]
    columns = ["parname"]
    aggregation = "first"
    sort_columns = true
```


### Output

#### Save csv file. 
For this operation all the steps are collected, the whole input is read and all previous operations are performed. The collected results are passed to the next step (if there are any)

```toml
[[steps]]
    operation = "read_csv"
    [steps.properties]
    path = "./tests/data/iris.csv"
    delimiter = ","
    header = true #does the csv includes header?
```

#### Preview 
Previews a collected version of the latest step in the terminal. This operations reads the first 1000  rows of the dataframe and process all the steps before printing. It prints only the first 5 and last 5 rows.
```toml
[[steps]]
operation = "preview"
[steps.properties]
```

## Tips

* The operations are lazy evaluated and optimized. That is the underlying software creates a graph of operations and optimize the graph before parallelizing them in the execution (called collect). However, there are a few operations that requires the operations to be collected before executing. As much as possible try run all operations that do not require collect before an operation that runs collect.
* Filter all values before collecting as this will reduce memory consumption


## Common error messages and what to do
