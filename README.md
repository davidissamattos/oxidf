# oxidf

## Intro

oxidf is a rust-TOML data processor. 
Instead of coding how a data frame will be processed using a programming language, we specify this process utilizing a TOML file. The TOML file is read by the oxidf software (rust-based) and the data file is then processed. We focus on fast performance.

We utilize the Polars library as the core data processor focusing as much as possible on optimized lazy evaluations 

Advantages: 
+ Simplified syntax. Everything is written as a TOML file. Syntax is kept to the essential and multiple examples are given.
+ All essential operations are included
+ No need to maintain an environment and manage libraries and dependencies. Single binary file
+ Performance. Fast performant code that runs in multiple platforms
+ Reproducible workflows that are easy to source control.

Disadvantages:
 - Limited operations that can be done. Utilizing a general programming language like Python we can manipulate data frames in an extremely flexible way. The set of operations available in oxidf is limited altough covering the most common scenarios
 - Limited input format.
 - Binary size. While we provide a single binary file that is easy to deploy and run. The size of the binary is large.


## TOML specification and operations

The toml file should be created as sequence of consecutive steps. The first step should be of input type, followed by the operations and output.
While it is desirable to have an output step as the latest one, the output can be an intermediate step (e.g. saving intermediate csv files or printing intermediate dataframes)

Most of the examples below are performed in the iris csv file.

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

#### Remove na. 
Remove the rows of a column containing NA
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