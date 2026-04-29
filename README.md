# Knowledge Graph over Databricks System Tables

This project contains a Spark Declarative Pipeline that maps the Databricks system tables
into a set of RDF triples that can be queried directly or loaded into a triplestore.
Additionally, it includes an RDFS ontology that defines the relationships it generates. 
To generate the pipeline, it uses [spark-r2r](https://github.com/aktungmak/spark-r2r).

## Getting started

1. Install the Databricks CLI from [https://docs.databricks.com/dev-tools/cli/databricks-cli.html](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
2. Authenticate to your Databricks workspace, if you have not done so already:
  ```
    $ databricks configure
  ```
3. To deploy a development copy of this project, type:
  ```
    $ databricks bundle deploy
  ```
    By default, this deploys a pipeline called `mapping_pipeline` to the
    selected workspace. This can be customised by editing `databricks.yml`.
4. To run the mapping pipeline, use the following command:
  ```
   $ databricks bundle run mapping_pipeline
  ```
   The output will be written to the table specified when deploying the bundle.
   You can now use [sparql2sql](https://github.com/aktungmak/sparql2sql) to run
   graph queries against it!

## Missing mappings

Not every column of every system table is mapped.
If you would like to contribute more mappings, please feel free to open a PR!