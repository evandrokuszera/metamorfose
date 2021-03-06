# METAMORFOSE
Metamorfose is a framework to convert relational databases (RDB) to document and column family NoSQLs built on top of Apache Spark. Our approach receives as input a set of directed acyclic graphs (DAG) representing the target NoSQL model. The DAGs are used to generate commands to transform the input RDB data. The approach supports different cardinalities, and the commands can be customized.

# PRE-REQUISITES:
 - JDK 8.
 - Apache Spark 2.3.0

# METAMORFOSE 1:
 - With Metamorfose1 it is possible to perform transformations with tabular data (RDB for CSV, CSV for RDB, CSV for CSV, etc.).
 - For more information see:
   - https://www.youtube.com/watch?v=ta9mXuCeIwM&t=12s
   - or the Metamorfose1/Metamorfose1_Demo.mp4.

# METAMORFOSE 5:
 - In this version it is possible to perform transformation from RDB to NoSQL nested models, as Document and Column Family models.
 - The user can specify a set of DAGs (Directed Acyclic Graphs) to represents the target NoSQL model (or how the entities are structured). 
 - The set of DAG will be convert into a set of Metamorfose functions (Map or MapReduce). 
 - With these functions Metamorfose can load relational data, transform and persist as nested data.
 - In the test package we show some examples of transformations (RDB to NoSQL). We provided:
   - a ConversionProcess, that encapsulates one or more NoSQL schemas.
   - a Postgres backup file, that can be used as input data.
   - the output data will be persisted in a local MongoDB database.
   - to execute the test files with Metamorfose you should install a local Postgres and MongoDB servers.
