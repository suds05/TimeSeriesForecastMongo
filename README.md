# Experiments in forecasting with time series data using Mongo

## Data used

## Analysis from Python with MongoDB Driver

## Analysis from PySpark using Spark Connector

## Comparisions
### 1. Specification of data logic
In Python with MongoDB driver, the aggregation data logic is specified in a custom DSL of MongoDB API
```python
      # Define the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    # Ensure the timestamp field exists and is not None
                    'released': {'$exists': True, '$ne': None}
                }
            },
            {
                '$addFields': {
                    'year': {'$year': f'${timestampField}'},
                    'month': {'$month': f'${timestampField}'},
                    'quarter': {
                        '$toInt': {
                            '$ceil': {
                                '$divide': [{'$month': f'${timestampField}'}, 3]
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    # Ensure the year field exists and is not None
                    'year': {'$exists': True, '$ne': None},
                    'year': {'$gte': 2010},
                    'year': {'$lte': 2015}
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': '$year',
                        'quarter': '$quarter'
                    },
                    'movies_in_window': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.quarter': 1
                }
            }]
```
With PySpark, logic is in code
```python
    agg_df = df\
        .filter(col("released").isNotNull())\
        .withColumn("year", year(col("released")))\
        .withColumn("quarter", quarter(col("released")))\
        .filter((df.year >= 2000) & (df.year <= 2005))\
        .groupBy("year", "quarter").count()\
        .sort("year", "quarter", ascending=False)
```

### 2. Storage dependency
   * With Python and MongoDB driver, data logic is tied to MongoDB storage
   * With Spark, data logic is storage independent. MongoDB dependency is only in dataframe intialiization. The data logic in Pyspark will continue to work with different storage drivers

   ```python
   # Read data from MongoDB
   df = spark.read\
      .format("mongodb")\
      .option("database", "sample_mflix")\
      .option("collection", "movies")\
      .load()
   ``` 

### 3. Unit testing
   * With Python and MongoDB driver, the data logic in custom DSL has to be integration tested. 
   * With Spark, we can both unit test by initializing data frame with in-memory list. (Integration testing is also possible)

```python
# Sample data
data = [
    {"title": "Movie 1", "released": "2001-01-01T00:00:00Z"},
    {"title": "Movie 2", "released": "2002-04-15T00:00:00Z"},
    {"title": "Movie 3", "released": "2003-07-20T00:00:00Z"},
    {"title": "Movie 4", "released": "2004-10-30T00:00:00Z"},
    {"title": "Movie 5", "released": "2005-12-25T00:00:00Z"},
    {"title": "Movie 6", "released": None},  # This should be filtered out
    {"title": "Movie 7", "released": ""}     # This should be filtered out
]

# Create DataFrame from the list
df = spark.createDataFrame(data, schema)
```

### 4. Richer set of built-in functions
    * Pyspark comes with a richer set of built-in functions. E.g. quarter function was built-in in Spark, while it had to be written in MongoDB DSL

### 5. Pushdown to Storage
    * With Python DSL, both filter and aggregation are pushed down to storage
    
    * With Spark, filter is pushed down, but aggregation happens in compute, albeit in a distributed way. 
        24/11/18 08:48:01 INFO V2ScanRelationPushDown: 
        Pushing operators to MongoTable()
        Pushed Filters: IsNotNull(year), IsNotNull(released)
        Post-Scan Filters: (cast(year#21 as int) >= 2000),(cast(year#21 as int) <= 2005)

        == Physical Plan ==
        AdaptiveSparkPlan isFinalPlan=false
        +- Sort [year#44 DESC NULLS LAST, quarter#67 DESC NULLS LAST], true, 0
        +- Exchange rangepartitioning(year#44 DESC NULLS LAST, quarter#67 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=22]
            +- HashAggregate(keys=[year#44, quarter#67], functions=[count(1)])
                +- Exchange hashpartitioning(year#44, quarter#67, 200), ENSURE_REQUIREMENTS, [plan_id=19]
                    +- HashAggregate(keys=[year#44, quarter#67], functions=[partial_count(1)])
                    +- Project [year(cast(released#15 as date)) AS year#44, quarter(cast(released#15 as date)) AS quarter#67]
                        +- Filter ((cast(year#21 as int) >= 2000) AND (cast(year#21 as int) <= 2005))
                            +- BatchScan MongoTable()[released#15, year#21] MongoScan{namespaceDescription=sample_mflix.movies} RuntimeFilters: []


    * Time Series forecasting will happen in compute for both. TBD: to check if this can be distributed

