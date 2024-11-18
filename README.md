# Forecasting with time series data using Mongo store
The aim of this experiments is to test forecasting of time series data stored in MongoDB. We will perform the experiment with two tools:
1. Simple Python process with primarily relying on [MongoDB aggregation pipelines](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/)
2. PySpark process using [MongoDB Spark Connector](https://www.mongodb.com/docs/spark-connector/v10.2/)

For the Forecasting, we will try to use techniques detailed in these papers: 
1. [Time series forecasting used for real-time
 anomaly detection on websites](https://www.semanticscholar.org/paper/Time-series-forecasting-used-for-real-time-anomaly-Galvas/43aa251f185ac6c85b988f2c0b96572eb0b26bca)
2. [An Introductory Study on Time Series Modeling and Forecasting](https://arxiv.org/abs/1302.6613)

## Data used
MongoDB atlas comes preloaded with a [sample_mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/#std-label-mflix-movies) database containing data on movies and movie theaters. The movies collection contains details on movies. Each document contains a single movie, and information such as its title, release year, and cast.

It has movies released from 1900 to 2016, containing a total of around 21,000 documents.

A typical document looks like this
```json
{"_id":{"$oid":"673b62683b95a572da4047ac"},"plot":"A group of bandits stage a brazen train hold-up, only to find a determined posse hot on their heels.","genres":["Short","Western"],"runtime":{"$numberInt":"11"},"cast":["A.C. Abadie","Gilbert M. 'Broncho Billy' Anderson","George Barnes","Justus D. Barnes"],"poster":"https://m.media-amazon.com/images/M/MV5BMTU3NjE5NzYtYTYyNS00MDVmLWIwYjgtMmYwYWIxZDYyNzU2XkEyXkFqcGdeQXVyNzQzNzQxNzI@._V1_SY1000_SX677_AL_.jpg","title":"The Great Train Robbery","fullplot":"Among the earliest existing films in American cinema - notable as the first film that presented a narrative story to tell - it depicts a group of cowboy outlaws who hold up a train and rob the passengers. They are then pursued by a Sheriff's posse. Several scenes have color included - all hand tinted.","languages":["English"],"released":{"$date":{"$numberLong":"-2085523200000"}},"directors":["Edwin S. Porter"],"rated":"TV-G","awards":{"wins":{"$numberInt":"1"},"nominations":{"$numberInt":"0"},"text":"1 win."},"lastupdated":"2015-08-13 00:27:59.177000000","year":{"$numberInt":"1903"},"imdb":{"rating":{"$numberDouble":"7.4"},"votes":{"$numberInt":"9847"},"id":{"$numberInt":"439"}},"countries":["USA"],"type":"movie","tomatoes":{"viewer":{"rating":{"$numberDouble":"3.7"},"numReviews":{"$numberInt":"2559"},"meter":{"$numberInt":"75"}},"fresh":{"$numberInt":"6"},"critic":{"rating":{"$numberDouble":"7.6"},"numReviews":{"$numberInt":"6"},"meter":{"$numberInt":"100"}},"rotten":{"$numberInt":"0"},"lastUpdated":{"$date":{"$numberLong":"1439061370000"}}},"num_mflix_comments":{"$numberInt":"0"}}
```

## Analysis from Python with MongoDB Driver
1. Aggregate the data yearly using MongoDB aggregation pipeline
2. Plot the data. It looks as below. As evident, 

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

