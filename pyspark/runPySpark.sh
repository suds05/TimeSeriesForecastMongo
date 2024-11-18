clear

# The Spark version and mongo-spark-connector version should match - this is super important
# E.g. Spark version 3.5.0 should use mongo-spark-connector_2.12. It doesn't work with mongo-spark-connector_2.13
# Test and make sure
spark-submit --master local --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 PySparkAgg.py
