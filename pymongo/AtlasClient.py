from pymongo import MongoClient


class AtlasClient ():

    def __init__(self, altas_uri, dbname):
        self.mongodb_client = MongoClient(altas_uri)
        self.database = self.mongodb_client[dbname]

    # A quick way to test if we can connect to Atlas instance
    def ping(self):
        self.mongodb_client.admin.command('ping')

    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    def find(self, collection_name, filter={}, limit=0):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items

    def print_aggs(aggs):
        for agg in aggs:
            if agg['_id'] is None or (isinstance(agg['_id'], list) and len(agg['_id']) == 0):
                print(f"Found null or empty _id: {agg}")
            else:
                print(agg)

    def agg_movies(self, collection_name, fieldToAggregate):
        # Define the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'year': {'$gte': 2010}
                }
            },
            {
                '$group': {
                    # Use the fieldToAggregate argument
                    '_id': f'${fieldToAggregate}',
                    'totalMovies': {'$sum': 1}
                }
            },
            {
                '$match': {
                    # Filter out documents where _id is None
                    '_id': {'$ne': None}
                }
            },
            {
                '$sort': {
                    'totalMovies': -1
                }
            }
        ]

        collection = self.database[collection_name]
        results = collection.aggregate(pipeline)
        results_list = list(results)

        # Flatten the _id field by using its first element
        for result in results_list:
            if isinstance(result['_id'], list) and len(result['_id']) > 0:
                result['_id'] = result['_id'][0]

        return results_list

    def agg_movies_by_quarter(self, collection_name, start_year, end_year):
        timestampField = 'released'

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
                    'year': {'$gte': start_year},
                    'year': {'$lte': end_year}
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

        collection = self.database[collection_name]
        results = collection.aggregate(pipeline)
        return results

    def agg_movies_by_year(self, collection_name):
        timestampField = 'released'

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
                    'year': {'$year': f'${timestampField}'}
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': '$year'
                    },
                    'movies_in_window': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1
                }
            }
        ]
       
        collection = self.database[collection_name]
        results = collection.aggregate(pipeline)
        results_list = list(results)

        # Flatten the _id field to create a single year string for easier handling
        for result in results_list:
            result['_id'] = f"{result['_id']['year']}"

        return results_list

