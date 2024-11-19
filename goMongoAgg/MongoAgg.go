package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoAgg struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// NewMongoAgg creates a new instance of MongoAgg and connects to the MongoDB cluster
func NewMongoAgg(databaseName string, collectionName string) (*MongoAgg, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
		return nil, err
	}

	uri := os.Getenv("ATLAS_URI")
	docs := "www.mongodb.com/docs/drivers/go/current/"
	if uri == "" {
		log.Fatal("Set your 'MONGODB_URI' environment variable. " +
			"See: " + docs +
			"usage-examples/#environment-variable")
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	collection := client.Database(databaseName).Collection(collectionName)
	return &MongoAgg{
		client:     client,
		collection: collection,
	}, nil
}

// Disconnect closes the connection to the MongoDB cluster
func (m *MongoAgg) disconnect() error {
	if err := m.client.Disconnect(context.Background()); err != nil {
		return err
	}
	return nil
}

// Find and print one test document from the collection
func (m *MongoAgg) findAndPrintSample(title string) error {
	var item *mongo.SingleResult
	var result bson.M

	if item = m.collection.FindOne(context.TODO(), bson.D{{"title", title}}); item.Err() != nil {
		return item.Err()
	}

	if err := item.Decode(&result); err != nil {
		return err
	}

	if jsonData, err := json.MarshalIndent(result, "", "    "); err != nil {
		return err
	} else {
		fmt.Printf("%s\n", jsonData)
		return nil
	}
}

// Function to aggregate movies by year
// This function uses MongoDB's native aggregation pipeline to group movies by year and quarter
func (m *MongoAgg) aggregateMoviesByYear() error {
	pipeline := mongo.Pipeline{
		bson.D{{"$match", bson.D{
			{"released", bson.D{{"$exists", true}, {"$ne", nil}}},
		}}},
		bson.D{{"$addFields", bson.D{
			{"year", bson.D{{"$year", "$released"}}},
			{"month", bson.D{{"$month", "$released"}}},
			{"quarter", bson.D{{"$toInt", bson.D{{"$ceil", bson.D{{"$divide", bson.A{bson.D{{"$month", "$released"}}, 3}}}}}}}},
		}}},
		bson.D{{"$match", bson.D{
			{"year", bson.D{{"$exists", true}, {"$ne", nil}}},
			{"year", bson.D{{"$gte", 2010}}},
			{"year", bson.D{{"$lte", 2015}}},
		}}},
		bson.D{{"$group", bson.D{
			{"_id", bson.D{
				{"year", "$year"},
				{"quarter", "$quarter"},
			}},
			{"movies_in_window", bson.D{{"$sum", 1}}},
		}}},
		bson.D{{"$sort", bson.D{
			{"_id.year", 1},
			{"_id.quarter", 1},
		}}},
	}

	cursor, err := m.collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.TODO(), &results); err != nil {
		return err
	}

	if jsonData, err := json.MarshalIndent(results, "", "    "); err != nil {
		return err
	} else {
		fmt.Printf("%s\n", jsonData)
		return nil
	}
}

// Function to aggregate movies by year
// In this version, we use Mongo's aggregation pipeline to filter movies by year and quarter
// But do the aggregation in Go code
func (m *MongoAgg) aggregateMoviesByYearInGo() error {
	pipeline := mongo.Pipeline{
		bson.D{{"$match", bson.D{
			{"released", bson.D{{"$exists", true}, {"$ne", nil}}},
		}}},
		bson.D{{"$addFields", bson.D{
			{"year", bson.D{{"$year", "$released"}}},
			{"month", bson.D{{"$month", "$released"}}},
			{"quarter", bson.D{{"$toInt", bson.D{{"$ceil", bson.D{{"$divide", bson.A{bson.D{{"$month", "$released"}}, 3}}}}}}}},
		}}},
		bson.D{{"$match", bson.D{
			{"year", bson.D{{"$exists", true}, {"$ne", nil}}},
			{"year", bson.D{{"$gte", 2010}}},
			{"year", bson.D{{"$lte", 2015}}},
		}}},
	}

	cursor, err := m.collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	// Iterate over the cursor and accumulate results into map.
	aggResults := make(map[string]int)

	for cursor.Next(context.Background()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			return err
		}

		year := result["year"].(int32)
		quarter := result["quarter"].(int32)
		key := fmt.Sprintf("%d-Q%d", year, quarter)
		aggResults[key]++
	}

	// Print the results
	for key, value := range aggResults {
		fmt.Printf("%s: %d\n", key, value)
	}
	return nil
}

func main() {
	m, err := NewMongoAgg("sample_mflix", "movies")
	if err != nil {
		panic(err)
	}

	defer m.disconnect()

	// title := "Back to the Future"
	// err = m.findAnagdPrintSample(title)
	// err = m.aggregateMoviesByYear()
	err = m.aggregateMoviesByYearInGo()
	if err != nil {
		fmt.Println("Error: ", err)
	}
}
