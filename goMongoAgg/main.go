package main

func main() {
	var err error
	var m *MongoAgg

	if m, err = NewMongoAgg("sample_mflix", "movies"); err != nil {
		panic(err)
	}

	defer m.Disconnect()

	// title := "Back to the Future"
	// err = m.findAndPrintSample(title)

	println("Aggregating movies by year and quarter in MongoDB")
	if err = m.AggregateMoviesByQuarterInMongo(); err != nil {
		panic(err)
	}

	println("Filtering movies by year and quarter in MongoDB, then aggregating in Go")
	if err = m.aggregateMoviesByQuarterInMemory(); err != nil {
		panic(err)
	}
}
