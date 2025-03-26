package models

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const connectionString = "mongodb://admin:adminpassword@localhost:27017/"
const dbName = "casino"
const collectionName = "transaction"

var mongoClient *mongo.Client

func ConnectDatabase() {

	clientOption := options.Client().ApplyURI(connectionString)

	client, err := mongo.Connect(context.TODO(), clientOption)
	if err != nil {
		panic(err)
	}

	mongoClient = client

}

func RevenueQuery(fromDate, toDate time.Time) []bson.M {
	collection := mongoClient.Database(dbName).Collection(collectionName)

	// Define the aggregation pipeline
	matchStage := bson.D{{
		Key: "$match", Value: bson.D{
			{Key: "createdAt", Value: bson.D{
				{Key: "$gte", Value: fromDate},
				{Key: "$lte", Value: toDate},
			}},
		},
	}}

	groupStage := bson.D{{
		Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$currency"},
			{Key: "totalWagered", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.D{{Key: "$eq", Value: bson.A{"$type", "Wager"}}},
						"$amount",
						0,
					}},
				}},
			}},
			{Key: "totalPayout", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.D{{Key: "$eq", Value: bson.A{"$type", "Payout"}}},
						"$amount",
						0,
					}},
				}},
			}},
			{Key: "totalWageredUSD", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.D{{Key: "$eq", Value: bson.A{"$type", "Wager"}}},
						"$usdAmount",
						0,
					}},
				}},
			}},
			{Key: "totalPayoutUSD", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.D{{Key: "$eq", Value: bson.A{"$type", "Payout"}}},
						"$usdAmount",
						0,
					}},
				}},
			}},
		},
	}}

	projectStage := bson.D{{
		Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "currency", Value: "$_id"},
			{Key: "ggr", Value: bson.D{
				{Key: "$subtract", Value: bson.A{"$totalWagered", "$totalPayout"}},
			}},
			{Key: "ggrUSD", Value: bson.D{
				{Key: "$subtract", Value: bson.A{"$totalWageredUSD", "$totalPayoutUSD"}},
			}},
		},
	}}

	// Execute the aggregation pipeline
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, projectStage})
	if err != nil {
		log.Fatal("Failed to query database")
	}
	defer cursor.Close(context.Background())

	// Process results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Fatal("Failed to process results")
	}
	return results
}
