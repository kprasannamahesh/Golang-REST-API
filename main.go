package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client

func main() {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://admin:adminpassword@localhost:27017/")

	// Connect to MongoDB
	var err error
	client, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	log.Println("Successfully connected to MongoDB!")

	// Initialize Gin router
	r := gin.Default()

	// Define route
	r.GET("/gross_gaming_rev", grossGamingRevHandler)
	r.GET("/daily_wager_volume", dailyWagerVolumeHandler)
	r.GET("/user/:user_id/wager_percentile", userWagerPercentileHandler)

	// Start the server
	r.Run(":8000")
}

func grossGamingRevHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")

	// Parse dates
	fromDate, err := time.Parse("2006-01-02", from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'from' date"})
		return
	}
	toDate, err := time.Parse("2006-01-02", to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'to' date"})
		return
	}

	// Ensure the client is not nil
	if client == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "MongoDB client is not initialized"})
		return
	}

	// Get the database and collection
	database := client.Database("casino")
	collection := database.Collection("transactions")

	// Ensure the collection is not nil
	if collection == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Collection is not initialized"})
		return
	}

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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query database"})
		return
	}
	defer cursor.Close(context.Background())

	// Process results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process results"})
		return
	}

	c.JSON(http.StatusOK, results)
}

func dailyWagerVolumeHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")

	fmt.Println("Query Parameters - From:", from, "To:", to)

	// Parse dates
	fromDate, err := time.Parse("2006-01-02", from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'from' date"})
		return
	}
	toDate, err := time.Parse("2006-01-02", to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'to' date"})
		return
	}

	fmt.Println("Parsed Dates - From:", fromDate, "To:", toDate)

	if client == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "MongoDB client is not initialized"})
		return
	}

	// Get the database and collection
	database := client.Database("casino")
	collection := database.Collection("transactions")

	// Ensure the collection is not nil
	if collection == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Collection is not initialized"})
		return
	}

	fmt.Println("Collection Name:", collection.Name())

	// Define the aggregation pipeline
	matchStage := bson.D{{
		Key: "$match", Value: bson.D{
			{Key: "createdAt", Value: bson.D{
				{Key: "$gte", Value: fromDate},
				{Key: "$lte", Value: toDate},
			}},
			{Key: "type", Value: "Wager"},
		},
	}}

	groupStage := bson.D{{
		Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "day", Value: bson.D{
					{Key: "$dateToString", Value: bson.D{
						{Key: "format", Value: "%Y-%m-%d"},
						{Key: "date", Value: "$createdAt"},
					}},
				}},
				{Key: "currency", Value: "$currency"},
			}},
			{Key: "totalAmount", Value: bson.D{
				{Key: "$sum", Value: "$amount"},
			}},
			{Key: "totalUSDAmount", Value: bson.D{
				{Key: "$sum", Value: "$usdAmount"},
			}},
		},
	}}

	projectStage := bson.D{{
		Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "day", Value: "$_id.day"},
			{Key: "currency", Value: "$_id.currency"},
			{Key: "totalAmount", Value: 1},
			{Key: "totalUSDAmount", Value: 1},
		},
	}}

	// Execute the aggregation pipeline
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, projectStage})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query database"})
		return
	}
	defer cursor.Close(context.Background())

	// Process results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process results"})
		return
	}

	if len(results) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No data found for the specified date range"})
		return
	}

	c.JSON(http.StatusOK, results)
}

func userWagerPercentileHandler(c *gin.Context) {
	userID := c.Param("user_id")
	from := c.Query("from")
	to := c.Query("to")
	log.Println("user id", userID)

	fmt.Println("Query Parameters - User ID:", userID, "From:", from, "To:", to)

	// Parse dates
	fromDate, err := time.Parse("2006-01-02", from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'from' date"})
		return
	}
	toDate, err := time.Parse("2006-01-02", to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'to' date"})
		return
	}

	fmt.Println("Parsed Dates - From:", fromDate, "To:", toDate)

	database := client.Database("casino")
	collection := database.Collection("transactions")

	// Ensure the collection is not nil
	if collection == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Collection is not initialized"})
		return
	}

	fmt.Println("Collection Name:", collection.Name())

	// Define the aggregation pipeline
	matchStage := bson.D{{
		Key: "$match", Value: bson.D{
			{Key: "createdAt", Value: bson.D{
				{Key: "$gte", Value: fromDate},
				{Key: "$lte", Value: toDate},
			}},
			{Key: "type", Value: "Wager"},
		},
	}}

	groupStage := bson.D{{
		Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$userId"},
			{Key: "totalUSDAmount", Value: bson.D{
				{Key: "$sum", Value: "$usdAmount"},
			}},
		},
	}}

	// Execute the aggregation pipeline
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query database"})
		return
	}
	defer cursor.Close(context.Background())

	// Process results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process results"})
		return
	}

	if len(results) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No data found for the specified date range"})
		return
	}

	// Calculate percentile
	// var userTotalUSDAmount decimal.Decimal128
	var totalUsers int
	var userRank int

	userIDObj, err := primitive.ObjectIDFromHex(userID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	for rank, result := range results {
		totalUsers++
		if result["_id"] == userIDObj {
			// userTotalUSDAmount = result["totalUSDAmount"].(primitive.Decimal128)
			userRank = rank + 1
		}
	}

	// // Sort results by totalUSDAmount
	// sort.Slice(results, func(i, j int) bool {
	// 	return results[i]["totalUSDAmount"].(primitive.Decimal128).Cmp(results[j]["totalUSDAmount"].(primitive.Decimal128)) == -1
	// 	//results[i]["totalUSDAmount"].(primitive.Decimal128).Float64() < results[j]["totalUSDAmount"].(primitive.Decimal128).Float64()
	// })

	// Find user's rank
	for rank, result := range results {
		if result["_id"] == userIDObj {
			userRank = rank + 1
			break
		}
	}

	percentile := (float64(userRank) / float64(totalUsers)) * 100

	c.JSON(http.StatusOK, gin.H{"percentile": percentile})
}
