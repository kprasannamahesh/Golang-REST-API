package utility

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Transaction represents the transaction structure in MongoDB.
type Transaction struct {
	ID        primitive.ObjectID   `bson:"_id"`
	CreatedAt time.Time            `bson:"createdAt"`
	UserID    primitive.ObjectID   `bson:"userId"`
	RoundID   string               `bson:"roundId"`
	Type      string               `bson:"type"` // Wager or Payout
	Amount    primitive.Decimal128 `bson:"amount"`
	Currency  string               `bson:"currency"`
	USDAmount primitive.Decimal128 `bson:"usdAmount"`
}

// Configuration constants
const (
	NumRounds  = 2000000
	NumUsers   = 500
	BatchSize  = 10000
	Database   = "casino"
	Collection = "transactions"
)

// Supported currencies and conversion rates
var currencies = []string{"ETH", "BTC", "USDT"}
var exchangeRates = map[string]float64{
	"ETH":  3000.0,  // 1 ETH = $3000
	"BTC":  50000.0, // 1 BTC = $50000
	"USDT": 1.0,     // 1 USDT = $1
}

// Use a thread-safe random generator
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))
var userIDs []primitive.ObjectID

func init() {
	// Generate unique user IDs
	for i := 0; i < NumUsers; i++ {
		userIDs = append(userIDs, primitive.NewObjectID())
	}
}

// randomAmount generates a random transaction amount as Decimal128.
func randomAmount(min, max float64) primitive.Decimal128 {
	amount := min + rng.Float64()*(max-min)
	dec, _ := primitive.ParseDecimal128(fmt.Sprintf("%.2f", amount))
	return dec
}

// randomCurrency selects a random currency.
func randomCurrency() string {
	return currencies[rng.Intn(len(currencies))]
}

// randomDate generates a random date within the past year.
func randomDate() time.Time {
	now := time.Now().UTC()
	pastYear := now.AddDate(-1, 0, 0)
	randomSeconds := rng.Int63n(int64(now.Sub(pastYear).Seconds()))
	return pastYear.Add(time.Duration(randomSeconds) * time.Second)
}

// decimal128ToFloat64 converts a Decimal128 to float64 safely.
func decimal128ToFloat64(d primitive.Decimal128) float64 {
	str := d.String()                       // Convert Decimal128 to string
	val, err := strconv.ParseFloat(str, 64) // Parse string to float64
	if err != nil {
		log.Printf("Error converting Decimal128 to float64: %v", err)
		return 0.0
	}
	return val
}

// usdEquivalent calculates the USD equivalent of the given amount.
func usdEquivalent(amount primitive.Decimal128, currency string) primitive.Decimal128 {
	amountFloat := decimal128ToFloat64(amount)
	usdValue := amountFloat * exchangeRates[currency]
	usdDec, _ := primitive.ParseDecimal128(fmt.Sprintf("%.2f", usdValue))
	return usdDec
}

// generateRoundTransactions creates a wager and payout transaction.
func generateRoundTransactions(roundID string, userID primitive.ObjectID) []Transaction {
	currency := randomCurrency()
	wagerTime := randomDate()
	payoutTime := wagerTime.Add(time.Duration(rng.Intn(3600)+60) * time.Second) // Payout is later than wager

	wagerAmount := randomAmount(10, 500)
	wagerAmountFloat := decimal128ToFloat64(wagerAmount)

	payoutAmount := randomAmount(0.5*wagerAmountFloat, 2.0*wagerAmountFloat)

	return []Transaction{
		{
			ID:        primitive.NewObjectID(),
			CreatedAt: wagerTime,
			UserID:    userID,
			RoundID:   roundID,
			Type:      "Wager",
			Amount:    wagerAmount,
			Currency:  currency,
			USDAmount: usdEquivalent(wagerAmount, currency),
		},
		{
			ID:        primitive.NewObjectID(),
			CreatedAt: payoutTime,
			UserID:    userID,
			RoundID:   roundID,
			Type:      "Payout",
			Amount:    payoutAmount,
			Currency:  currency,
			USDAmount: usdEquivalent(payoutAmount, currency),
		},
	}
}

// LoadData inserts random transactions into MongoDB.
func LoadData(mongoURI string) {
	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("MongoDB Connection Error:", err)
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database(Database).Collection(Collection)

	var batch []interface{}
	var wg sync.WaitGroup

	for i := 0; i < NumRounds; i++ {
		roundID := primitive.NewObjectID().Hex()
		userID := userIDs[rng.Intn(len(userIDs))]
		transactions := generateRoundTransactions(roundID, userID)
		// fmt.Println("transactions:", transactions)

		for _, t := range transactions {
			batch = append(batch, t)
		}

		// Insert in batches
		if len(batch) >= BatchSize {
			wg.Add(1)
			go func(data []interface{}) {
				defer wg.Done()
				_, err := collection.InsertMany(context.TODO(), data)
				if err != nil {
					log.Fatal("Error inserting batch:", err)
				}
			}(batch)
			batch = nil // Clear batch
			fmt.Printf("Inserted %d rounds...\n", i+1)
		}
	}

	// Insert remaining records
	if len(batch) > 0 {
		_, err := collection.InsertMany(context.TODO(), batch)
		if err != nil {
			log.Fatal("Final batch insert error:", err)
		}
		fmt.Printf("Final insert: %d records\n", len(batch))
	}

	wg.Wait()
	fmt.Println("Data generation complete.")
}
