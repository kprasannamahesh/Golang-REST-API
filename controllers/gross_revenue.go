package controllers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kprasannamahesh/golang-rest-api/models"
	"go.mongodb.org/mongo-driver/bson"
)

func GrossGamingRevHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")

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

	var results []bson.M
	results = models.RevenueQuery(fromDate, toDate)

	c.JSON(http.StatusOK, results)
}
