package main

import (
	"fmt"
	"os"

	"github.com/Linaf/awsservices/dynamodbservice"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	log "github.com/sirupsen/logrus"
)

type Music struct {
	Artist     string
	SongTitle  string
	Performing string
}

func main() {
	artist := "Jano"
	filt := expression.Name("Artist").Equal(expression.Value(artist))
	proj := expression.NamesList(expression.Name("Artist"), expression.Name("SongTitle"), expression.Name("performing"))
	tableName := "Music"

	dynamodbService, err := dynamodbservice.InitializeDynamoDBSvc("us-east-1", "http://localhost:8000")
	if err != nil {
		log.Fatal("error initializing dynamodb client : %v", err)
	}
	//list table
	dynamodbService.ListTables(&dynamodb.ListTablesInput{})

	//FilterQueryExpression
	// queryoutput, err := dynamodbService.FilterQueryExpression(filt, proj, tableName)
	// if err != nil {
	// 	log.Fatal("error getting music for Artist %s", artist)
	// 	return
	// }

	//FilterScanExpression
	scanoutput, err := dynamodbService.FilterScanExpression(filt, proj, tableName)
	if err != nil {
		log.Fatal("error getting music for Artist %s", artist)
		return
	}
	//fmt.Printf(" response %v", scanoutput)

	for _, i := range scanoutput.Items {
		music := Music{}
		err = dynamodbattribute.UnmarshalMap(i, &music)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			os.Exit(1)
		}
		fmt.Printf(" Music: %v", music)

	}

	//update Item

	artist2 := "Betty G"
	songTitle := "Sheger"
	performing := "Sheraton Addis Addis"

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(performing),
			},
		},
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Artist": {
				S: aws.String(artist2),
			},
			"SongTitle": {
				S: aws.String(songTitle),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set performing = :r"),
	}
	fmt.Printf("%v", input)

	_, err = dynamodbService.Update(input, tableName)
	if err != nil {
		fmt.Printf("error updating %v", err)
		return
	}

	getItemOutput, err := dynamodbService.GetItem(tableName, "Artist", "SongTitle", "Betty G", "Sheger")
	if err != nil {
		fmt.Printf("error updating %v", err)
		return
	}

	fmt.Printf("error updating %v", getItemOutput)

}
