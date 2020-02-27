package dynamodbservice

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	log "github.com/sirupsen/logrus"
)

type DynamodbService interface {
	ListTables(listTablesInput *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	//PutTable(item dynamodb.Item, tableName string) (*dynamodb.PutItemOutput, error)
	GetItem(tableName string, hashKey string, sortKey string, hashKeyValue string, sortKeyValue string) (*dynamodb.GetItemOutput, error)
	FilterQueryExpression(ConditionBuilder expression.ConditionBuilder, projectionBuilder expression.ProjectionBuilder, tableName string) (*dynamodb.QueryOutput, error)
	FilterScanExpression(ConditionBuilder expression.ConditionBuilder, projectionBuilder expression.ProjectionBuilder, tableName string) (*dynamodb.ScanOutput, error)
	Update(updateItemInput *dynamodb.UpdateItemInput, tableName string) (*dynamodb.UpdateItemOutput, error)
}

type dynamodbService struct {
	dbSvc *dynamodb.DynamoDB
}

func InitializeDynamoDBSvc(region, endpoint string) (DynamodbService, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000")})
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	var dbSvc *dynamodb.DynamoDB
	dbSvc = dynamodb.New(sess)
	return &dynamodbService{dbSvc: dbSvc}, nil
}

func (d *dynamodbService) ListTables(listTablesInput *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {

	result, err := d.dbSvc.ListTables(listTablesInput)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	log.Println("Tables:")
	for _, table := range result.TableNames {
		log.Println(*table)
	}
	return result, nil
}

// func (d *dynamodbService) PutTable(item dynamodb.Item, tableName string) (*dynamodb.PutItemOutput, error) {

// 	av, err := dynamodbattribute.MarshalMap(item)
// 	if err != nil {
// 		fmt.Println("Got error marshalling new movie item:")
// 		fmt.Println(err.Error())
// 		os.Exit(1)
// 	}

// 	input := &dynamodb.PutItemInput{
// 		Item:      av,
// 		TableName: aws.String(tableName),
// 	}
// 	putItemOutput, err := d.dbSvc.PutItem(input)
// 	if err != nil {
// 		fmt.Println("Got error calling PutItem:")
// 		fmt.Println(err.Error())
// 		os.Exit(1)
// 	}

// 	return putItemOutput, nil

// }

func (d *dynamodbService) GetItem(tableName string, hashKey string, sortKey string, hashKeyValue string, sortKeyValue string) (*dynamodb.GetItemOutput, error) {

	result, err := d.dbSvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			hashKey: {
				S: aws.String(hashKeyValue),
			},
			sortKey: {
				S: aws.String(sortKeyValue),
			},
		},
	})
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return result, nil
}

func (d *dynamodbService) FilterQueryExpression(ConditionBuilder expression.ConditionBuilder, projectionBuilder expression.ProjectionBuilder, tableName string) (*dynamodb.QueryOutput, error) {

	expr, err := expression.NewBuilder().WithFilter(ConditionBuilder).WithProjection(projectionBuilder).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	params := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	result, err := d.dbSvc.Query(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		os.Exit(1)
	}
	return result, nil
}

func (d *dynamodbService) FilterScanExpression(ConditionBuilder expression.ConditionBuilder, projectionBuilder expression.ProjectionBuilder, tableName string) (*dynamodb.ScanOutput, error) {

	expr, err := expression.NewBuilder().WithFilter(ConditionBuilder).WithProjection(projectionBuilder).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	result, err := d.dbSvc.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		os.Exit(1)
	}
	//log.Infof("result %v", result)
	return result, nil
}

func (d *dynamodbService) Update(updateItemInput *dynamodb.UpdateItemInput, tableName string) (*dynamodb.UpdateItemOutput, error) {

	updateItemOutput, err := d.dbSvc.UpdateItem(updateItemInput)
	if err != nil {
		log.Infof(err.Error())
		return nil, err
	}

	log.Infof("Successfully updated %s", tableName)
	log.Infof("%v", updateItemOutput)
	return updateItemOutput, nil
}
