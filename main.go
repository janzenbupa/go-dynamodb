package dal

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var accessKey string
var secretKey string

type dynamoDbConfig struct {
	accessKey string `json:"string"`
	secretKey string `json":"string"`
}

func main() {
	configData, _ := os.Open("configuration.json")
	defer configData.Close()
	data, _ := ioutil.ReadAll(configData)

	dbAccess := dynamoDbConfig{}
	_ = json.Unmarshal(data, &dbAccess)
	accessKey = dbAccess.accessKey
	secretKey = dbAccess.secretKey
}

func buildConnection() dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			accessKey,
			secretKey,
			""),
	}))

	svc := dynamodb.New(sess)
	return *svc
}

func Write(brand string, model string, year int, price float32) {

	svc := buildConnection()

	input := &dynamodb.PutItemInput{
		TableName: aws.String("Bike"),
		Item: map[string]*dynamodb.AttributeValue{
			"Brand": {
				S: aws.String(brand),
			},
			"Model": {
				S: aws.String(model),
			},
			"Year": {
				N: aws.String(strconv.Itoa(year)),
			},
			"Price": {
				N: aws.String(strconv.FormatFloat(float64(price), 'f', -1, 32)),
			},
		},
	}
	_, err := svc.PutItem(input)
	if err != nil {
		return
	}
}

func ReadByBike(model string) ([]map[string]*dynamodb.AttributeValue, error) {
	svc := buildConnection()

	input := &dynamodb.QueryInput{
		TableName:              aws.String("Bike"),
		KeyConditionExpression: aws.String("Model = :model"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":model": {
				S: aws.String(model),
			},
		},
	}

	result, err := svc.Query(input)
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func Read(limit int64) ([]map[string]*dynamodb.AttributeValue, error) {
	svc := buildConnection()

	input := &dynamodb.ScanInput{
		TableName: aws.String("Bike"),
		Limit:     aws.Int64(limit),
	}

	result, err := svc.Scan(input)
	if err != nil {
		return nil, err
	}

	items := result.Items

	return items, nil
}
