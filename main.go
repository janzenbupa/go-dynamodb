package dal

import (
	"encoding/json"
	"io/ioutil"
	"os"

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

func Write(bankAccountId string, merchant string) {

	svc := buildConnection()

	input := &dynamodb.PutItemInput{
		TableName: aws.String("BankAccount"),
		Item: map[string]*dynamodb.AttributeValue{
			"BankAccountId": {
				S: aws.String(bankAccountId),
			},
			"Merchant": {
				S: aws.String(merchant),
			},
		},
	}
	_, err := svc.PutItem(input)
	if err != nil {
		return
	}
}

func ReadByBankAccount(bankAccountId string) (map[string]*dynamodb.AttributeValue, error) {
	svc := buildConnection()

	input := &dynamodb.GetItemInput{
		TableName: aws.String("BankAccount"),
		Key: map[string]*dynamodb.AttributeValue{
			"BankAccountId": {
				S: aws.String(bankAccountId),
			},
		},
	}

	result, err := svc.GetItem(input)
	if err != nil {
		return nil, err
	}

	if result.Item == nil {
		return nil, nil
	}

	return result.Item, nil
}

func Read(limit int64) ([]map[string]*dynamodb.AttributeValue, error) {
	svc := buildConnection()

	input := &dynamodb.ScanInput{
		TableName: aws.String("BankAccount"),
		Limit:     aws.Int64(limit),
	}

	result, err := svc.Scan(input)
	if err != nil {
		return nil, err
	}

	items := result.Items

	return items, nil
}
