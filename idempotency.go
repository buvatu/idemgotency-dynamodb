package idempotency

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"log/slog"
	"os"
	"sync"
	"time"
)

type idempotencyConfig struct {
	DebugMode         bool                     `yaml:"debugMode"`
	Region            string                   `yaml:"region"`
	UseDefaultConfig  bool                     `yaml:"useDefaultConfig"`
	AccessKeyID       string                   `yaml:"accessKeyId"`
	SecretAccessKey   string                   `yaml:"secretAccessKey"`
	ServiceName       string                   `yaml:"serviceName"`
	DefaultLockTime   time.Duration            `yaml:"defaultLockTime"`
	OperationLockTime map[string]time.Duration `yaml:"operationLockTime"`
}

type idempotencyExecutor struct {
	config         idempotencyConfig
	dynamoDBClient *dynamodb.Client
	logger         *slog.Logger
}

func Init(configFilePath string) error {
	// Read the input config file
	configData, err := os.ReadFile(configFilePath)
	if err != nil {
		return errors.New("[Config] fail to open config file: " + configFilePath + ". Caused by: " + err.Error())
	}

	// convert yaml file to idempotencyConfig
	var config idempotencyConfig
	if err = yaml.Unmarshal(configData, &config); err != nil {
		return errors.New("[Config] fail to parse config file: " + configFilePath + ". Caused by: " + err.Error())
	}

	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(config.Region)}

	if config.DebugMode {
		opts = append(opts, awsconfig.WithBaseEndpoint("http://localhost:8000"))
	}
	if !config.UseDefaultConfig {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, "")))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return errors.New("[Config] fail to load aws config. Caused by: " + err.Error())
	}

	executor = &idempotencyExecutor{
		config:         config,
		dynamoDBClient: dynamodb.NewFromConfig(cfg),
		logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	return nil
}

// Initialize the idempotencyExecutor
var executor *idempotencyExecutor

func ExecuteExactlyOne[T any](operationName string, idempotencyKey uuid.UUID, idempotentFunction func() (T, error)) (T, error) {
	var executionResult T

	if executor == nil {
		return executionResult, errors.New("the idempotency instance is not initialized")
	}

	var (
		err           error
		idempotencyID string
		lockID        = uuid.New().String()
		lockedAt      time.Time
		lockedUntil   time.Time
		lockTime      = executor.config.OperationLockTime[operationName]
	)
	if lockTime == 0 {
		lockTime = executor.config.DefaultLockTime
	}

	// Get idempotencyID as a MD5 string
	hash := md5.Sum([]byte(executor.config.ServiceName + "-" + operationName + "-" + idempotencyKey.String()))
	idempotencyID = hex.EncodeToString(hash[:])

	exists, storedResult, currentLockID, currentLockedUntil := executor.getStoredIdempotentOperationResult(idempotencyID)

	gob.Register(executionResult)

	// If the operation is already executed with the same idempotencyKey, return the result immediately
	if exists && storedResult != nil {
		dec := gob.NewDecoder(bytes.NewBuffer(storedResult))
		if err = dec.Decode(&executionResult); err != nil {
			return executionResult, err
		}
		return executionResult, nil
	}

	// If the operation is still locked, return the error
	if currentLockID != "" && currentLockedUntil.After(time.Now()) {
		return executionResult, errors.New("the operation is still locked")
	}

	lockedAt = time.Now()
	lockedUntil = lockedAt.Add(lockTime)

	if currentLockID == "" { // Insert lock
		if err = executor.insertLock(idempotencyID, operationName, idempotencyKey, lockID, lockedAt, lockedUntil); err != nil {
			return executionResult, err
		}
	} else {
		if err = executor.updateLock(idempotencyID, currentLockID, lockID, lockedAt, lockedUntil); err != nil {
			return executionResult, err
		}
	}

	executionResult, err = idempotentFunction()

	if err != nil {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = executor.saveFailOperation(idempotencyID, operationName, lockID, lockedAt, lockedUntil, err.Error())
		}()
		wg.Wait()
		return executionResult, err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(executionResult); err != nil {
		executor.logger.Error("fail to encode execution result", "idempotencyID", idempotencyID, "error", err)
		return executionResult, err
	}

	if err = executor.saveIdempotentOperationResult(idempotencyID, lockID, buf.Bytes()); err != nil {
		return executionResult, err
	}

	return executionResult, nil
}

// Get idempotent result
func (executor *idempotencyExecutor) getStoredIdempotentOperationResult(idempotencyID string) (exists bool, storedResult []byte, lockID string, lockedUntil time.Time) {
	resp, err := executor.dynamoDBClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("idempotent_operation_result"),
		Key: map[string]types.AttributeValue{
			"idempotency_id": &types.AttributeValueMemberS{Value: idempotencyID},
		},
		ProjectionExpression: aws.String("lock_id, locked_until, execution_result"),
	})
	if err != nil {
		executor.logger.Error("fail to get stored idempotent operation result", "idempotencyID", idempotencyID, "error", err)
		return false, nil, "", time.Time{}
	}
	if resp.Item == nil || len(resp.Item) == 0 { // Record does not exist
		return false, nil, "", time.Time{}
	}

	if av, ok := resp.Item["lock_id"].(*types.AttributeValueMemberS); ok {
		lockID = av.Value
	}
	if av, ok := resp.Item["locked_until"].(*types.AttributeValueMemberS); ok {
		lockedUntil, _ = time.Parse(time.RFC3339Nano, av.Value)
	}
	if av, ok := resp.Item["execution_result"].(*types.AttributeValueMemberB); ok {
		storedResult = av.Value
	}

	return true, storedResult, lockID, lockedUntil
}

func (executor *idempotencyExecutor) insertLock(idempotencyID string, operationName string, idempotencyKey uuid.UUID, lockID string, lockedAt time.Time, lockedUntil time.Time) error {
	_, err := executor.dynamoDBClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("idempotent_operation_result"),
		Item: map[string]types.AttributeValue{
			"idempotency_id":  &types.AttributeValueMemberS{Value: idempotencyID},
			"service_name":    &types.AttributeValueMemberS{Value: executor.config.ServiceName},
			"operation_name":  &types.AttributeValueMemberS{Value: operationName},
			"idempotency_key": &types.AttributeValueMemberS{Value: idempotencyKey.String()},
			"lock_id":         &types.AttributeValueMemberS{Value: lockID},
			"locked_at":       &types.AttributeValueMemberS{Value: lockedAt.Format(time.RFC3339Nano)},
			"locked_until":    &types.AttributeValueMemberS{Value: lockedUntil.Format(time.RFC3339Nano)},
		},
		ConditionExpression: aws.String("attribute_not_exists(idempotency_id)"),
	})
	if err != nil {
		executor.logger.Error("fail to insert idempotent operation result as a lock record", "idempotencyID", idempotencyID, "lockID", lockID, "error", err)
		return err
	}
	return nil
}

func (executor *idempotencyExecutor) updateLock(idempotencyID string, currentLockID string, newLockID string, lockedAt time.Time, lockedUntil time.Time) error {
	_, err := executor.dynamoDBClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("idempotent_operation_result"),
		Key: map[string]types.AttributeValue{
			"idempotency_id": &types.AttributeValueMemberS{Value: idempotencyID},
		},
		UpdateExpression:    aws.String("set lock_id = :newLockID, locked_at = :lockedAt, locked_until = :lockedUntil"),
		ConditionExpression: aws.String("lock_id = :currentLockID"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newLockID":     &types.AttributeValueMemberS{Value: newLockID},
			":currentLockID": &types.AttributeValueMemberS{Value: currentLockID},
			":lockedAt":      &types.AttributeValueMemberS{Value: lockedAt.Format(time.RFC3339Nano)},
			":lockedUntil":   &types.AttributeValueMemberS{Value: lockedUntil.Format(time.RFC3339Nano)},
		},
	})
	if err != nil {
		executor.logger.Error("fail to update idempotent operation result as a lock record", "idempotencyID", idempotencyID, "currentLockID", currentLockID, "error", err)
		return err
	}
	return nil
}

func (executor *idempotencyExecutor) saveIdempotentOperationResult(idempotencyID string, lockID string, encodedExecutionResult []byte) error {
	_, err := executor.dynamoDBClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("idempotent_operation_result"),
		Key: map[string]types.AttributeValue{
			"idempotency_id": &types.AttributeValueMemberS{Value: idempotencyID},
		},
		UpdateExpression:    aws.String("set released_at = :releasedAt, execution_result = :executionResult"),
		ConditionExpression: aws.String("lock_id = :lockID"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lockID":          &types.AttributeValueMemberS{Value: lockID},
			":releasedAt":      &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339Nano)},
			":executionResult": &types.AttributeValueMemberB{Value: encodedExecutionResult},
		},
	})
	if err != nil {
		executor.logger.Error("fail to save idempotent operation result", "idempotencyID", idempotencyID, "lockID", lockID, "error", err)
		return err
	}
	return nil
}

func (executor *idempotencyExecutor) saveFailOperation(idempotencyID string, operationName string, lockID string, lockedAt time.Time, lockedUntil time.Time, errorMessage string) error {
	_, err := executor.dynamoDBClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("fail_operation_result"),
		Item: map[string]types.AttributeValue{
			"idempotency_id": &types.AttributeValueMemberS{Value: idempotencyID},
			"service_name":   &types.AttributeValueMemberS{Value: executor.config.ServiceName},
			"operation_name": &types.AttributeValueMemberS{Value: operationName},
			"lock_id":        &types.AttributeValueMemberS{Value: lockID},
			"locked_at":      &types.AttributeValueMemberS{Value: lockedAt.Format(time.RFC3339Nano)},
			"locked_until":   &types.AttributeValueMemberS{Value: lockedUntil.Format(time.RFC3339Nano)},
			"inserted_at":    &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339Nano)},
			"error_message":  &types.AttributeValueMemberS{Value: errorMessage},
		}})
	if err != nil {
		executor.logger.Error("fail to insert fail operation result", "idempotencyID", idempotencyID, "lockID", lockID, "error", err)
		return err
	}
	return nil
}
