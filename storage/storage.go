package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var client *s3.Client
var bucket string

func Init() {
	bucket = os.Getenv("B2_BUCKET")
	keyID := os.Getenv("B2_KEY_ID")
	appKey := os.Getenv("B2_APPLICATION_KEY")
	endpoint := os.Getenv("B2_ENDPOINT")

	client = s3.NewFromConfig(aws.Config{
		Region: "us-east-005",
		Credentials: credentials.NewStaticCredentialsProvider(keyID, appKey, ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: "https://" + endpoint,
				}, nil
			},
		),
	})
}

func Write(key string, data []byte) error {
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("b2 write %s: %w", key, err)
	}
	return nil
}

func Read(key string) ([]byte, error) {
	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("b2 read %s: %w", key, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("b2 read body %s: %w", key, err)
	}
	return data, nil
}

func Delete(key string) error {
	_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("b2 delete %s: %w", key, err)
	}
	return nil
}

func Size(key string) (int64, error) {
	resp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("b2 size %s: %w", key, err)
	}
	return *resp.ContentLength, nil
}