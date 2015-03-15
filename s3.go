package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/s3"
)

var (
	// If logging is needed
	// config = &s3.S3Config{&aws.Config{LogLevel: 1}}
	PutObject = s3.New(nil).PutObject
)

func init() {
	// Set the API timeout to 1 minute.
	aws.IAMClient.Timeout = 1 * time.Minute
}

// Writes the committed buffer to a file with the
// hostname and timestamp in the path.
type S3Committer struct {
	bucket, prefix string
	hostname       string
}

func (s3c S3Committer) Commit(buf []byte) int {
	timestamp := time.Now().Format(time.RFC3339Nano)
	name := fmt.Sprintf("%s/%s-%s.txt", s3c.prefix, s3c.hostname, timestamp)

	log.Printf("Committing %d bytes to %q", len(buf), name)

	_, err := PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(name),
		Body:   bytes.NewReader(buf),
	})

	if err != nil {
		log.Printf("Failed to put to S3: %v", err)
		return len(buf)
	}
	return 0
}
