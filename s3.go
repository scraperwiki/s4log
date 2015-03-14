package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/s3"
)

// TODO(pwaller): Not sure what the region is needed for.
// If anyone cares, maybe it's possible to figure this out programmatically?
// For example, via 169.254.169.254/latest/meta-data/placement/availability-zone
// const AWS_REGION = "eu-west-1"

var (
	setupS3  sync.Once
	s3client *s3.S3
)

func ensureS3Setup() {
	setupS3.Do(func() {
		aws.IAMClient.Timeout = 1 * time.Minute
		c := &s3.S3Config{&aws.Config{LogLevel: 1}}
		s3client = s3.New(c)
	})
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

	ensureS3Setup()

	log.Printf("PutObject: %q %q", s3c.bucket, name)

	out, req, err := s3client.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(s3c.bucket),
		Key:           aws.String(name),
		Body:          bytes.NewReader(buf),
		ContentLength: aws.Long(int64(len(buf))),
	})

	log.Printf("out, req = %v, %v", out, req)

	if err != nil {
		log.Printf("Failed to put to S3: %v", err)
		return len(buf)
	}
	return 0
}
