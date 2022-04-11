package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

type privateS3Storage struct {
	publicBucketUrl string
	client          S3Client
	bucket          string
}

// NewS3 creates a new S3 storage service
func NewPrivateS3(client S3Client, bucket string, publicBucketUrl string) Storage {
	return &privateS3Storage{client: client, bucket: bucket, publicBucketUrl: publicBucketUrl}
}

func (s *privateS3Storage) Name() string {
	return "PrivateS3"
}

// Test tests whether our S3 client is properly configured
func (s *privateS3Storage) Test() error {
	_, err := s.client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	return err
}

func (s *privateS3Storage) Get(path string) (string, []byte, error) {
	out, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return "", nil, errors.Wrapf(err, "error getting S3 object")
	}

	contents, err := ioutil.ReadAll(out.Body)
	if err != nil {
		return "", nil, errors.Wrapf(err, "error reading S3 object")
	}

	return aws.StringValue(out.ContentType), contents, nil
}

// Put writes the passed in file to the bucket with the passed in content type
func (s *privateS3Storage) Put(path string, contentType string, contents []byte) (string, error) {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Body:        bytes.NewReader(contents),
		Key:         aws.String(path),
		ContentType: aws.String(contentType),
		ACL:         aws.String(s3.BucketCannedACLPrivate),
	})
	if err != nil {
		return "", errors.Wrapf(err, "error putting S3 object")
	}

	return s.url(path), nil
}

func (s *privateS3Storage) url(path string) string {
	return fmt.Sprintf("%s%s", s.publicBucketUrl, path)
}
