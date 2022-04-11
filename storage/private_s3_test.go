package storage_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/nyaruka/gocommon/storage"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

type testPrivateS3Client struct {
	returnError           error
	headBucketReturnValue *s3.HeadBucketOutput
	getObjectReturnValue  *s3.GetObjectOutput
	putObjectReturnValue  *s3.PutObjectOutput
}

func (c *testPrivateS3Client) HeadBucket(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.headBucketReturnValue, nil
}
func (c *testPrivateS3Client) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.getObjectReturnValue, nil
}
func (c *testPrivateS3Client) PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.putObjectReturnValue, nil
}

func TestPrivateS3Test(t *testing.T) {
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", "https://mybucket.s3.amazonaws.com")

	assert.NoError(t, s.Test())

	client.returnError = errors.New("boom")

	assert.EqualError(t, s.Test(), "boom")
}

func TestPrivateS3Get(t *testing.T) {
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", "https://mybucket.s3.amazonaws.com")

	client.getObjectReturnValue = &s3.GetObjectOutput{
		ContentType: aws.String("text/plain"),
		Body:        ioutil.NopCloser(bytes.NewReader([]byte(`HELLOWORLD`))),
	}

	contentType, contents, err := s.Get("/foo/things")
	assert.NoError(t, err)
	assert.Equal(t, "text/plain", contentType)
	assert.Equal(t, []byte(`HELLOWORLD`), contents)

	client.returnError = errors.New("boom")

	_, _, err = s.Get("/foo/things")
	assert.EqualError(t, err, "error getting S3 object: boom")
}

func TestPrivateS3Put(t *testing.T) {
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", "https://mybucket.s3.amazonaws.com")

	url, err := s.Put("/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.NoError(t, err)
	assert.Equal(t, "https://mybucket.s3.amazonaws.com/foo/things", url)

	client.returnError = errors.New("boom")

	_, err = s.Put("/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.EqualError(t, err, "error putting S3 object: boom")
}
