package storage_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws/request"
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

func (c *testPrivateS3Client) HeadBucketWithContext(ctx context.Context, input *s3.HeadBucketInput, opts ...request.Option) (*s3.HeadBucketOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.headBucketReturnValue, nil
}
func (c *testPrivateS3Client) GetObjectWithContext(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.getObjectReturnValue, nil
}
func (c *testPrivateS3Client) PutObjectWithContext(ctx context.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.putObjectReturnValue, nil
}

func TestPrivateS3Test(t *testing.T) {
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", 1,"https://mybucket.s3.amazonaws.com")

	assert.NoError(t, s.Test(context.Background()))

	client.returnError = errors.New("boom")

	assert.EqualError(t, s.Test(context.Background()), "boom")
}

func TestPrivateS3Get(t *testing.T) {
	ctx := context.Background()
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", 1,"https://mybucket.s3.amazonaws.com")

	client.getObjectReturnValue = &s3.GetObjectOutput{
		ContentType: aws.String("text/plain"),
		Body:        ioutil.NopCloser(bytes.NewReader([]byte(`HELLOWORLD`))),
	}

	contentType, contents, err := s.Get(ctx, "/foo/things")
	assert.NoError(t, err)
	assert.Equal(t, "text/plain", contentType)
	assert.Equal(t, []byte(`HELLOWORLD`), contents)

	client.returnError = errors.New("boom")

	_, _, err = s.Get(ctx, "/foo/things")
	assert.EqualError(t, err, "error getting S3 object: boom")
}

func TestPrivateS3Put(t *testing.T) {
	ctx := context.Background()
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", 1,"https://mybucket.s3.amazonaws.com")

	url, err := s.Put(ctx, "/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.NoError(t, err)
	assert.Equal(t, "https://mybucket.s3.amazonaws.com/foo/things", url)

	client.returnError = errors.New("boom")

	_, err = s.Put(ctx, "/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.EqualError(t, err, "error putting S3 object: boom")
}

func TestPrivateS3BatchPut(t *testing.T) {

	ctx := context.Background()
	client := &testPrivateS3Client{}
	s := storage.NewPrivateS3(client, "mybucket", 10, "https://mybucket.s3.amazonaws.com")

	uploads := []*storage.Upload{
		&storage.Upload{
			Path:        "https://mybucket.s3.amazonaws.com/foo/thing1",
			Body:        []byte(`HELLOWORLD`),
			ContentType: "text/plain",
			ACL:         s3.BucketCannedACLPrivate,
		},
		&storage.Upload{
			Path:        "https://mybucket.s3.amazonaws.com/foo/thing2",
			Body:        []byte(`HELLOWORLD2`),
			ContentType: "text/plain",
			ACL:         s3.BucketCannedACLPrivate,
		},
	}

	err := s.BatchPut(ctx, uploads)
	assert.NoError(t, err)

	assert.NotEmpty(t, uploads[0].URL)
	assert.NotEmpty(t, uploads[1].URL)

	// try again, with a single thread and throwing an error
	s = storage.NewS3(client, "mybucket", 1)
	client.returnError = errors.New("boom")

	uploads[0].URL = ""
	uploads[1].URL = ""

	err = s.BatchPut(ctx, uploads)

	assert.Error(t, err)

	assert.Empty(t, uploads[0].URL)
	assert.Empty(t, uploads[1].URL)
	assert.NotEmpty(t, uploads[0].Error)
}
