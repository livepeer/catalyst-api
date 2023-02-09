package clients

import (
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

type S3 interface {
	PresignS3(bucket, key string) (string, error)
	GetObject(bucket, key string) (*s3.GetObjectOutput, error)
}

type S3Client struct {
	S3 *s3.S3
}

func (c *S3Client) PresignS3(bucket, key string) (string, error) {
	req, _ := c.S3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return req.Presign(60 * time.Minute)
}

func (c *S3Client) GetObject(bucket, key string) (*s3.GetObjectOutput, error) {
	return c.S3.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
}
