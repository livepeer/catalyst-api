package clients

import (
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Signer interface {
	PresignS3(bucket, key string) (string, error)
}

type S3Client struct {
	s3 *s3.S3
}

func (c *S3Client) PresignS3(bucket, key string) (string, error) {
	req, _ := c.s3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return req.Presign(5 * time.Minute)
}
