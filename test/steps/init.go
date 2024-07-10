package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/julienschmidt/httprouter"
	"github.com/minio/madmin-go"
)

const (
	minioAddress = "127.0.0.1:9000"
	minioKey     = "minioadmin"
)

func (s *StepContext) StartStudioAPI(listen string) error {
	router := httprouter.New()
	router.POST("/cb", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		_, _ = io.WriteString(w, "")
	})
	router.POST("/api/access-control/gate", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
		gateRequest := make(map[string]interface{})
		err = json.Unmarshal(body, &gateRequest)
		if err != nil {
			panic(err)
		}
		s.GateAPICallType = gateRequest["type"]
		s.GateAPICallCount++
		w.Header().Set("Cache-Control", "max-age=600, stale-while-revalidate=900")
		w.WriteHeader(s.GateAPIStatus)
	})
	router.GET("/ok", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusOK)
	})

	s.Studio = http.Server{Addr: listen, Handler: router}
	go func() {
		_ = s.Studio.ListenAndServe()
	}()

	WaitForStartup("http://" + listen + "/ok")

	return nil
}

func WaitForStartup(url string) {
	retries := backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 5)
	err := backoff.Retry(func() error {
		_, err := http.Get(url)
		return err
	}, retries)
	if err != nil {
		panic(err)
	}
}

func (s *StepContext) StartObjectStore() error {
	app := exec.Command("./minio", "server", "--address", minioAddress, path.Join(os.TempDir(), "catalyst-minio"))
	outfile, err := os.Create("logs/minio.log")
	if err != nil {
		return err
	}
	defer outfile.Close()
	app.Stdout = outfile
	app.Stderr = outfile
	if err := app.Start(); err != nil {
		return err
	}

	admin, err := madmin.New(minioAddress, minioKey, minioKey, false)
	if err != nil {
		return err
	}
	s.MinioAdmin = admin

	minioClient, err := minio.New(minioAddress, &minio.Options{
		Creds:  credentials.NewStaticV4(minioKey, minioKey, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}

	WaitForStartup("http://" + minioAddress + "/minio/health/live")

	ctx := context.Background()

	// Create buckets if they do not exist.
	buckets := []string{"rec-bucket", "rec-fallback-bucket", "source"}
	for _, bucket := range buckets {
		exists, err := minioClient.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("failed to check if bucket exists: %w", err)
		}
		if exists {
			continue
		}
		err = minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}

	// Set bucket policy to allow anonymous download.
	for _, bucket := range buckets {
		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Effect": "Allow",
					"Principal": {"AWS": ["*"]},
					"Action": ["s3:GetObject"],
					"Resource": ["arn:aws:s3:::%s/*"]
				}
			]
		}`, bucket)

		err = minioClient.SetBucketPolicy(ctx, bucket, policy)
		if err != nil {
			return err
		}
	}

	// populate recording bucket
	files := []string{"fixtures/tiny.m3u8", "fixtures/seg-0.ts", "fixtures/seg-1.ts", "fixtures/seg-2.ts"}
	for _, file := range files {
		_, err := minioClient.FPutObject(ctx, "rec-bucket", path.Base(file), file, minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}

	// populate recording fallback bucket
	files = []string{"fixtures/rec-fallback-bucket/tiny.m3u8", "fixtures/rec-fallback-bucket/seg-3.ts"}
	for _, file := range files {
		_, err := minioClient.FPutObject(ctx, "rec-fallback-bucket", path.Base(file), file, minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
