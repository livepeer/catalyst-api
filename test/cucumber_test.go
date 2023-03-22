package cucumber

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/livepeer/catalyst-api/test/steps"
)

var baseURL = "http://localhost:4949"
var app *exec.Cmd

func init() {
	// Build the app
	buildApp := exec.Command(
		"go", "build",
		"-ldflags", "-X 'github.com/livepeer/catalyst-api/config.Version=cucumber-test-version'",
		"-o", "test/app",
	)
	buildApp.Dir = ".."
	if buildErr := buildApp.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Get minio
	getMinio := exec.Command("go", "get", "github.com/minio/minio@v0.0.0-20221229230822-b8943fdf19ac")
	getMinio.Dir = ".."
	if buildErr := getMinio.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Build minio
	buildMinio := exec.Command("go", "build", "-o", "test/minio", "github.com/minio/minio")
	buildMinio.Dir = ".."
	if buildErr := buildMinio.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Check which environment we're running in
	if strings.ToLower(os.Getenv("CUCUMBER_ENV")) == "canary" {
		baseURL = "http://TODO"
	}
}

func startApp() error {
	app = exec.Command("./app", "-private-bucket", "fixtures/playback-bucket")
	outfile, err := os.Create("logs/app.log")
	if err != nil {
		return err
	}
	defer outfile.Close()
	app.Stdout = outfile
	app.Stderr = outfile
	if err := app.Start(); err != nil {
		return err
	}

	// Wait for app to start - TODO: Be smarter and do this with a healthcheck
	time.Sleep(2 * time.Second)

	return nil
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	// Allows our steps to share data between themselves, e.g the response of the last HTTP call (which future steps can check is correct)
	var stepContext = steps.StepContext{
		BaseURL: baseURL,
	}

	ctx.Step(`^the VOD API is running$`, startApp)
	ctx.Step(`^the Client app is authenticated$`, stepContext.SetAuthHeaders)
	ctx.Step(`^an object store is available$`, stepContext.StartObjectStore)
	ctx.Step(`^Studio API server is running at "([^"]*)"$`, stepContext.StartStudioAPI)
	ctx.Step(`^Mist is running at "([^"]*)"$`, stepContext.StartMist)

	ctx.Step(`^I query the "([^"]*)" endpoint$`, stepContext.CreateGetRequest)
	ctx.Step(`^I submit to the "([^"]*)" endpoint with "([^"]*)"$`, stepContext.CreatePostRequest)
	ctx.Step(`^receive a response within "(\d+)" seconds$`, stepContext.CallAPI)
	ctx.Step(`^I get an HTTP response with code "([^"]*)"$`, stepContext.CheckHTTPResponseCode)
	ctx.Step(`^I get an HTTP response with code "([^"]*)" and the following body "([^"]*)"$`, stepContext.CheckHTTPResponseCodeAndBody)
	ctx.Step(`^my "((failed)|(successful))" request metrics get recorded$`, stepContext.CheckRecordedMetrics)
	ctx.Step(`^Mist receives a request for segmenting with "([^"]*)" second segments$`, stepContext.CheckMist)
	ctx.Step(`^the body matches file "([^"]*)"$`, stepContext.CheckHTTPResponseBodyFromFile)

	ctx.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		if app != nil && app.Process != nil {
			if err := app.Process.Kill(); err != nil {
				fmt.Println("Error while killing app process:", err.Error())
			}
			if err := app.Wait(); err != nil {
				if err.Error() != "signal: killed" {
					fmt.Println("Error while waiting for app to exit:", err.Error())
				}
			}
		}

		_ = stepContext.Mist.Shutdown(ctx)
		_ = stepContext.Studio.Shutdown(ctx)
		if stepContext.MinioAdmin != nil {
			_ = stepContext.MinioAdmin.ServiceStop(ctx)
		}
		return ctx, nil
	})
}

func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			TestingT:      t,
			Strict:        true,
			StopOnFailure: true,
			Format:        "cucumber",
			Paths:         []string{"features"},
		},
	}

	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}
