package cucumber

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/cucumber/godog"
	"github.com/livepeer/catalyst-api/test/steps"
)

var baseURL = "http://127.0.0.1:8989"
var baseInternalURL = "http://127.0.0.1:7979"
var sourceOutputDir string
var app *exec.Cmd

func init() {
	// Build the app
	buildApp := exec.Command(
		"go", "build",
		"-ldflags", "-X 'github.com/livepeer/catalyst-api/config.Version=cucumber-test-version'",
		"-o", "test/app",
	)
	buildApp.Env = append(os.Environ(), "CGO_ENABLED=0")
	buildApp.Dir = ".."
	buildApp.Stderr = os.Stderr
	buildApp.Stdout = os.Stdout
	if buildErr := buildApp.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Build minio
	buildMinio := exec.Command("go", "install", "github.com/minio/minio@v0.0.0-20221229230822-b8943fdf19ac")
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	buildMinio.Env = append(os.Environ(), fmt.Sprintf("GOBIN=%s", wd), "CGO_ENABLED=1")
	buildMinio.Dir = ".."
	buildMinio.Stderr = os.Stderr
	buildMinio.Stdout = os.Stdout
	if buildErr := buildMinio.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Check which environment we're running in
	if strings.ToLower(os.Getenv("CUCUMBER_ENV")) == "canary" {
		baseURL = "http://TODO"
	}
}

func startApp() error {
	sourceOutputDir = fmt.Sprintf("file://%s/%s/", os.TempDir(), "livepeer/source")
	app = exec.Command("./app", "-private-bucket", "fixtures/playback-bucket", "-gate-url", "http://localhost:3000/api/access-control/gate", "-source-output", sourceOutputDir)
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

	// Wait for app to start
	steps.WaitForStartup(baseURL + "/ok")

	return nil
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	// Allows our steps to share data between themselves, e.g the response of the last HTTP call (which future steps can check is correct)
	var stepContext = steps.StepContext{
		BaseURL:         baseURL,
		BaseInternalURL: baseInternalURL,
		SourceOutputDir: sourceOutputDir,
	}

	ctx.Step(`^the VOD API is running$`, startApp)
	ctx.Step(`^the Client app is authenticated$`, stepContext.SetAuthHeaders)
	ctx.Step(`^an object store is available$`, stepContext.StartObjectStore)
	ctx.Step(`^Studio API server is running at "([^"]*)"$`, stepContext.StartStudioAPI)
	ctx.Step(`^ffmpeg is available$`, stepContext.CheckFfmpeg)
	ctx.Step(`^a Broadcaster is running at "([^"]*)"$`, stepContext.StartBroadcaster)
	ctx.Step(`^a callback server is running at "([^"]*)"$`, stepContext.StartCallbackHandler)

	ctx.Step(`^I query the "([^"]*)" endpoint( with "([^"]*)")?$`, stepContext.CreateRequest)
	ctx.Step(`^I query the internal "([^"]*)" endpoint$`, stepContext.CreateGetRequestInternal)
	ctx.Step(`^I submit to the "([^"]*)" endpoint with "([^"]*)"$`, stepContext.CreatePostRequest)
	ctx.Step(`^I submit to the internal "([^"]*)" endpoint with "([^"]*)"$`, stepContext.CreatePostRequestInternal)
	ctx.Step(`^receive a response within "(\d+)" seconds$`, stepContext.CallAPI)
	ctx.Step(`^we wait for 5 seconds$`, stepContext.Wait)
	ctx.Step(`^I get an HTTP response with code "([^"]*)"$`, stepContext.CheckHTTPResponseCode)
	ctx.Step(`^I get an HTTP response with code "([^"]*)" and the following body "([^"]*)"$`, stepContext.CheckHTTPResponseCodeAndBody)
	ctx.Step(`^my "(failed|successful)" (vod|playback) request metrics get recorded$`, stepContext.CheckRecordedMetrics)
	ctx.Step(`^the body matches file "([^"]*)"$`, stepContext.CheckHTTPResponseBodyFromFile)
	ctx.Step(`^the gate API will (allow|deny) playback$`, stepContext.SetGateAPIResponse)
	ctx.Step(`^the gate API will be called (\d+) times$`, stepContext.CheckGateAPICallCount)
	ctx.Step(`^the headers match$`, stepContext.CheckHTTPHeaders)
	ctx.Step(`^I receive a Request ID in the response body$`, stepContext.SaveRequestID)
	ctx.Step(`^"(\d+)" source segments are written to storage within "(\d+)" seconds$`, stepContext.AllOfTheSourceSegmentsAreWrittenToStorageWithinSeconds)
	ctx.Step(`^the source manifest is written to storage within "(\d+)" seconds and contains "(\d+)" segments$`, stepContext.TheSourceManifestIsWrittenToStorageWithinSeconds)
	ctx.Step(`^the gate API call was valid$`, stepContext.CheckGateAPICallValid)
	ctx.Step(`^the Broadcaster receives "(\d+)" segments for transcoding within "(\d+)" seconds$`, stepContext.BroadcasterReceivesSegmentsWithinSeconds)
	ctx.Step(`^"(\d+)" transcoded segments and manifests have been written to disk for profiles "([^"]*)" within "(\d+)" seconds$`, stepContext.TranscodedSegmentsWrittenToDiskWithinSeconds)
	ctx.Step(`^the source playback manifest is written to storage within "(\d+)" seconds$`, stepContext.SourcePlaybackManifestWrittenToDisk)
	ctx.Step(`^I receive a "([^"]*)" callback within "(\d+)" seconds$`, stepContext.CheckCallback)

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

		_ = stepContext.Studio.Shutdown(ctx)
		if stepContext.MinioAdmin != nil {
			_ = stepContext.MinioAdmin.ServiceStop(ctx)
		}
		_ = stepContext.Broadcaster.Shutdown(ctx)
		_ = stepContext.CallbackHandler.Shutdown(ctx)
		return ctx, nil
	})
}
