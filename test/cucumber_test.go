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

var baseURL = "http://127.0.0.1:18989"
var baseInternalURL = "http://127.0.0.1:17979"

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

	// Build minio
	buildMinio := exec.Command("go", "install", "github.com/minio/minio@v0.0.0-20221229230822-b8943fdf19ac")
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	buildMinio.Env = append(os.Environ(), fmt.Sprintf("GOBIN=%s", wd))
	buildMinio.Dir = ".."
	if buildErr := buildMinio.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Check which environment we're running in
	if strings.ToLower(os.Getenv("CUCUMBER_ENV")) == "canary" {
		baseURL = "http://TODO"
	}
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	// Allows our steps to share data between themselves, e.g the response of the last HTTP call (which future steps can check is correct)
	var stepContext = steps.StepContext{
		BaseURL:         baseURL,
		BaseInternalURL: baseInternalURL,
	}

	ctx.Step(`^the VOD API is running$`, stepContext.StartApp)
	ctx.Step(`^the Client app is authenticated$`, stepContext.SetAuthHeaders)
	ctx.Step(`^an object store is available$`, stepContext.StartObjectStore)
	ctx.Step(`^Studio API server is running at "([^"]*)"$`, stepContext.StartStudioAPI)
	ctx.Step(`^ffmpeg is available$`, stepContext.CheckFfmpeg)
	ctx.Step(`^a Broadcaster is running at "([^"]*)"$`, stepContext.StartBroadcaster)
	ctx.Step(`^a Postgres database is running$`, stepContext.StartDatabase)
	ctx.Step(`^a callback server is running at "([^"]*)"$`, stepContext.StartCallbackHandler)
	ctx.Step(`^I query the "([^"]*)" endpoint( with "([^"]*)")?$`, stepContext.CreateRequest)
	ctx.Step(`^I query the internal "([^"]*)" endpoint$`, stepContext.CreateGetRequestInternal)
	ctx.Step(`^I submit to the "([^"]*)" endpoint with "([^"]*)"$`, stepContext.CreatePostRequest)
	ctx.Step(`^I submit to the internal "([^"]*)" endpoint with "([^"]*)"$`, stepContext.CreatePostRequestInternal)
	ctx.Step(`^receive[s]? a response within "(\d+)" seconds$`, stepContext.CallAPI)
	ctx.Step(`^we wait for 5 seconds$`, stepContext.Wait)
	ctx.Step(`^(?:I|Mist) get[s]? an HTTP response with code "([^"]*)"$`, stepContext.CheckHTTPResponseCode)
	ctx.Step(`^I get an HTTP response with code "([^"]*)" and the following body "([^"]*)"$`, stepContext.CheckHTTPResponseCodeAndBody)
	ctx.Step(`^Mist calls the "([^"]*)" trigger with "([^"]*)" and ID "([^"]*)"$`, stepContext.CreateTriggerRequest)
	ctx.Step(`^my "(failed|successful)" (vod|playback) request metrics get recorded$`, stepContext.CheckRecordedMetrics)
	ctx.Step(`^a "([^"]*)" metric is recorded with a value of "([^"]*)"$`, stepContext.CheckMetricEqual)
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
	ctx.Step(`^a source copy (has|has not) been written to disk$`, stepContext.SourceCopyWrittenToDisk)
	ctx.Step(`^a row is written to the database containing the following values$`, stepContext.CheckDatabase)

	// Mediaconvert Steps
	ctx.Step(`^Mediaconvert is running at "([^"]*)"$`, stepContext.StartMediaconvert)
	ctx.Step(`^Mediaconvert receives a valid job creation request within "([^"]*)" seconds$`, stepContext.MediaconvertReceivesAValidRequestJobCreationRequest)

	ctx.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		if steps.App != nil && steps.App.Process != nil {
			if err := steps.App.Process.Kill(); err != nil {
				fmt.Println("Error while killing app process:", err.Error())
			}
			if err := steps.App.Wait(); err != nil {
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
		_ = stepContext.Mediaconvert.Shutdown(ctx)
		_ = stepContext.CallbackHandler.Shutdown(ctx)
		if stepContext.Database != nil {
			_ = stepContext.Database.Stop()
		}
		return ctx, nil
	})
}
