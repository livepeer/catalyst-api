package cucumber

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/livepeer/catalyst-api/test/steps"
)

var baseURL = "http://localhost:4949"
var app *exec.Cmd

func init() {
	// Build the app
	build := exec.Command("go", "build", "-o", "test/app")
	build.Dir = ".."
	if buildErr := build.Run(); buildErr != nil {
		panic(buildErr)
	}

	// Check which environment we're running in
	if strings.ToLower(os.Getenv("CUCUMBER_ENV")) == "canary" {
		baseURL = "http://TODO"
	}
}

func startApp() error {
	app = exec.Command("./app")
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
	ctx.Step(`^I call the "([^"]*)" endpoint and receive a response within "(\d+)" seconds$`, stepContext.CallAPI)
	ctx.Step(`^I receive an HTTP "(\d+)".*`, stepContext.CheckHTTPResponseCode)
	ctx.Step(`^I receive an HTTP body '([^']*)'$`, stepContext.CheckHTTPResponseBody)

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
		return ctx, nil
	})
}
