package c2pa

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

const c2paManifestTemplate = `
{
  "alg": "%s",
  "private_key": "%s",
  "sign_cert": "%s",
  "ta_url": "http://timestamp.digicert.com",

  "claim_generator": "Livepeer Studio",
  "assertions": [
    {
      "label": "c2pa.actions",
      "data": {
        "actions": [
          {
            "action": "c2pa.published"
          }
        ]
      }
    }
  ]
}
`

type C2PA struct {
	alg            string
	privateKeyPath string
	signCertPath   string
}

func NewC2PA(alg, privateKeyPath, signCertPath string) C2PA {
	return C2PA{
		alg:            alg,
		privateKeyPath: privateKeyPath,
		signCertPath:   signCertPath,
	}
}

func (c C2PA) c2paManifest() string {
	return fmt.Sprintf(c2paManifestTemplate, c.alg, c.privateKeyPath, c.signCertPath)
}

func (c C2PA) SignFile(inFile, outFile string) error {
	args := []string{
		inFile,
		"-c",
		c.c2paManifest(),
		"-f",
		"-o",
		outFile,
	}
	_, err := runCmd(exec.CommandContext(context.TODO(), "c2patool", args...))
	return err
}

func runCmd(cmd *exec.Cmd) (string, error) {
	var stdOut bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	err := cmd.Run()

	if len(stdErr.Bytes()) > 0 {
		return "", fmt.Errorf("failed creating C2PA Manifest: %s", stdErr.String())
	}

	return stdOut.String(), err
}
