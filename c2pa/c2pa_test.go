package c2pa

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"testing"
)

func TestSign(t *testing.T) {
	_, err := exec.LookPath("c2patool")
	if err != nil {
		fmt.Println("No c2patool installed, test skipped")
		return
	}

	outFile := "test/tiny_output_signed.mp4"
	c := NewC2PA("es256", "test/es256_private.key", "test/es256_certs.pem")
	defer os.Remove(outFile) // nolint:errcheck

	err = c.SignFile("test/tiny.mp4", outFile, "Tiny", "")

	require.Nil(t, err)
	out, err := runCmd(exec.CommandContext(context.TODO(), "c2patool", outFile))
	require.Nil(t, err)
	assert.Contains(t, out, "\"action\": \"c2pa.published\"")
}

func TestSignWithParent(t *testing.T) {
	_, err := exec.LookPath("c2patool")
	if err != nil {
		fmt.Println("No c2patool installed, test skipped")
		return
	}

	outFile := "test/tiny_cut_output_signed.mp4"
	c := NewC2PA("es256", "test/es256_private.key", "test/es256_certs.pem")
	defer os.Remove(outFile) // nolint:errcheck

	err = c.SignFile("test/tiny_cut.mp4", outFile, "Tiny", "test/tiny_signed.mp4")

	require.Nil(t, err)
	out, err := runCmd(exec.CommandContext(context.TODO(), "c2patool", outFile))
	require.Nil(t, err)
	assert.Contains(t, out, "\"action\": \"c2pa.published\"")
	assert.Contains(t, out, "\"relationship\": \"parentOf\"")
}

func TestSign_NotExistingPrivateKey(t *testing.T) {
	_, err := exec.LookPath("c2patool")
	if err != nil {
		fmt.Println("No c2patool installed, test skipped")
		return
	}

	c := NewC2PA("es256", "some/path/notexisting", "test/es256_certs.pem")
	err = c.SignFile("test/tiny.mp4", "test/tiny_signed.mp4", "Tiny", "")
	require.ErrorContains(t, err, "No such file or directory")
}

func TestSign_NotExistingSigningCert(t *testing.T) {
	_, err := exec.LookPath("c2patool")
	if err != nil {
		fmt.Println("No c2patool installed, test skipped")
		return
	}

	c := NewC2PA("es256", "test/es256_private.key", "some/path/notexisting")
	err = c.SignFile("test/tiny.mp4", "test/tiny_signed.mp4", "Tiny", "")
	require.ErrorContains(t, err, "No such file or directory")
}
