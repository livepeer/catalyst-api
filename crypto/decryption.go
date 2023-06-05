package crypto

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"strings"

	"github.com/d1str0/pkcs7"
	"github.com/golang/glog"
)

type DecryptionKeys struct {
	DecryptKey   *rsa.PrivateKey
	EncryptedKey string
}

func LoadPrivateKey(privateKeyBase64 string) (*rsa.PrivateKey, error) {
	privateKey, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("file-decrypt: error decoding private key: %v", err)
	}

	block, _ := pem.Decode(privateKey)
	if block == nil {
		return nil, fmt.Errorf("file-decrypt: error decoding PEM block from private key")
	}

	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("file-decrypt: error parsing private key: %v", err)
	}

	return priv, nil
}

func ValidateKeyPair(pub string, privkey rsa.PrivateKey) (bool, error) {
	pubkey, err := base64.StdEncoding.DecodeString(pub)
	if err != nil {
		glog.Fatalf("Error decoding base64 encoded key: %v", err)
		return false, err
	}
	block, _ := pem.Decode(pubkey)
	if block == nil {
		glog.Fatalf("failed to parse PEM block containing the public key")
		err := fmt.Errorf("failed to parse PEM block containing the public key")
		return false, err
	}

	publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		glog.Fatalf("Error parsing vod decrypt public key: %v", err)
		return false, err
	}
	if !publicKey.Equal(privkey.Public()) {
		glog.Fatalf("Public key does not match private key")
		return false, err
	}
	return true, nil
}

// Decrypts a file encrypted with AES (key length depends on input) in CBC block
// chaining mode and PKCS#7 padding. The provided key must be encoded in base16,
// and the first block of the input is the IV. The output is a pipe reader that
// can be used to stream the decrypted file.
func DecryptAESCBC(reader io.Reader, privateKey *rsa.PrivateKey, encryptedKeyFile string) (io.ReadCloser, error) {

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(reader, iv); err != nil {
		return nil, fmt.Errorf("error reading iv from input: %w", err)
	}

	return DecryptAESCBCWithIV(io.NopCloser(reader), privateKey, encryptedKeyFile, iv)
}

func DecryptAESCBCWithIV(reader io.ReadCloser, privateKey *rsa.PrivateKey, encryptedKeyB64 string, iv []byte) (io.ReadCloser, error) {

	encryptedKey, err := base64.StdEncoding.DecodeString(encryptedKeyB64)
	if err != nil {
		glog.Errorf("Error decoding base64 encoded key: %v", err)
	}

	// Decrypt the key with the RSA private key
	key, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privateKey, encryptedKey, nil)

	if err != nil {
		glog.Errorf("Error decrypting key: %v", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("error creating cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer reader.Close()
		defer pipeWriter.Close()

		if err := decryptReaderTo(reader, pipeWriter, decrypter); err != nil {
			pipeWriter.CloseWithError(err)
		}
	}()

	return pipeReader, nil
}

func decryptReaderTo(readerRaw io.Reader, writer io.Writer, decrypter cipher.BlockMode) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	blockSize := decrypter.BlockSize()
	buffer := make([]byte, 256*blockSize)
	reader := bufio.NewReaderSize(readerRaw, 2*len(buffer))

	for {
		n, err := io.ReadFull(reader, buffer)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			// unexpected EOF is returned when input ends before the buffer size
			return err
		} else if n == 0 {
			break
		}

		chunk := buffer[:n]

		// we add some dummy bytes in the end to make a full block and still try to
		// decrypt. this is non standard and can only ever happen on the last chunk.
		needsFakePadding := n%blockSize != 0
		if needsFakePadding {
			glog.Warningf("Input is not a multiple of AES block size, not padded with PKCS#7")
			fakePaddingSize := blockSize - (n % blockSize)
			chunk = buffer[:n+fakePaddingSize]
		}

		decrypter.CryptBlocks(chunk, chunk)

		if needsFakePadding {
			// remove the fake padding
			chunk = chunk[:n]
		} else if _, peekErr := reader.Peek(1); peekErr == io.EOF {
			// this means we're on the last chunk, so handle padding
			lastBlock := chunk[len(chunk)-blockSize:]

			unpadded, err := pkcs7.Unpad(lastBlock)
			if err != nil {
				return fmt.Errorf("bad input PKCS#7 padding: %w", err)
			}

			padSize := len(lastBlock) - len(unpadded)
			chunk = chunk[:len(chunk)-padSize]
		}

		if _, err := writer.Write(chunk); err != nil {
			return err
		}
	}

	return nil
}

func ConvertToSpki(pemB64PublicKey string) (string, error) {
	// Decode from base64
	pemPublicKey, err := base64.StdEncoding.DecodeString(pemB64PublicKey)

	if err != nil {
		glog.Errorf("Error decoding base64 encoded key: %v", err)
		return "", err
	}

	block, _ := pem.Decode([]byte(pemPublicKey))
	if block == nil {
		err := fmt.Errorf("failed to parse PEM block containing the public key")
		return "", err
	}

	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		glog.Errorf("failed to parse RSA public key: %v", err)
		return "", err
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		glog.Errorf("failed to marshal RSA public key: %v", err)
		return "", err
	}

	pubPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	})

	pubPEMStr := string(pubPEM)

	// Remove header and footer
	spkiPubKeyBase64 := strings.Join(strings.Split(pubPEMStr, "\n")[1:len(strings.Split(pubPEMStr, "\n"))-2], "")

	// Encode to base64
	spkiPubKeyBase64 = base64.StdEncoding.EncodeToString([]byte(spkiPubKeyBase64))

	return spkiPubKeyBase64, nil
}
