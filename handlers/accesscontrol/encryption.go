package accesscontrol

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
)

type EncryptionHandlersCollection struct {
	publicKey     string
	spkiPublicKey string
	nodeName      string
}

func NewEncryptionHandlersCollection(cli config.Cli, spkiPublicKey string) *EncryptionHandlersCollection {
	return &EncryptionHandlersCollection{
		publicKey:     cli.VodDecryptPublicKey,
		spkiPublicKey: spkiPublicKey,
		nodeName:      cli.NodeName,
	}
}

func (ec *EncryptionHandlersCollection) PublicKeyHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		responseData := map[string]string{
			"public_key":      ec.publicKey,
			"spki_public_key": ec.spkiPublicKey,
			"node_name":       ec.nodeName,
		}

		res, err := json.Marshal(responseData)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = w.Write(res)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
