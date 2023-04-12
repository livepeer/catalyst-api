package events

import (
	"encoding/json"
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/go-livepeer/eth"
)

type fullMessage struct {
	Data      apitypes.TypedData `json:"data"`
	Signature string             `json:"sig"`
}

func Sign(typedData apitypes.TypedData) string {
	id := new(big.Int).SetInt64(int64(80001))
	am, err := eth.NewAccountManager(ethcommon.HexToAddress(""), ".", id, "secretpassword")
	if err != nil {
		panic(err)
	}
	err = am.Unlock("secretpassword")
	if err != nil {
		panic(err)
	}
	b, err := am.SignTypedData(typedData)
	if err != nil {
		panic(fmt.Errorf("error signing typed data: %w", err))
	}
	message := fullMessage{
		Data:      typedData,
		Signature: fmt.Sprintf("%s", hexutil.Bytes(b)),
	}
	b2, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return string(b2)
}
