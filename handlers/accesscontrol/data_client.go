package accesscontrol

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type DataAPICaller interface {
	QueryServerViewCount(userID string) (int32, error)
}

// DataClient is a client for the Livepeer Data API
type DataClient struct {
	Endpoint    string
	AccessToken string
}

type ViewCountResponse struct {
	ViewCount int32 `json:"viewCount"`
}

func NewDataClient(endpoint, accessToken string) *DataClient {
	return &DataClient{
		Endpoint:    endpoint,
		AccessToken: accessToken,
	}
}

func (d *DataClient) QueryServerViewCount(userID string) (int32, error) {
	if userID == "" {
		return 0, fmt.Errorf("userID is empty")
	}

	url := fmt.Sprintf("%s/views/internal/server/now?userId=%s", d.Endpoint, userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request, err=%v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", d.AccessToken))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to perform request, err=%v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code %d", res.StatusCode)
	}

	var viewCountRes []ViewCountResponse
	if err := json.NewDecoder(res.Body).Decode(&viewCountRes); err != nil {
		return 0, fmt.Errorf("failed to decode response body, err=%v", err)
	}

	if len(viewCountRes) != 1 {
		return 0, fmt.Errorf("view count does not contain exactly one element, viewCountRes=%v", viewCountRes)
	}

	return viewCountRes[0].ViewCount, nil
}
