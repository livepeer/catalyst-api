package catalyst

import (
	"math"
	"sort"
)

// Rate the nodes as Good / Okay / Bad based on distance from the request
func geoScores(nodes []ScoredNode, requestLatitude, requestLongitude float64) []ScoredNode {
	// Calculate distance from request for each node
	for i := range nodes {
		// Convert latitude and longitude from degrees to radians
		lat1 := toRadians(requestLatitude)
		lon1 := toRadians(requestLongitude)
		lat2 := toRadians(nodes[i].GeoLatitude)
		lon2 := toRadians(nodes[i].GeoLongitude)

		// Haversine formula
		dlat := lat2 - lat1
		dlon := lon2 - lon1
		a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dlon/2)*math.Sin(dlon/2)
		c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

		// Distance in kilometers
		nodes[i].GeoDistance = earthRadius * c
	}

	// Order nodes by distance
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GeoDistance < nodes[j].GeoDistance
	})

	// Assign scores
	var baseDistance = nodes[0].GeoDistance
	var goodDistance = baseDistance + 1500
	var okayDistance = baseDistance + 7500
	for i := range nodes {
		if nodes[i].GeoDistance <= goodDistance {
			nodes[i].GeoScore = 2
		} else if nodes[i].GeoDistance <= okayDistance {
			nodes[i].GeoScore = 1
		} else {
			nodes[i].GeoScore = 0
		}
	}

	return nodes
}
