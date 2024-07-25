package config

import "strings"

// GetStorageBackupURL returns the backup URL for the given URL or an empty string if it doesn't exist. The backup URL
// is found by checking the `StorageFallbackURLs` global config map. If any of the primary URL prefixes (keys in map)
// are in `urlStr`, it is replaced with the backup URL prefix (associated value of the key in the map).
func GetStorageBackupURL(urlStr string) string {
	for primary, backup := range StorageFallbackURLs {
		if strings.HasPrefix(urlStr, primary) {
			return strings.Replace(urlStr, primary, backup, 1)
		}
	}
	return ""
}
