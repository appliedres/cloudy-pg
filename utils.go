package cloudypg

import (
	"encoding/json"
	"regexp"
)

func SanitizeConnectionString(connectionString string) string {

	// "postgres://%v:%v@%v:5432/%v"
	re := regexp.MustCompile(`(.*://.*:).*(@.*)`)

	return re.ReplaceAllString(connectionString, `$1********$2`)
}

func toByte(i any) ([]byte, error) {
	return json.Marshal(i)
}

func fromByte[T any](data []byte) (*T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return &v, err
}
