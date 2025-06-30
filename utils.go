package cloudypg

import (
	"bytes"
	"encoding/json"
	"regexp"
)

func SanitizeConnectionString(connectionString string) string {

	// "postgres://%v:%v@%v:5432/%v"
	re := regexp.MustCompile(`(.*://.*:).*(@.*)`)

	return re.ReplaceAllString(connectionString, `$1********$2`)
}

func toByte(i any) ([]byte, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	// Remove null bytes from the data
	data = RemoveNullBytes(data)
	return data, nil
}

func fromByte[T any](data []byte) (*T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return &v, err
}

func RemoveNullBytes(data []byte) []byte {
	var buf bytes.Buffer
	for _, b := range data {
		if b != 0 {
			buf.WriteByte(b)
		}
	}
	return buf.Bytes()
}
