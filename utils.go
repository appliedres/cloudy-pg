package cloudypg

import "regexp"

func SanitizeConnectionString(connectionString string) string {

	// "postgres://%v:%v@%v:5432/%v"
	re := regexp.MustCompile(`(.*://.*:).*(@.*)`)

	return re.ReplaceAllString(connectionString, `$1********$2`)
}
