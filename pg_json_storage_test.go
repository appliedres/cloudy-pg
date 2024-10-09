package cloudypg

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
)

type testData struct {
	ID        string `json:"id"`
	TimeStamp strfmt.DateTime
	Count     int64
	Level1    *Level1 `json:"level1"`
}

type Level1 struct {
	Value  string  `json:"value"`
	Level2 *Level2 `json:"level2"`
}
type Level2 struct {
	Value string `json:"level2value"`
}

func RandomInt(max int64, min ...int64) int64 {
	minv := big.NewInt(0)
	if len(min) == 1 {
		minv = big.NewInt(min[0])
	}
	maxv := big.NewInt(max) // 121 because it's exclusive
	randomBigInt, _ := rand.Int(rand.Reader, maxv.Sub(maxv, minv))
	randomNum := randomBigInt.Int64() + minv.Int64()
	return randomNum
}

func randomTestData() (*testData, []byte) {
	td := &testData{
		ID:        cloudy.GenerateId("TD", 20),
		TimeStamp: strfmt.DateTime(time.Now()),
		Count:     RandomInt(10000),
		Level1: &Level1{
			Value: "Embed",
			Level2: &Level2{
				Value: "Level2",
			},
		},
	}

	data, _ := json.Marshal(td)
	return td, data
}

func TestQueryTable(t *testing.T) {
	cfg := CreateDefaultPostgresqlContainer(t)
	cfg.Table = "testdata"
	ctx := cloudy.StartContext()
	ucfg := &UntypedPostgreSqlConfig{
		PostgreSqlConfig: *cfg,
	}
	ds := NewUntypedPostgreSqlJsonDataStore(ctx, ucfg)
	err := ds.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	td1, tdb1 := randomTestData()
	td2, tdb2 := randomTestData()
	td3, tdb3 := randomTestData()
	td4, tdb4 := randomTestData()

	data := [][]byte{tdb1, tdb2, tdb3, tdb4}
	keys := []string{td1.ID, td2.ID, td3.ID, td4.ID}

	err = ds.SaveAll(ctx, data, keys)
	if err != nil {
		t.Fatal(err)
	}

	q1 := datastore.NewQuery()
	q1.Conditions.Equals("level1.level2.level2value", "Level2")

	r1, err := ds.Query(ctx, q1)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(r1), 4)

	q := datastore.NewQuery()
	q.Colums = []string{"id", "TimeStamp", "Count"}

	results, err := ds.QueryTable(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range results {
		fmt.Printf("%v\t%v\t%v\n", row[0], row[1], row[2])
	}

}
