package cloudypg

/*
 To start postgres in docker

 docker run --name postgres -e POSTGRES_PASSWORD=admin -d -p 5432:5432 postgres

*/
import (
	"os"
	"testing"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
	"github.com/stretchr/testify/assert"
)

// 	cmd := exec.Command("docker", "run", "--rm", "--name", "skycloud-test-postgres", "-e", "POSTGRES_PASSWORD=admin", "-d", "-p", "5432:5432", "postgres")

var localPG = "postgres://postgres:admin@localhost:5432/postgres"
var pgConfig = &PostgreSqlConfig{
	Connection: localPG,
	Database:   "sample",
	Table:      "sample",
}

func TestMain(m *testing.M) {
	// Write code here to run before tests
	created, err := cloudy.StartDocker("cloudy-test-postgres", []string{"-e", "POSTGRES_PASSWORD=admin", "-d", "-p", "5432:5432", "postgres"}, "")
	if err != nil {
		panic(err)
	}

	// Run tests
	exitVal := m.Run()

	// Write code here to run after tests
	if created {
		err = cloudy.ShutdownDocker("cloudy-test-postgres")
		if err != nil {
			panic(err)
		}
	}

	// Exit with exit value from tests
	os.Exit(exitVal)
}

func TestPostrgreSqlJsonDataStore(t *testing.T) {
	ctx := cloudy.StartContext()
	ds := NewPostgreSqlJsonDataStore[datastore.TestItem](ctx, pgConfig)
	datastore.JsonDataStoreTest(t, ctx, ds)
}

func TestPostrgreSqlJsonDataStoreDynamic(t *testing.T) {
	ctx := cloudy.StartContext()

	menv := cloudy.NewMapEnvironment()
	menv.Set("DS_DRIVER", PostgresProviderID)
	menv.Set("DS_CONNECTION", pgConfig.Connection)
	menv.Set("DS_TABLE", pgConfig.Table)
	menv.Set("DS_DATABASE", pgConfig.Database)
	env := cloudy.NewEnvironment(cloudy.NewTieredEnvironment(menv))

	ds, err := datastore.JsonDataStoreProviders.NewFromEnv(env, "DRIVER")
	tds := datastore.NewTypedStore[datastore.TestItem](ds)
	assert.Nil(t, err, "Not expecting an error")

	datastore.JsonDataStoreTest(t, ctx, tds)
}

func TestPostrgreSqlJsonDataStoreQuery(t *testing.T) {
	ctx := cloudy.StartContext()
	ds := NewPostgreSqlJsonDataStore[datastore.TestQueryItem](ctx, pgConfig)
	datastore.QueryJsonDataStoreTest(t, ctx, ds)
}
