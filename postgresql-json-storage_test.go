package cloudypg

/*
 To start postgres in docker

 docker run --name postgres -e POSTGRES_PASSWORD=admin -d -p 5432:5432 postgres

*/
import (
	"testing"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
)

// 	cmd := exec.Command("docker", "run", "--rm", "--name", "skycloud-test-postgres", "-e", "POSTGRES_PASSWORD=admin", "-d", "-p", "5432:5432", "postgres")

// var localPG = "postgres://postgres:admin@localhost:5432/postgres"
// var pgConfig = &PostgreSqlConfig{
// 	Connection: localPG,
// 	Database:   "sample",
// 	Table:      "sample",
// }

// func TestMain(m *testing.M) {
// 	// Write code here to run before tests
// 	created, err := cloudy.StartDocker("cloudy-test-postgres", []string{"-e", "POSTGRES_PASSWORD=admin", "-d", "-p", "5432:5432", "postgres"}, "")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Run tests
// 	exitVal := m.Run()

// 	// Write code here to run after tests
// 	if created {
// 		err = cloudy.ShutdownDocker("cloudy-test-postgres")
// 		if err != nil {
// 			panic(err)
// 		}
// 	}

// 	// Exit with exit value from tests
// 	os.Exit(exitVal)
// }

func TestPostrgreSqlJsonDataStore(t *testing.T) {
	pgConfig := CreateDefaultPostgresqlContainer(t)
	ctx := cloudy.StartContext()
	pgConfig.Table = "sample"
	ds := NewPostgreSqlJsonDataStore[datastore.TestItem](ctx, pgConfig)
	datastore.JsonDataStoreTest(t, ctx, ds)
}

func TestPostrgreSqlJsonDataStoreQuery(t *testing.T) {
	ctx := cloudy.StartContext()
	pgConfig := CreateDefaultPostgresqlContainer(t)
	pgConfig.Table = "sample"

	ds := NewPostgreSqlJsonDataStore[datastore.TestQueryItem](ctx, pgConfig)
	datastore.QueryJsonDataStoreTest(t, ctx, ds)
}
