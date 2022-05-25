package cloudypg

/*
 To start postgres in docker

 docker run --name postgres -e POSTGRES_PASSWORD=admin -d -p 5432:5432 postgres

*/
import (
	"os"
	"testing"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/tests"
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
	created, err := tests.StartDocker("cloudy-test-postgres", []string{"-e", "POSTGRES_PASSWORD=admin", "-d", "-p", "5432:5432", "postgres"}, "")
	if err != nil {
		panic(err)
	}

	// Run tests
	exitVal := m.Run()

	// Write code here to run after tests
	if created {
		err = tests.ShutdownDocker("cloudy-test-postgres")
		if err != nil {
			panic(err)
		}
	}

	// Exit with exit value from tests
	os.Exit(exitVal)
}

func TestPostrgreSqlJsonDataStore(t *testing.T) {
	ctx := cloudy.StartContext()
	ds := NewPostgreSqlJsonDataStore[tests.TestItem](ctx, pgConfig)
	tests.JsonDataStoreTest(t, ctx, ds)
}

func TestPostrgreSqlJsonDataStoreQuery(t *testing.T) {
	ctx := cloudy.StartContext()
	ds := NewPostgreSqlJsonDataStore[tests.TestQueryItem](ctx, pgConfig)
	tests.QueryJsonDataStoreTest(t, ctx, ds)
}
