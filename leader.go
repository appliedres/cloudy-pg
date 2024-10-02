package cloudypg

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/jackc/pgx/v5"
)

var _ cloudy.LeaderElector = (*PgLeader)(nil)

type PgLeader struct {
	leader bool
	cfg    *PostgreSqlConfig
	conn   *pgx.Conn
}

func (pgl *PgLeader) IsLeader() bool {
	return pgl.leader
}

func NewPgLeader() *PgLeader {
	return &PgLeader{}
}

func (pgl *PgLeader) Connect(cfg interface{}) error {
	pgc := cfg.(*PostgreSqlConfig)
	if pgc == nil {
		return errors.New("No Connection")
	}
	return pgl.ConnectPg(pgc)
}

func (pgl *PgLeader) ConnectPg(cfg *PostgreSqlConfig) error {
	pgl.cfg = cfg
	conn, err := pgx.Connect(context.Background(), cfg.GetConnectionString())
	pgl.conn = conn
	return err
}

func (pgl *PgLeader) ConnectStr(connStr string) error {
	conn, err := pgx.Connect(context.Background(), connStr)
	pgl.conn = conn
	return err
}

func (pgl *PgLeader) Elect(onElection func(isLeader bool)) {
	for {
		leaderStatusC := make(chan bool)
		go pgl.runElection(leaderStatusC)

		// if first message on channel is true then current instance is leader
		isLeader := <-leaderStatusC
		pgl.leader = isLeader
		if isLeader {
			// start interval based tasks when pod becomes leader
			onElection(isLeader)

			// second message on channel will be will be sent when current pod is no longer the leader
			<-leaderStatusC

			// cancel all interval based jobs when no longer leader
			pgl.leader = false
			onElection(false)
		} else {
			onElection(false)
		}

		// wait for 1 minute before retrying election
		<-time.After(1 * time.Minute)
	}
}

func (pgl *PgLeader) runElection(leaderStatusC chan<- bool) {
	ctx := context.TODO()
	conn := pgl.conn

	defer func() {
		leaderStatusC <- false
	}()

	acquireLockQ := "SELECT pg_try_advisory_lock(10)"

	var becameLeader bool
	if err := conn.QueryRow(ctx, acquireLockQ).Scan(&becameLeader); err != nil {
		log.Default().Printf("leader election failed with error: %s", err.Error())
		return
	}
	if !becameLeader {
		log.Default().Printf("leader election failed")
		return
	}

	log.Default().Printf("new leader")
	leaderStatusC <- true

	for {
		<-time.After(1 * time.Minute)

		ctx2, _ := context.WithTimeout(context.Background(), 5*time.Second)

		// ensure that an advisory lock is held on the ID 10 by the same connection as the one running this query
		checkLockQ := "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND objid = 10"

		var lockCount int
		lockCountErr := conn.QueryRow(ctx2, checkLockQ).Scan(&lockCount)
		if lockCount == 0 {
			log.Default().Printf("no longer leader: %s", lockCountErr.Error())
			break
		}
	}
}
