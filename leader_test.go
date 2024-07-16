package cloudypg

import (
	"fmt"
	"testing"
	"time"
)

func TestLeader(t *testing.T) {
	cfg := CreatePostgresqlContainer(t)

	leader := NewPgLeader()
	err := leader.Connect(cfg)
	if err != nil {
		t.Fatal("Error")
	}

	go leader.Elect(func(isLeader bool) {
		fmt.Printf("IS LEADER : %v\n", isLeader)
	})

	cnt := 0
	for {
		fmt.Printf("Is Leader: %v\n", leader.IsLeader())
		time.Sleep(2 * time.Second)
		cnt++
		if cnt > 100 {
			break
		}
	}

}
