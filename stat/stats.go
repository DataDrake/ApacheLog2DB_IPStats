package stat

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/DataDrake/ApacheLog2DB/source"
	"os"
	"sync"
)

var MAX_WORKERS = 10

func UpdateStats(db *sql.DB) error {
	c := make(chan *source.Source)
	wg := &sync.WaitGroup{}
	ss, err := source.ReadAll(db)
	for i := 0; i < MAX_WORKERS; i++ {
		go GetStat(wg, db, c)
	}
	if err == nil {
		for _, s := range ss {

			c <- s
		}
		wg.Wait()
	}
	return nil
}

func GetStat(wg *sync.WaitGroup, db *sql.DB, c chan *source.Source) {
	wg.Add(1)
	for s := range c {
		_, err := ReadOrCreate(db, s)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	wg.Done()
}

func GetAllStats(db *sql.DB) error {
	return errors.New("Feature not yet supported")
}
