//
// Copyright 2016-2017 Bryan T. Meyers <bmeyers@datadrake.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package stat

import (
	"errors"
	"fmt"
	"github.com/DataDrake/ApacheLog2DB/source"
	"github.com/jmoiron/sqlx"
	"os"
	"sync"
)

// MaxWorkers indicates the maximum number of workers to break up work across
const MaxWorkers = 10

// UpdateStats creates workers and then hands them off tasks one by one
func UpdateStats(db *sqlx.DB) error {
	c := make(chan *source.Source)
	wg := &sync.WaitGroup{}
	ss, err := source.ReadAll(db)
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go GetStat(wg, db, c)
	}
	if err == nil {
		for _, s := range ss {
			c <- s
		}
		close(c)
		wg.Wait()
	}
	return nil
}

// GetStat is a worker task that reads sources off of a channel and updates their stats one by one
func GetStat(wg *sync.WaitGroup, db *sqlx.DB, c chan *source.Source) {
	for {
		s, more := <-c
		if !more {
			wg.Done()
			break
		}
		_, err := ReadOrCreate(db, s)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	wg.Done()
}

// GetAllStats will retrieve all stats in an exported format
func GetAllStats(db *sqlx.DB) error {
	return errors.New("Feature not yet supported")
}
