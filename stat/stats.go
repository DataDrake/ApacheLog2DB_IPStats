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

var MAX_WORKERS = 10

func UpdateStats(db *sqlx.DB) error {
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

func GetStat(wg *sync.WaitGroup, db *sqlx.DB, c chan *source.Source) {
	wg.Add(1)
	for s := range c {
		_, err := ReadOrCreate(db, s)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	wg.Done()
}

func GetAllStats(db *sqlx.DB) error {
	return errors.New("Feature not yet supported")
}
