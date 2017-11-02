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
	"github.com/DataDrake/ApacheLog2DB/core"
	"github.com/DataDrake/ApacheLog2DB/global"
	"github.com/jmoiron/sqlx"
)

// SliceContains checks for a value in a slice
func SliceContains(vs []string, v string) bool {
	for _, curr := range vs {
		if curr == v {
			return true
		}
	}
	return false
}

func getTables(db *sqlx.DB) ([]string, error) {
	tables := make([]string, 0)
	found, err := db.Query(core.GetTablesQueries[global.DB_TYPE])
	if err != nil {
		return nil, err
	}
	var table string
	found.Scan(&table)
	if len(table) > 0 {
		tables = append(tables, table)
	}
	for found.Next() {
		found.Scan(&table)
		if len(table) > 0 {
			tables = append(tables, table)
		}
	}
	found.Close()
	return tables, err
}

// CreateMissing adds the ipstats table if it does not exist
func CreateMissing(db *sqlx.DB) error {
	tables, err := getTables(db)

	if !SliceContains(tables, "ipstats") {
		err = CreateTable(db)
		if err != nil {
			return err
		}
	}

	return err
}
