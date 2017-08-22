package stat

import (
	"github.com/DataDrake/ApacheLog2DB/core"
	"github.com/DataDrake/ApacheLog2DB/global"
    "github.com/jmoiron/sqlx"
)

func SliceContains(vs []string, v string) bool {
	for _, curr := range vs {
		if curr == v {
			return true
		}
	}
	return false
}

func get_tables(db *sqlx.DB) ([]string, error) {
	tables := make([]string, 0)
	found, err := db.Query(core.GET_TABLES[global.DB_TYPE])
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

func CreateMissing(db *sqlx.DB) error {
	tables, err := get_tables(db)

	if !SliceContains(tables, "ipstats") {
		err = CreateTable(db)
		if err != nil {
			return err
		}
	}

	return err
}
