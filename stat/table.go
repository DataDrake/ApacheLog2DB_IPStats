package stat

import (
	"database/sql"
)

func SliceContains(vs []string, v string) bool {
	for _, curr := range vs {
		if curr == v {
			return true
		}
	}
	return false
}

func get_tables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)
	found, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
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

func CreateMissing(db *sql.DB) error {
	tables, err := get_tables(db)

	if !SliceContains(tables, "ipstats") {
		err = CreateTable(db)
		if err != nil {
			return err
		}
	}

	return err
}
