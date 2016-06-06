package stat

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/DataDrake/ApacheLog2DB/source"
	"os"
)

func UpdateStats(db *sql.DB) error {
	ss, err := source.ReadAll(db)
	if err == nil {
		for _, s := range ss {
			_, err = ReadOrCreate(db, s)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
			}
		}
	}
	return nil
}

func GetAllStats(db *sql.DB) error {
	return errors.New("Feature not yet supported")
}
