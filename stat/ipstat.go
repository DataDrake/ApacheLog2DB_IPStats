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
	"github.com/DataDrake/ApacheLog2DB/global"
	"github.com/DataDrake/ApacheLog2DB/source"
	"github.com/DataDrake/ipstat/data"
	"github.com/DataDrake/ipstat/lms"
	"github.com/jmoiron/sqlx"
	"os"
)

// MaxRetries in the number of times to try get the stats, given failure on an attempt
const MaxRetries = 3

// IPStat is a SQL type for the link characteristics of a given IP
type IPStat struct {
	ID        int
	Bandwidth float64
	Latency   float64
	SourceID  int
}

// NewIPStat returns an initialized IPStat
func NewIPStat(bw float64, lat float64, sourceid int) *IPStat {
	return &IPStat{-1, bw, lat, sourceid}
}

// GetStats returns the IPStat record for a Source, if one exists
func GetStats(s *source.Source) (*IPStat, error) {
	stat := &IPStat{}
	for i := 0; i < MaxRetries; i++ {
		samples, err := data.CollectDataPoints(s.IP, 100, 1500, 100)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		} else {

			slope, intercept := lms.LMS_Perf(samples)
			if slope > 0 {
				stat.Bandwidth = float64(1.0) / slope
				stat.Latency = intercept
				stat.SourceID = s.ID
				return stat, nil
			}
		}
	}
	return stat, errors.New("Failed to get stats for: " + s.IP)
}

// ReadOrCreate gets an existing IPStat if it exists, or creates a new one if it doesn't
func ReadOrCreate(db *sqlx.DB, s *source.Source) (*IPStat, error) {
	stat, err := ReadSource(db, s.ID)
	if err != nil {
		stat, err = GetStats(s)
		if err != nil {
			return nil, err
		}
		err = Insert(db, stat)
		if err == nil {
			stat, err = ReadSource(db, s.ID)
		}
	}
	return stat, err
}

// CreateTableQueries is a mapping of SQL dialect to Table CREATE query for IPStats
var CreateTableQueries = map[string]string{
	"mysql": `CREATE TABLE ipstats ( id INTEGER PRIMARY KEY AUTO_INCREMENT,
	bandwidth DOUBLE, latency DOUBLE, sourceid INTEGER,
	FOREIGN KEY(sourceid) REFERENCES sources(id) )`,
	"sqlite": `CREATE TABLE ipstats ( id INTEGER PRIMARY KEY AUTOINCREMENT,
	bandwidth DOUBLE, latency DOUBLE, sourceid INTEGER,
	FOREIGN KEY(sourceid) REFERENCES sources(id) )`,
}

// CreateTable creates the IPStats table in a given DB
func CreateTable(d *sqlx.DB) error {
	_, err := d.Exec(CreateTableQueries[global.DB_TYPE])
	return err
}

// Insert creates a new entry for an IPStat
func Insert(d *sqlx.DB, s *IPStat) error {
	_, err := d.Exec("INSERT INTO ipstats VALUES( NULL , ? , ? , ? )", s.Bandwidth, s.Latency, s.SourceID)
	return err
}

// ReadSource attempts to get an IPStat by sourceid if one exists
func ReadSource(d *sqlx.DB, sourceid int) (*IPStat, error) {
	s := &IPStat{}
	var err error
	row := d.QueryRow("SELECT * FROM ipstats WHERE sourceid=?", sourceid)
	if row == nil {
		err = errors.New("Stats not found")
	} else {
		err = row.Scan(&s.ID, &s.Bandwidth, &s.Latency, &s.SourceID)
	}
	return s, err
}

// Read gets an IPStat by its own ID
func Read(d *sqlx.DB, id int) (*IPStat, error) {
	s := &IPStat{}
	var err error
	row := d.QueryRow("SELECT * FROM ipstats WHERE id=?", id)
	if row == nil {
		err = errors.New("Agent not found")
	} else {
		err = row.Scan(&s.ID, &s.Bandwidth, &s.Latency, &s.SourceID)
	}
	return s, err
}

// ReadAll gets an array of all IPStats in the DB
func ReadAll(d *sqlx.DB) ([]*IPStat, error) {
	ss := make([]*IPStat, 0)
	rows, err := d.Query("SELECT * FROM ipstats")
	if err == nil {
		for rows.Next() {
			s := &IPStat{}
			err = rows.Scan(&s.ID, &s.Bandwidth, &s.Latency, &s.SourceID)
			if err == nil && s.ID >= 0 && s.SourceID > 0 {
				ss = append(ss, s)
			}
		}
		rows.Close()
	}
	return ss, err
}

// Update modifies an existing IPStat
func Update(d *sqlx.DB, s *IPStat) error {
	_, err := d.Exec("UPDATE ipstats SET bandwidth=? latency=? sourceid=? WHERE id=?", s.Bandwidth, s.Latency, s.SourceID, s.ID)
	return err
}
