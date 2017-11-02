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
	"fmt"
	"github.com/DataDrake/ApacheLog2DB/source"
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
)

// FindClosest estimates the bandwidth and latency of an IP based on others in the closest matching subnet
func FindClosest(IP string, avgs, avgs2 map[string]float64) (float64, float64) {
	octets := strings.Split(IP, ".")
	bw := avgs["all"]
	lat := avgs2["all"]
	for i := range octets {
		str := strings.Join(octets[0:i], ".")
		if avgs[str] != 0.0 {
			bw = avgs[str]
			lat = avgs2[str]
		}
	}
	return bw, lat
}

// FillBlanks fills in missing link characteristics for IPs
func FillBlanks(db *sqlx.DB, avgs, avgs2 map[string]float64) error {
	srcs, err := source.ReadAll(db)
	if err != nil {
		return err
	}
	for _, src := range srcs {
		_, err := ReadSource(db, src.ID)
		if err == nil {
			continue
		}
		stat := &IPStat{}
		stat.Bandwidth, stat.Latency = FindClosest(src.IP, avgs, avgs2)
		stat.SourceID = src.ID
		err = Insert(db, stat)
		if err != nil {
			fmt.Fprint(os.Stderr, err.Error())
		}
	}
	return nil
}

// UpdateTotals adds a new IP stat into global totals
func UpdateTotals(IP string, s *IPStat, avgs, avgs2, cts map[string]float64) {
	octets := strings.Split(IP, ".")
	avgs["all"] += s.Bandwidth
	avgs2["all"] += s.Latency
	cts["all"] += 1.0
	for i := range octets {
		str := strings.Join(octets[0:i], ".")
		avgs[str] += s.Bandwidth
		avgs2[str] += s.Latency
		cts[str] += 1.0
	}
}

// GetAverages calculates the averages from the subnet totals
func GetAverages(db *sqlx.DB) (map[string]float64, map[string]float64, error) {
	stats, err := ReadAll(db)
	if err != nil {
		return nil, nil, err
	}
	avgs := make(map[string]float64)
	avgs2 := make(map[string]float64)
	counts := make(map[string]float64)
	for _, s := range stats {
		src, err := source.Read(db, s.SourceID)
		if err != nil {
			fmt.Fprint(os.Stderr, err.Error())
			continue
		}
		UpdateTotals(src.IP, s, avgs, avgs2, counts)
	}
	for s, count := range counts {
		avgs[s] /= count
		avgs2[s] /= count
	}
	avgs["all"] /= counts["all"]
	avgs2["all"] /= counts["all"]

	return avgs, avgs2, nil
}

// FillInBlanks calculates global averages and then fills in any blank IP stat entries with estimates
func FillInBlanks(db *sqlx.DB) error {
	avgs, avgs2, err := GetAverages(db)
	if err != nil {
		return err
	}
	err = FillBlanks(db, avgs, avgs2)
	return err
}
