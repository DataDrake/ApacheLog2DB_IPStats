//
// Copyright 2017 Bryan T. Meyers <bmeyers@datadrake.com>
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

package cli

import (
    "fmt"
	"github.com/DataDrake/ApacheLog2DB/global"
	"github.com/DataDrake/ApacheLog2DB_IPStats/stat"
    "github.com/DataDrake/cli-ng/cmd"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

// Estimate fills in empty ipstats by usings averages for the closest subnet
var Estimate = cmd.CMD{
	Name:  "estimate",
	Alias: "E",
	Short: "Estimate empty stats for Sources",
	Args:  &EstimateArgs{},
	Run:   EstimateRun,
}

// EstimateArgs contains the arguments for the "estimate" subcommand
type EstimateArgs struct {
	DB string `desc:"Connection string for an ApacheLog2DB database"`
}

// EstimateRun fills in the blanks
func EstimateRun(r *cmd.RootCMD, c *cmd.CMD) {
    args := c.Args.(*EstimateArgs)
	db, err := global.OpenDatabase(args.DB)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer db.Close()
	if err = stat.CreateMissing(db); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	err = stat.FillInBlanks(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
