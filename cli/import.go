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

// Import creates the ipstats table if it is missing and then scans IPs
var Import = cmd.CMD{
	Name:  "import",
	Alias: "I",
	Short: "Import stats for Sources",
	Args:  &ImportArgs{},
	Run:   ImportRun,
}

// ImportArgs contains the arguments for the "import" subcommand
type ImportArgs struct {
	DB string `desc:"Connection string for an ApacheLog2DB database"`
}

// ImportRun carries out the Source scan
func ImportRun(r *cmd.RootCMD, c *cmd.CMD) {
    args := c.Args.(*ImportArgs)
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
	err = stat.GetAllStats(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
