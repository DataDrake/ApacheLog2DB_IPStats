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

package main

import (
	"flag"
	"fmt"
	"github.com/DataDrake/ApacheLog2DB/global"
	"github.com/DataDrake/ApacheLog2DB_IPStats/stat"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

func usage() {
	fmt.Println("USAGE: ipstats [OPTION]... DB_STRING")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	update := flag.Bool("u", false, "Update existing IPStats")
	fib := flag.Bool("b", false, "Fill missing IPStats with averages")
	flag.Parse()

	args := flag.Args()

	if len(args) != 1 {
		usage()
		os.Exit(1)
	}

	if args[0] == "-" || args[0] == "--" {
		fmt.Fprintf(os.Stderr, "Input file must be a db string")
		os.Exit(1)
	}

	db, err := global.OpenDatabase(args[0])
	defer db.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err = stat.CreateMissing(db); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	if *update {
		err = stat.UpdateStats(db)
	} else if *fib {
		err = stat.FillInBlanks(db)
	} else {
		err = stat.GetAllStats(db)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
