package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/DataDrake/ApacheLog2DB_IPStats/stat"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

func usage() {
	fmt.Println("USAGE: ipstats [OPTION]... DB_FILE")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	update := flag.Bool("u", false, "Update existing IPStats")
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

	db, err := sql.Open("sqlite3", args[0])
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
	} else {
		err = stat.GetAllStats(db)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
