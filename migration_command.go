package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/Trifocals3537/tessarr/internal/migration"
)

func runLegacyMigration(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("migrate-from-decypharr", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var source string
	var target string
	var dryRun bool
	flags.StringVar(&source, "source", "", "offline Decypharr state directory")
	flags.StringVar(&target, "target", "", "new Tessarr state directory")
	flags.BoolVar(&dryRun, "dry-run", false, "inspect and verify the migration plan without writing")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected migration arguments: %v", flags.Args())
	}

	result, err := migration.Migrate(migration.Options{
		Source: source,
		Target: target,
		DryRun: dryRun,
	})
	if err != nil {
		return err
	}
	status := "migration complete"
	if result.DryRun {
		status = "dry run complete; no files were written"
	} else if result.AlreadyMigrated {
		status = "migration already complete and verified"
	}
	_, err = fmt.Fprintf(
		stdout,
		"%s: %d files, %d directories, %d bytes, %d renamed state entries\n",
		status,
		result.Files,
		result.Directories,
		result.Bytes,
		result.RenamedEntries,
	)
	return err
}
