package main

import "golang.org/x/sys/unix"

func adminMigrationRenameNoReplace(source, destination string) error {
	return unix.RenamexNp(source, destination, unix.RENAME_EXCL)
}
