//go:build !linux && !darwin

package main

import "fmt"

func adminMigrationRenameNoReplace(source, destination string) error {
	return fmt.Errorf("atomic no-replace bundle publication is unsupported on this platform")
}
