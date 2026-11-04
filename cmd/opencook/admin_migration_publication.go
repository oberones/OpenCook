package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errAdminMigrationBackupOutputExists = errors.New("backup destination already exists")

// Resolve existing ancestors as well as symlink aliases of not-yet-created
// paths. Dangling symlinks and unresolvable paths fail closed.
func adminMigrationResolvedPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var suffix []string
	for {
		_, err = os.Lstat(absolute)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(absolute)
			if err != nil {
				return "", err
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(absolute)
		if parent == absolute {
			return "", err
		}
		suffix = append(suffix, filepath.Base(absolute))
		absolute = parent
	}
}

func adminMigrationProtectPaths(output string, sources ...string) error {
	if strings.TrimSpace(output) == "" || filepath.Clean(output) == "." || filepath.Clean(output) == string(os.PathSeparator) {
		return errAdminMigrationUnsafeSourcePath
	}
	destination, err := adminMigrationResolvedPath(output)
	if err != nil {
		return err
	}
	contains := func(a, b string) bool {
		rel, err := filepath.Rel(a, b)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
	}
	for _, source := range sources {
		if strings.TrimSpace(source) == "" {
			continue
		}
		protected, err := adminMigrationResolvedPath(source)
		if err != nil {
			return err
		}
		if contains(destination, protected) || contains(protected, destination) {
			return errAdminMigrationNormalizeOutputOverlaps
		}
	}
	return nil
}

type adminMigrationPublicationError struct {
	Previous string
	Code     string
	Stage    string
	Lock     string
	Err      error
}

func (e *adminMigrationPublicationError) Error() string { return e.Code + ": " + e.Err.Error() }
func (e *adminMigrationPublicationError) Unwrap() error { return e.Err }

func adminMigrationPublicationFinding(err error, fallback string) adminMigrationFinding {
	finding := adminMigrationSourceErrorFinding(fallback, "bundle could not be written")
	var publication *adminMigrationPublicationError
	if errors.As(err, &publication) {
		finding.Code = publication.Code
		finding.Message = "bundle publication failed; inspect recoverable staging and destination lock before retry"
		finding.Details = map[string]string{"staging_path": adminMigrationRedactMigrationPath(publication.Stage), "lock_path": adminMigrationRedactMigrationPath(publication.Lock)}
		if publication.Previous != "" {
			finding.Details["previous_output_path"] = adminMigrationRedactMigrationPath(publication.Previous)
		}
	} else if errors.Is(err, errAdminMigrationBackupOutputExists) {
		finding.Code = "backup_output_exists"
		finding.Message = "backup destination already exists; choose a fresh destination even with --yes"
	} else if errors.Is(err, errAdminMigrationNormalizeOutputOverlaps) {
		return adminMigrationNormalizeOutputFinding(err)
	}
	return finding
}

func adminMigrationRequireFreshBackup(output string) error {
	if strings.TrimSpace(output) == "" {
		return errAdminMigrationUnsafeSourcePath
	}
	if _, err := os.Lstat(output); err == nil {
		return errAdminMigrationBackupOutputExists
	} else if !os.IsNotExist(err) {
		return err
	}
	return adminMigrationProtectPaths(output)
}

// publishBundle owns only its unique staging directory and exclusive lock. A
// lock left after interruption is never automatically reclaimed. The lock file
// records the staging path for operator recovery. Kernel no-replace rename also
// protects against writers that do not participate in the lock protocol.
func adminMigrationPublishBundle(output, kind string, overwrite bool, protected []string, write func(string) error) (resultErr error) {
	if kind == "backup" {
		if err := adminMigrationRequireFreshBackup(output); err != nil {
			return err
		}
	}
	if err := adminMigrationProtectPaths(output, protected...); err != nil {
		return err
	}
	// Resolve the parent once so changing a symlink ancestor cannot redirect writes.
	parent, err := adminMigrationResolvedPath(filepath.Dir(output))
	if err != nil {
		return err
	}
	output = filepath.Join(parent, filepath.Base(output))
	if err := adminMigrationMakeOutputParent(parent); err != nil {
		return err
	}
	lockPath := filepath.Join(parent, "."+filepath.Base(output)+".publish.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return &adminMigrationPublicationError{Code: "bundle_destination_locked", Lock: lockPath, Err: err}
	}
	stage := ""
	preserve := false
	defer func() {
		_ = lock.Close()
		if !preserve && stage != "" {
			if err := os.RemoveAll(stage); err != nil {
				preserve = true
				resultErr = &adminMigrationPublicationError{Code: "bundle_cleanup_failed", Stage: stage, Lock: lockPath, Err: err}
			}
		}
		if !preserve {
			if err := os.Remove(lockPath); err != nil {
				resultErr = &adminMigrationPublicationError{Code: "bundle_cleanup_failed", Stage: stage, Lock: lockPath, Err: err}
			}
		}
	}()
	if kind == "backup" {
		if err := adminMigrationRequireFreshBackup(output); err != nil {
			return err
		}
	}
	stale, err := adminMigrationInterruptedNormalizedSourceOutputDirs(output)
	if err != nil {
		return err
	}
	if len(stale) > 0 {
		return &adminMigrationPublicationError{Code: "bundle_staging_interrupted", Stage: stale[0], Err: errAdminMigrationNormalizeOutputStaleTmp}
	}
	stage, err = os.MkdirTemp(parent, "."+filepath.Base(output)+"-"+kind+"-*")
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintf(lock, "pid=%d\nstaging=%s\n", os.Getpid(), stage); err != nil {
		return err
	}
	if err = lock.Sync(); err != nil {
		return err
	}
	if err = adminMigrationSyncDirectory(parent); err != nil {
		return err
	}
	if err = write(stage); err != nil {
		return err
	}
	if err = adminMigrationSyncTree(stage); err != nil {
		return err
	}
	if err = adminMigrationProtectPaths(output, protected...); err != nil {
		return err
	}
	// Normalization retains its explicit --yes replacement contract, saving the
	// old output until the new bundle is published. Backups never take this path.
	previous := ""
	if overwrite {
		if _, err = os.Lstat(output); err == nil {
			previous = stage + "-previous"
			if err = adminMigrationRenameNoReplace(output, previous); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if err = adminMigrationRenameNoReplace(stage, output); err != nil {
		preserve = true
		if previous != "" {
			if restoreErr := adminMigrationRenameNoReplace(previous, output); restoreErr != nil {
				err = errors.Join(err, restoreErr)
			} else {
				previous = ""
			}
		}
		return &adminMigrationPublicationError{Code: "bundle_publication_failed", Stage: stage, Previous: previous, Lock: lockPath, Err: err}
	}
	stage = ""
	if err = adminMigrationSyncDirectory(parent); err != nil {
		preserve = true
		return &adminMigrationPublicationError{Code: "bundle_publication_failed", Stage: output, Previous: previous, Lock: lockPath, Err: err}
	}
	if previous != "" {
		if err = os.RemoveAll(previous); err != nil {
			preserve = true
			return &adminMigrationPublicationError{Code: "bundle_cleanup_failed", Stage: previous, Lock: lockPath, Err: err}
		}
	}
	return nil
}

func adminMigrationSyncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func adminMigrationSyncTree(root string) error {
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		return errors.Join(syncErr, closeErr)
	})
	if err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := adminMigrationSyncDirectory(directories[i]); err != nil {
			return err
		}
	}
	return nil
}

// Newly created ancestor directories also need synchronization for publication
// durability when the requested destination's parent did not yet exist.
func adminMigrationMakeOutputParent(parent string) error {
	var directories []string
	existing := parent
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return err
		}
		directories = append(directories, existing)
		next := filepath.Dir(existing)
		if next == existing {
			return fmt.Errorf("output ancestor unavailable")
		}
		existing = next
	}
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	directories = append(directories, existing)
	for _, directory := range directories {
		if err := adminMigrationSyncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}
