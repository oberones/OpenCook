package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type adminJSONClient interface {
	DoJSON(context.Context, string, string, any, any) error
}

type adminOutputOptions struct {
	privateKeyOut       string
	overwritePrivateKey bool
}

func (c *command) runAdminCommand(ctx context.Context, args []string) int {
	if len(args) == 0 {
		c.printAdminUsage(c.stderr)
		return exitUsage
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminUsage(c.stdout)
		return exitOK
	}
	if len(args) > 0 && args[0] == "reindex" {
		return c.runAdminReindex(ctx, args[1:], false)
	}
	if len(args) > 0 && args[0] == "search" {
		return c.runAdminSearch(ctx, args[1:], false)
	}
	if adminCommandIsOffline(args) {
		return c.runAdminOfflineCommand(ctx, args)
	}

	cfg := c.loadAdminConfig()
	fs := flag.NewFlagSet("opencook admin", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg.BindFlags(fs)
	jsonOutput := fs.Bool("json", false, "print JSON output")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(c.stderr, "admin: %v\n\n", err)
		c.printAdminUsage(c.stderr)
		return exitUsage
	}

	rest := fs.Args()
	if len(rest) == 0 {
		c.printAdminUsage(c.stderr)
		return exitUsage
	}
	if rest[0] == "reindex" {
		return c.runAdminReindex(ctx, rest[1:], *jsonOutput)
	}
	if rest[0] == "search" {
		return c.runAdminSearch(ctx, rest[1:], *jsonOutput)
	}
	if adminCommandIsOffline(rest) {
		return c.runAdminOfflineCommand(ctx, rest)
	}

	if (rest[0] == "orgs" || rest[0] == "organizations") && len(rest) > 1 && rest[1] == "create" {
		payload, output, code, ok := c.prepareAdminOrganizationCreate(rest[2:])
		if !ok {
			return code
		}
		client, err := c.newAdmin(cfg)
		if err != nil {
			fmt.Fprintf(c.stderr, "admin client: %v\n", err)
			return exitDependencyUnavailable
		}
		return c.adminDoWithOutputOptions(ctx, client, http.MethodPost, "/organizations", payload, output)
	}

	client, err := c.newAdmin(cfg)
	if err != nil {
		fmt.Fprintf(c.stderr, "admin client: %v\n", err)
		return exitDependencyUnavailable
	}

	switch rest[0] {
	case "status":
		return c.runAdminStatus(ctx, client, rest[1:])
	case "users":
		return c.runAdminUsers(ctx, client, rest[1:])
	case "orgs", "organizations":
		return c.runAdminOrganizations(ctx, client, rest[1:])
	case "clients":
		return c.runAdminClients(ctx, client, rest[1:])
	case "groups":
		return c.runAdminGroups(ctx, client, rest[1:])
	case "containers":
		return c.runAdminContainers(ctx, client, rest[1:])
	case "acls":
		return c.runAdminACLs(ctx, client, rest[1:])
	case "help", "-h", "--help":
		if len(rest) > 1 {
			return c.adminUsageError("%s does not accept arguments: %v\n\n", rest[0], rest[1:])
		}
		c.printAdminUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin command %q\n\n", rest[0])
	}
}

func (c *command) runAdminStatus(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) != 0 {
		return c.adminUsageError("admin status does not accept arguments: %v\n\n", args)
	}
	return c.adminDo(ctx, client, http.MethodGet, "/_status", nil, "")
}

func (c *command) runAdminUsers(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin users requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 1 {
			return c.adminUsageError("admin users list does not accept arguments: %v\n\n", args[1:])
		}
		return c.adminDo(ctx, client, http.MethodGet, "/users", nil, "")
	case "show":
		if len(args) != 2 {
			return c.adminUsageError("usage: opencook admin users show USER\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("users", args[1]), nil, "")
	case "create":
		return c.runAdminUserCreate(ctx, client, args[1:])
	case "keys":
		return c.runAdminUserKeys(ctx, client, args[1:])
	case "help", "-h", "--help":
		c.printAdminUsersUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin users command %q\n\n", args[0])
	}
}

func (c *command) runAdminUserCreate(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("usage: opencook admin users create USER [flags]\n\n")
	}

	username := args[0]
	fs := flag.NewFlagSet("opencook admin users create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	displayName := fs.String("display-name", "", "display name")
	firstName := fs.String("first-name", "", "first name")
	lastName := fs.String("last-name", "", "last name")
	email := fs.String("email", "", "email address")
	keyName := fs.String("key-name", "", "initial key name; only default is supported by the current live route")
	publicKeyPath := fs.String("public-key", "", "path to public key PEM")
	privateKeyOut := fs.String("private-key-out", "", "write generated private key to PATH, or - for stdout")
	if err := fs.Parse(args[1:]); err != nil {
		return c.adminFlagError("admin users create", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin users create received unexpected arguments: %v\n\n", fs.Args())
	}
	if *keyName != "" && *keyName != "default" {
		return c.adminUsageError("admin users create currently supports only the default initial key; use users keys add for named keys\n\n")
	}

	publicKey, err := readOptionalPublicKey(*publicKeyPath)
	if err != nil {
		fmt.Fprintf(c.stderr, "read public key: %v\n", err)
		return exitDependencyUnavailable
	}

	payload := map[string]any{
		"username":     username,
		"display_name": *displayName,
		"first_name":   *firstName,
		"last_name":    *lastName,
		"email":        *email,
		"public_key":   publicKey,
	}
	if publicKey == "" && (*privateKeyOut != "" || *keyName == "default") {
		payload["create_key"] = true
	}
	return c.adminDo(ctx, client, http.MethodPost, "/users", payload, *privateKeyOut)
}

func (c *command) runAdminUserKeys(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin users keys requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 2 {
			return c.adminUsageError("usage: opencook admin users keys list USER\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("users", args[1], "keys"), nil, "")
	case "show":
		if len(args) != 3 {
			return c.adminUsageError("usage: opencook admin users keys show USER KEY\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("users", args[1], "keys", args[2]), nil, "")
	case "add", "create":
		if len(args) < 2 {
			return c.adminUsageError("usage: opencook admin users keys add USER [flags]\n\n")
		}
		return c.runAdminKeyAdd(ctx, client, adminPath("users", args[1], "keys"), args[2:])
	case "update":
		if len(args) < 3 {
			return c.adminUsageError("usage: opencook admin users keys update USER KEY [flags]\n\n")
		}
		return c.runAdminKeyUpdate(ctx, client, adminPath("users", args[1], "keys", args[2]), args[3:])
	case "delete", "remove":
		if len(args) < 3 {
			return c.adminUsageError("usage: opencook admin users keys delete USER KEY --yes\n\n")
		}
		return c.runAdminKeyDelete(ctx, client, adminPath("users", args[1], "keys", args[2]), args[3:])
	case "help", "-h", "--help":
		c.printAdminUserKeysUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin users keys command %q\n\n", args[0])
	}
}

func (c *command) runAdminOrganizations(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin orgs requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 1 {
			return c.adminUsageError("admin orgs list does not accept arguments: %v\n\n", args[1:])
		}
		return c.adminDo(ctx, client, http.MethodGet, "/organizations", nil, "")
	case "show":
		if len(args) != 2 {
			return c.adminUsageError("usage: opencook admin orgs show ORG\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1]), nil, "")
	case "create":
		return c.runAdminOrganizationCreate(ctx, client, args[1:])
	case "add-user", "remove-user":
		return c.adminUsageError("admin orgs %s is offline-only; rerun with --offline --yes\n\n", args[0])
	case "help", "-h", "--help":
		c.printAdminOrganizationsUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin orgs command %q\n\n", args[0])
	}
}

func (c *command) runAdminGroups(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin groups requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 2 {
			return c.adminUsageError("usage: opencook admin groups list ORG\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "groups"), nil, "")
	case "show":
		if len(args) != 3 {
			return c.adminUsageError("usage: opencook admin groups show ORG GROUP\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "groups", args[2]), nil, "")
	case "add-actor", "remove-actor":
		return c.adminUsageError("admin groups %s is offline-only; rerun with --offline --yes\n\n", args[0])
	case "help", "-h", "--help":
		c.printAdminGroupsUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin groups command %q\n\n", args[0])
	}
}

func (c *command) runAdminContainers(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin containers requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 2 {
			return c.adminUsageError("usage: opencook admin containers list ORG\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "containers"), nil, "")
	case "show":
		if len(args) != 3 {
			return c.adminUsageError("usage: opencook admin containers show ORG CONTAINER\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "containers", args[2]), nil, "")
	case "help", "-h", "--help":
		c.printAdminContainersUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin containers command %q\n\n", args[0])
	}
}

func (c *command) runAdminACLs(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin acls requires a subcommand\n\n")
	}

	switch args[0] {
	case "get":
		if len(args) < 2 {
			return c.adminUsageError("usage: opencook admin acls get TYPE ARGS...\n\n")
		}
		path, ok := adminACLPath(args[1:])
		if !ok {
			return c.adminUsageError("usage: opencook admin acls get user USER | org ORG | group ORG GROUP | container ORG CONTAINER | client ORG CLIENT\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, path, nil, "")
	case "repair-defaults":
		return c.adminUsageError("admin acls repair-defaults is offline-only; rerun with --offline and --dry-run or --yes\n\n")
	case "help", "-h", "--help":
		c.printAdminACLsUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin acls command %q\n\n", args[0])
	}
}

func (c *command) runAdminOrganizationCreate(ctx context.Context, client adminJSONClient, args []string) int {
	payload, output, code, ok := c.prepareAdminOrganizationCreate(args)
	if !ok {
		return code
	}
	return c.adminDoWithOutputOptions(ctx, client, http.MethodPost, "/organizations", payload, output)
}

func (c *command) prepareAdminOrganizationCreate(args []string) (map[string]any, adminOutputOptions, int, bool) {
	if len(args) == 0 {
		return nil, adminOutputOptions{}, c.adminUsageError("usage: opencook admin orgs create ORG --full-name NAME [flags]\n\n"), false
	}

	name := args[0]
	fs := flag.NewFlagSet("opencook admin orgs create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fullName := fs.String("full-name", "", "organization full name")
	orgType := fs.String("org-type", "", "organization type")
	associationUser := fs.String("association-user", "", "reserved for a later membership slice")
	validatorKeyOut := fs.String("validator-key-out", "", "write generated validator private key to PATH, or - for stdout")
	if err := fs.Parse(args[1:]); err != nil {
		return nil, adminOutputOptions{}, c.adminFlagError("admin orgs create", err), false
	}
	if fs.NArg() != 0 {
		return nil, adminOutputOptions{}, c.adminUsageError("admin orgs create received unexpected arguments: %v\n\n", fs.Args()), false
	}
	if *associationUser != "" {
		return nil, adminOutputOptions{}, c.adminUsageError("admin orgs create --association-user is not exposed by the live HTTP route yet; sign as the desired owning requestor\n\n"), false
	}
	output := adminOutputOptions{privateKeyOut: *validatorKeyOut}
	overwriteValidatorKey, code := c.confirmAdminFileOverwrite(*validatorKeyOut, "validator key file", "organization creation canceled")
	if code != exitOK {
		return nil, adminOutputOptions{}, code, false
	}
	output.overwritePrivateKey = overwriteValidatorKey

	payload := map[string]any{
		"name":      name,
		"full_name": *fullName,
		"org_type":  *orgType,
	}
	return payload, output, exitOK, true
}

func (c *command) runAdminClients(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin clients requires a subcommand\n\n")
	}
	if args[0] != "keys" {
		return c.adminUsageError("unknown admin clients command %q\n\n", args[0])
	}
	return c.runAdminClientKeys(ctx, client, args[1:])
}

func (c *command) runAdminClientKeys(ctx context.Context, client adminJSONClient, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin clients keys requires a subcommand\n\n")
	}

	switch args[0] {
	case "list":
		if len(args) != 3 {
			return c.adminUsageError("usage: opencook admin clients keys list ORG CLIENT\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "clients", args[2], "keys"), nil, "")
	case "show":
		if len(args) != 4 {
			return c.adminUsageError("usage: opencook admin clients keys show ORG CLIENT KEY\n\n")
		}
		return c.adminDo(ctx, client, http.MethodGet, adminPath("organizations", args[1], "clients", args[2], "keys", args[3]), nil, "")
	case "add", "create":
		if len(args) < 3 {
			return c.adminUsageError("usage: opencook admin clients keys add ORG CLIENT [flags]\n\n")
		}
		return c.runAdminKeyAdd(ctx, client, adminPath("organizations", args[1], "clients", args[2], "keys"), args[3:])
	case "update":
		if len(args) < 4 {
			return c.adminUsageError("usage: opencook admin clients keys update ORG CLIENT KEY [flags]\n\n")
		}
		return c.runAdminKeyUpdate(ctx, client, adminPath("organizations", args[1], "clients", args[2], "keys", args[3]), args[4:])
	case "delete", "remove":
		if len(args) < 4 {
			return c.adminUsageError("usage: opencook admin clients keys delete ORG CLIENT KEY --yes\n\n")
		}
		return c.runAdminKeyDelete(ctx, client, adminPath("organizations", args[1], "clients", args[2], "keys", args[3]), args[4:])
	case "help", "-h", "--help":
		c.printAdminClientKeysUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin clients keys command %q\n\n", args[0])
	}
}

func (c *command) runAdminKeyAdd(ctx context.Context, client adminJSONClient, path string, args []string) int {
	fs := flag.NewFlagSet("opencook admin keys add", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	keyName := fs.String("key-name", "", "key name")
	publicKeyPath := fs.String("public-key", "", "path to public key PEM")
	expirationDate := fs.String("expiration-date", "infinity", "Chef expiration date, or infinity")
	privateKeyOut := fs.String("private-key-out", "", "write generated private key to PATH, or - for stdout")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin keys add", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin keys add received unexpected arguments: %v\n\n", fs.Args())
	}

	publicKey, err := readOptionalPublicKey(*publicKeyPath)
	if err != nil {
		fmt.Fprintf(c.stderr, "read public key: %v\n", err)
		return exitDependencyUnavailable
	}
	payload := map[string]any{
		"name":            *keyName,
		"public_key":      publicKey,
		"create_key":      publicKey == "",
		"expiration_date": *expirationDate,
	}
	return c.adminDo(ctx, client, http.MethodPost, path, payload, *privateKeyOut)
}

func (c *command) runAdminKeyUpdate(ctx context.Context, client adminJSONClient, path string, args []string) int {
	fs := flag.NewFlagSet("opencook admin keys update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	newName := fs.String("new-name", "", "new key name")
	publicKeyPath := fs.String("public-key", "", "path to replacement public key PEM")
	expirationDate := fs.String("expiration-date", "", "Chef expiration date, or infinity")
	createKey := fs.Bool("create-key", false, "generate a replacement key pair")
	privateKeyOut := fs.String("private-key-out", "", "write generated private key to PATH, or - for stdout")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin keys update", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin keys update received unexpected arguments: %v\n\n", fs.Args())
	}

	payload := map[string]any{}
	if *newName != "" {
		payload["name"] = *newName
	}
	if *expirationDate != "" {
		payload["expiration_date"] = *expirationDate
	}
	if *createKey {
		payload["create_key"] = true
	}
	publicKey, err := readOptionalPublicKey(*publicKeyPath)
	if err != nil {
		fmt.Fprintf(c.stderr, "read public key: %v\n", err)
		return exitDependencyUnavailable
	}
	if publicKey != "" {
		payload["public_key"] = publicKey
	}

	return c.adminDo(ctx, client, http.MethodPut, path, payload, *privateKeyOut)
}

func (c *command) runAdminKeyDelete(ctx context.Context, client adminJSONClient, path string, args []string) int {
	fs := flag.NewFlagSet("opencook admin keys delete", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	yes := fs.Bool("yes", false, "confirm deletion")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin keys delete", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin keys delete received unexpected arguments: %v\n\n", fs.Args())
	}
	if !*yes {
		return c.adminUsageError("refusing to delete key without --yes\n\n")
	}
	return c.adminDo(ctx, client, http.MethodDelete, path, nil, "")
}

func (c *command) adminDo(ctx context.Context, client adminJSONClient, method, path string, payload any, privateKeyOut string) int {
	return c.adminDoWithOutputOptions(ctx, client, method, path, payload, adminOutputOptions{privateKeyOut: privateKeyOut})
}

func (c *command) adminDoWithOutputOptions(ctx context.Context, client adminJSONClient, method, path string, payload any, output adminOutputOptions) int {
	var out any
	if err := client.DoJSON(ctx, method, path, payload, &out); err != nil {
		fmt.Fprintf(c.stderr, "admin request: %v\n", err)
		return exitDependencyUnavailable
	}
	if err := c.writeAdminOutput(out, output); err != nil {
		fmt.Fprintf(c.stderr, "write admin output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitOK
}

func (c *command) writeAdminOutput(out any, output adminOutputOptions) error {
	privateKey, hasPrivateKey := findPrivateKey(out)
	if !hasPrivateKey {
		return writePrettyJSON(c.stdout, out)
	}

	switch output.privateKeyOut {
	case "":
		fmt.Fprintln(c.stderr, "private key omitted; use --private-key-out PATH or --private-key-out - to print it")
		return writePrettyJSON(c.stdout, redactPrivateKeys(out))
	case "-":
		if _, err := fmt.Fprint(c.stdout, privateKey); err != nil {
			return err
		}
		if !strings.HasSuffix(privateKey, "\n") {
			_, err := fmt.Fprintln(c.stdout)
			return err
		}
		return nil
	default:
		if err := writePrivateKeyFile(output.privateKeyOut, privateKey, output.overwritePrivateKey); err != nil {
			return err
		}
		fmt.Fprintf(c.stderr, "private key written to %s\n", output.privateKeyOut)
		return writePrettyJSON(c.stdout, redactPrivateKeys(out))
	}
}

func (c *command) confirmAdminFileOverwrite(path, label, cancelMessage string) (bool, int) {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return false, exitOK
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, exitOK
		}
		fmt.Fprintf(c.stderr, "check %s: %v\n", label, err)
		return false, exitDependencyUnavailable
	}

	fmt.Fprintf(c.stderr, "warning: %s %s already exists and will be overwritten\n", label, path)
	fmt.Fprintf(c.stderr, "Proceed? [y/N]: ")
	confirmed, err := c.readYesNo()
	fmt.Fprintln(c.stderr)
	if err != nil {
		fmt.Fprintf(c.stderr, "read confirmation: %v\n", err)
		return false, exitDependencyUnavailable
	}
	if !confirmed {
		fmt.Fprintln(c.stderr, cancelMessage)
		return false, exitUsage
	}
	return true, exitOK
}

func (c *command) readYesNo() (bool, error) {
	if c.stdin == nil {
		return false, nil
	}
	answer, err := bufio.NewReader(c.stdin).ReadString('\n')
	if err != nil && err != io.EOF {
		return false, err
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "y" || answer == "yes", nil
}

func readOptionalPublicKey(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func writePrivateKeyFile(path, privateKey string, overwrite bool) error {
	flags := os.O_WRONLY | os.O_CREATE
	if overwrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(privateKey); err != nil {
		return err
	}
	if !strings.HasSuffix(privateKey, "\n") {
		if _, err := file.WriteString("\n"); err != nil {
			return err
		}
	}
	return file.Chmod(0o600)
}

func writePrettyJSON(w io.Writer, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err = fmt.Fprintln(w)
	return err
}

func findPrivateKey(value any) (string, bool) {
	switch v := value.(type) {
	case map[string]any:
		if privateKey, ok := v["private_key"].(string); ok && privateKey != "" {
			return privateKey, true
		}
		for _, nested := range v {
			if privateKey, ok := findPrivateKey(nested); ok {
				return privateKey, true
			}
		}
	case []any:
		for _, nested := range v {
			if privateKey, ok := findPrivateKey(nested); ok {
				return privateKey, true
			}
		}
	}
	return "", false
}

func redactPrivateKeys(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, nested := range v {
			if key == "private_key" {
				continue
			}
			out[key] = redactPrivateKeys(nested)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, nested := range v {
			out[i] = redactPrivateKeys(nested)
		}
		return out
	default:
		return value
	}
}

func adminPath(parts ...string) string {
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		escaped = append(escaped, url.PathEscape(part))
	}
	return "/" + strings.Join(escaped, "/")
}

func (c *command) adminFlagError(name string, err error) int {
	return c.adminUsageError("%s: %v\n\n", name, err)
}

func (c *command) adminUsageError(format string, args ...any) int {
	fmt.Fprintf(c.stderr, format, args...)
	c.printAdminUsage(c.stderr)
	return exitUsage
}

func (c *command) printAdminUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin [flags] status
  opencook admin [flags] users list
  opencook admin [flags] users show USER
  opencook admin [flags] users create USER [--first-name FIRST] [--last-name LAST] [--email EMAIL] [--public-key PATH] [--private-key-out PATH|-]
  opencook admin [flags] users keys list USER
  opencook admin [flags] users keys show USER KEY
  opencook admin [flags] users keys add USER --key-name NAME [--public-key PATH] [--expiration-date DATE] [--private-key-out PATH|-]
  opencook admin [flags] users keys update USER KEY [--new-name NAME] [--public-key PATH] [--expiration-date DATE] [--create-key] [--private-key-out PATH|-]
  opencook admin [flags] users keys delete USER KEY --yes
  opencook admin [flags] orgs list
  opencook admin [flags] orgs show ORG
  opencook admin [flags] orgs create ORG --full-name NAME [--org-type TYPE] [--validator-key-out PATH|-]
  opencook admin orgs add-user ORG USER --offline --yes [--admin]
  opencook admin orgs remove-user ORG USER --offline --yes [--force]
  opencook admin [flags] groups list ORG
  opencook admin [flags] groups show ORG GROUP
  opencook admin groups add-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]
  opencook admin groups remove-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]
  opencook admin [flags] containers list ORG
  opencook admin [flags] containers show ORG CONTAINER
  opencook admin [flags] acls get user USER
  opencook admin [flags] acls get org ORG
  opencook admin [flags] acls get group ORG GROUP
  opencook admin [flags] acls get container ORG CONTAINER
  opencook admin [flags] acls get client ORG CLIENT
  opencook admin acls repair-defaults --offline [--org ORG] [--dry-run|--yes]
  opencook admin server-admins list --offline
  opencook admin server-admins grant USER --offline --yes
  opencook admin server-admins revoke USER --offline --yes
  opencook admin [flags] clients keys list ORG CLIENT
  opencook admin [flags] clients keys show ORG CLIENT KEY
  opencook admin [flags] clients keys add ORG CLIENT --key-name NAME [--public-key PATH] [--expiration-date DATE] [--private-key-out PATH|-]
  opencook admin [flags] clients keys update ORG CLIENT KEY [--new-name NAME] [--public-key PATH] [--expiration-date DATE] [--create-key] [--private-key-out PATH|-]
  opencook admin [flags] clients keys delete ORG CLIENT KEY --yes
  opencook admin reindex --org ORG [--complete|--drop|--no-drop] [--index INDEX] [--name NAME ...] [--dry-run] [--with-timing] [--json]
  opencook admin reindex --all-orgs [--complete|--drop|--no-drop] [--dry-run] [--with-timing] [--json]
  opencook admin search check [--org ORG|--all-orgs] [--index INDEX] [--with-timing] [--json]
  opencook admin search repair [--org ORG|--all-orgs] [--index INDEX] [--dry-run|--yes] [--with-timing] [--json]

Admin flags:
  --server-url URL
  --requestor-name NAME
  --requestor-type user|client
  --private-key PATH
  --default-org ORG
  --server-api-version VERSION
  --json

Offline flags:
  --offline
  --yes
  --postgres-dsn DSN
`)
}

func (c *command) printAdminUsersUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin users list
  opencook admin users show USER
  opencook admin users create USER [flags]
  opencook admin users keys COMMAND
`)
}

func (c *command) printAdminUserKeysUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin users keys list USER
  opencook admin users keys show USER KEY
  opencook admin users keys add USER --key-name NAME [flags]
  opencook admin users keys update USER KEY [flags]
  opencook admin users keys delete USER KEY --yes
`)
}

func (c *command) printAdminOrganizationsUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin orgs list
  opencook admin orgs show ORG
  opencook admin orgs create ORG --full-name NAME [flags]
  opencook admin orgs add-user ORG USER --offline --yes [--admin]
  opencook admin orgs remove-user ORG USER --offline --yes [--force]
`)
}

func (c *command) printAdminGroupsUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin groups list ORG
  opencook admin groups show ORG GROUP
  opencook admin groups add-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]
  opencook admin groups remove-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]
`)
}

func (c *command) printAdminContainersUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin containers list ORG
  opencook admin containers show ORG CONTAINER
`)
}

func (c *command) printAdminACLsUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin acls get user USER
  opencook admin acls get org ORG
  opencook admin acls get group ORG GROUP
  opencook admin acls get container ORG CONTAINER
  opencook admin acls get client ORG CLIENT
  opencook admin acls repair-defaults --offline [--org ORG] [--dry-run|--yes]
`)
}

func (c *command) printAdminClientKeysUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin clients keys list ORG CLIENT
  opencook admin clients keys show ORG CLIENT KEY
  opencook admin clients keys add ORG CLIENT --key-name NAME [flags]
  opencook admin clients keys update ORG CLIENT KEY [flags]
  opencook admin clients keys delete ORG CLIENT KEY --yes
`)
}
