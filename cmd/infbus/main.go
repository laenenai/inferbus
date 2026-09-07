// infbus is a single binary with role subcommands: gateway, worker, harvester.
package main

import (
	"fmt"
	"io"
	"os"
)

const version = "0.1.0-dev"

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

func run(args []string, stdout io.Writer) int {
	if len(args) == 0 {
		usage(stdout)
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintf(stdout, "infbus %s\n", version)
		return 0
	case "gateway", "worker", "harvester":
		return notImplemented(args[0], stdout)
	default:
		fmt.Fprintf(stdout, "unknown subcommand %q\n", args[0])
		usage(stdout)
		return 2
	}
}

func notImplemented(role string, stdout io.Writer) int {
	fmt.Fprintf(stdout, "%s: not implemented yet\n", role)
	return 1
}

func usage(w io.Writer) {
	fmt.Fprint(w, "usage: infbus <gateway|worker|harvester|version>\n")
}
