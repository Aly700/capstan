// Command capstan-server runs the Capstan server. Implemented by the server lane; this stub
// exists so the image and CI can be built against the real entrypoint path from the start.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "capstan-server: not implemented yet (server lane)")
	os.Exit(1)
}
