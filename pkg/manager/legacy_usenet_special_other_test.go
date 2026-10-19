//go:build !unix

package manager

import "fmt"

func createLegacySpecialArtifact(string) error {
	return fmt.Errorf("special-file fixture is unsupported")
}
