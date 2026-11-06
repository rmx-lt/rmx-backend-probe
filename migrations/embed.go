// Package migrations holds the SQL schema migrations, embedded in the binary.
// They are applied by goose (see internal/db) in the order of their version
// prefix, which is a timestamp.
package migrations

import "embed"

// FS contains every *.sql migration file.
//
//go:embed *.sql
var FS embed.FS
