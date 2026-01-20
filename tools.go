//go:build tools

// Package tools provides tool dependencies for go generate.
// This file ensures oapi-codegen is tracked in go.mod.
package tools

import (
	_ "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
)
