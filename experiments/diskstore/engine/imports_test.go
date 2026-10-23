// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine_test

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestNoKubernetesImports(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read engine directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := entry.Name()
		node, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("failed to parse %s: %v", path, err)
		}
		for _, imp := range node.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(importPath, "k8s.io/") {
				t.Errorf("file %s illegally imports %s: engine package must not depend on k8s.io", path, importPath)
			}
		}
	}
}
