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

package gari

import (
	"flag"
	"strings"
)

type commaSeparatedSliceValue struct {
	slice *[]string
}

func (c commaSeparatedSliceValue) String() string {
	if c.slice == nil || len(*c.slice) == 0 {
		return ""
	}
	return strings.Join(*c.slice, ",")
}

func (c commaSeparatedSliceValue) Set(val string) error {
	for _, part := range strings.Split(val, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			*c.slice = append(*c.slice, trimmed)
		}
	}
	return nil
}

// CommaSeparatedSlice returns a flag.Value that binds a command-line flag
// to a string slice. Each occurrence of the flag is split on commas, trimmed of
// whitespace, and non-empty values are appended to the slice.
func CommaSeparatedSlice(slice *[]string) flag.Value {
	return commaSeparatedSliceValue{slice: slice}
}

type stringSliceValue struct {
	slice *[]string
}

func (s stringSliceValue) String() string {
	if s.slice == nil || len(*s.slice) == 0 {
		return ""
	}
	return strings.Join(*s.slice, ",")
}

func (s stringSliceValue) Set(val string) error {
	*s.slice = append(*s.slice, val)
	return nil
}

// StringSlice returns a flag.Value that appends flag values directly into the target slice.
func StringSlice(slice *[]string) flag.Value {
	return stringSliceValue{slice: slice}
}
