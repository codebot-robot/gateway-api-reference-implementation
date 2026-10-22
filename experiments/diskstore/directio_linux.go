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

//go:build linux

package diskstore

import (
	"golang.org/x/sys/unix"
)

func platformPreadv(fd uintptr, off int64, iov [][]byte) error {
	_, err := unix.Preadv(int(fd), iov, off)
	return err
}

func platformPwritev(fd uintptr, off int64, iov [][]byte) error {
	_, err := unix.Pwritev(int(fd), iov, off)
	return err
}

// DirectIOOpenFlag returns unix.O_DIRECT on Linux.
func DirectIOOpenFlag() int {
	return unix.O_DIRECT
}

func applyPlatformDirectIOFlags(fd uintptr) error {
	return nil
}
