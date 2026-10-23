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

package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// CompressionCodec identifies the algorithm used to compress values.
type CompressionCodec uint8

const (
	// CodecNone stores the value raw without compression.
	CodecNone CompressionCodec = 0

	// CodecZstd compresses values using zstandard at fastest speed.
	CodecZstd CompressionCodec = 1

	// CodecS2 compresses values using S2 block format.
	CodecS2 CompressionCodec = 2
)

func (c CompressionCodec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecZstd:
		return "zstd"
	case CodecS2:
		return "s2"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

const (
	// DefaultCompressionThreshold is the slotted-page payload size limit (4000 bytes).
	// Values above this threshold attempt compression.
	DefaultCompressionThreshold = BlockUsableSize - 64

	// DefaultMaxObjectSize is the maximum uncompressed object size accepted during decompression (64 MiB).
	DefaultMaxObjectSize = MaxRecordFieldLength

	// compressionHeaderSize is 1 byte codec + 4 bytes uncompressed length.
	compressionHeaderSize = 5
)

var (
	// ErrCorruptCompression indicates corrupt compression metadata, unknown codec, or invalid payload.
	ErrCorruptCompression = errors.New("corrupt compressed payload")

	// ErrObjectTooLarge indicates recorded uncompressed length exceeds the configured maximum object size.
	ErrObjectTooLarge = errors.New("uncompressed object length exceeds limit")
)

var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				panic(err)
			}
			return enc
		},
	}

	zstdDecoderPool = sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecodeAllCapLimit(true),
			)
			if err != nil {
				panic(err)
			}
			return dec
		},
	}
)

func compressValue(payload []byte, codec CompressionCodec, threshold int) ([]byte, error) {
	if codec == CodecNone || len(payload) <= threshold {
		out := make([]byte, 1+len(payload))
		out[0] = byte(CodecNone)
		copy(out[1:], payload)
		return out, nil
	}

	var compressed []byte
	switch codec {
	case CodecZstd:
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		compressed = enc.EncodeAll(payload, nil)
		zstdEncoderPool.Put(enc)
	case CodecS2:
		compressed = s2.Encode(nil, payload)
	default:
		return nil, fmt.Errorf("%w: unsupported codec %d", ErrCorruptCompression, codec)
	}

	// Store raw if compressed form isn't at least about 10% smaller.
	if int64(len(compressed))*10 > int64(len(payload))*9 {
		out := make([]byte, 1+len(payload))
		out[0] = byte(CodecNone)
		copy(out[1:], payload)
		return out, nil
	}

	out := make([]byte, compressionHeaderSize+len(compressed))
	out[0] = byte(codec)
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[5:], compressed)
	return out, nil
}

func decompressValue(data []byte, maxObjectSize int) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("%w: payload too short (%d bytes)", ErrCorruptCompression, len(data))
	}

	codec := CompressionCodec(data[0])
	switch codec {
	case CodecNone:
		return data[1:], nil

	case CodecZstd:
		if len(data) < compressionHeaderSize {
			return nil, fmt.Errorf("%w: truncated compressed record (%d bytes)", ErrCorruptCompression, len(data))
		}
		uncompressedLen := binary.BigEndian.Uint32(data[1:5])
		if uncompressedLen > uint32(maxObjectSize) {
			return nil, fmt.Errorf("%w: recorded length %d exceeds limit %d", ErrObjectTooLarge, uncompressedLen, maxObjectSize)
		}
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		dst := make([]byte, 0, uncompressedLen)
		decompressed, err := dec.DecodeAll(data[compressionHeaderSize:], dst)
		zstdDecoderPool.Put(dec)
		if err != nil {
			return nil, fmt.Errorf("%w: zstd decode failed: %v", ErrCorruptCompression, err)
		}
		if len(decompressed) != int(uncompressedLen) {
			return nil, fmt.Errorf("%w: decompressed length %d does not match recorded length %d", ErrCorruptCompression, len(decompressed), uncompressedLen)
		}
		return decompressed, nil

	case CodecS2:
		if len(data) < compressionHeaderSize {
			return nil, fmt.Errorf("%w: truncated compressed record (%d bytes)", ErrCorruptCompression, len(data))
		}
		uncompressedLen := binary.BigEndian.Uint32(data[1:5])
		if uncompressedLen > uint32(maxObjectSize) {
			return nil, fmt.Errorf("%w: recorded length %d exceeds limit %d", ErrObjectTooLarge, uncompressedLen, maxObjectSize)
		}
		expectedLen, err := s2.DecodedLen(data[compressionHeaderSize:])
		if err != nil {
			return nil, fmt.Errorf("%w: s2 decode length check failed: %v", ErrCorruptCompression, err)
		}
		if expectedLen != int(uncompressedLen) {
			return nil, fmt.Errorf("%w: s2 decoded length %d does not match recorded length %d", ErrCorruptCompression, expectedLen, uncompressedLen)
		}
		dst := make([]byte, 0, uncompressedLen)
		decompressed, err := s2.Decode(dst, data[compressionHeaderSize:])
		if err != nil {
			return nil, fmt.Errorf("%w: s2 decode failed: %v", ErrCorruptCompression, err)
		}
		if len(decompressed) != int(uncompressedLen) {
			return nil, fmt.Errorf("%w: decompressed length %d does not match recorded length %d", ErrCorruptCompression, len(decompressed), uncompressedLen)
		}
		return decompressed, nil

	default:
		return nil, fmt.Errorf("%w: unknown compression codec byte 0x%02x", ErrCorruptCompression, data[0])
	}
}
