# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.27-bookworm AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
COPY snigateway/go.mod snigateway/go.sum ./snigateway/
COPY gcpgateway/go.mod gcpgateway/go.sum ./gcpgateway/

RUN go mod download
RUN cd snigateway && go mod download
RUN cd gcpgateway && go mod download

COPY . .

WORKDIR /app/gcpgateway
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o gcpgateway cmd/gcpgateway/main.go

FROM gcr.io/distroless/static-debian12
WORKDIR /
COPY --from=builder /app/gcpgateway/gcpgateway .
USER 65532:65532
ENTRYPOINT ["/gcpgateway"]
