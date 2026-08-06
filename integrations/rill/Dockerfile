# syntax=docker/dockerfile:1

FROM golang:1.25.6-bookworm AS build
WORKDIR /src
COPY integrations/rill/go.mod integrations/rill/go.sum ./
RUN go mod download
COPY integrations/rill/ ./
RUN CGO_ENABLED=1 go build -trimpath -o /out/rill .

FROM rilldata/rill:v0.88.6
COPY --from=build /out/rill /usr/local/bin/rill
