# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY gen/ gen/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o /out/capstan-server ./cmd/capstan-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/capstan-server /capstan-server
EXPOSE 7233
USER nonroot
ENTRYPOINT ["/capstan-server", "serve"]
