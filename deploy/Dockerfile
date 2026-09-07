FROM golang:1.26-alpine AS build
# go.mod declares a newer Go language version than this builder image ships
# (golang:1.27-alpine does not exist on Docker Hub yet); GOTOOLCHAIN=auto lets
# the toolchain download and use the go.mod-declared version transparently
# instead of refusing the build (the default GOTOOLCHAIN=local would).
ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/infbus ./cmd/infbus

FROM alpine:3.20
RUN adduser -D infbus
COPY --from=build /out/infbus /usr/local/bin/infbus
USER infbus
ENTRYPOINT ["infbus"]
