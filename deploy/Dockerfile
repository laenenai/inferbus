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
RUN CGO_ENABLED=0 go build -o /out/inferbus ./cmd/inferbus

FROM alpine:3.20
RUN adduser -D inferbus
COPY --from=build /out/inferbus /usr/local/bin/inferbus
USER inferbus
ENTRYPOINT ["inferbus"]
