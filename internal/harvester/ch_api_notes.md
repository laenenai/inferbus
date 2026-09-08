# clickhouse-go/v2 API notes (authority for chsink.go)

Pinned by `go get github.com/ClickHouse/clickhouse-go/v2@latest` on 2026-09-08:
resolved to **v2.48.0** (recorded in go.mod/go.sum).

This file is the source of truth for field/method names used in `chsink.go`.
If the sketch in the M4 design doc (`docs/design-usage.md` §2) or the task
brief diverges from what's below, **this file wins** — cite the relevant
section in code comments.

## `clickhouse.Open`

```
go doc github.com/ClickHouse/clickhouse-go/v2 Open
```

```go
package clickhouse // import "github.com/ClickHouse/clickhouse-go/v2"

func Open(opt *Options) (driver.Conn, error)
```

Takes an `*Options`, returns a `driver.Conn` (the native-API connection
interface) plus an error. No separate "ping" step is required by `Open`
itself, but we call `conn.Ping(ctx)` after opening to fail fast on a bad DSN
per this repo's fail-fast convention.

## `clickhouse.Options`

```
go doc github.com/ClickHouse/clickhouse-go/v2 Options
```

Relevant fields for us (full doc has many more, elided):

```go
type Options struct {
	Protocol   Protocol   // clickhouse.Native or clickhouse.HTTP
	Addr       []string   // host:port pairs
	Auth       Auth       // Database / Username / Password
	DialTimeout          time.Duration // default 30s
	MaxOpenConns         int
	MaxIdleConns         int
	ConnMaxLifetime      time.Duration
	// ... TLS, Settings, Compression, etc — not needed for our DSN-driven path
}
```

We never hand-build `Options`: we always go through `ParseDSN` (see below) so
the DSN string (`clickhouse://host:port/database?...`) is the single source
of connection config, matching how every other sink/store in this repo is
constructed (a DSN string in, `*Options` never exposed to callers).

## `clickhouse.ParseDSN`

```
go doc github.com/ClickHouse/clickhouse-go/v2 ParseDSN
```

```go
func ParseDSN(dsn string) (*Options, error)
```

Confirmed by reading `clickhouse_options.go` (`Options.fromDSN`) in the
module cache: DSN form is `clickhouse://[user[:password]@]host:port[,host2:port2...]/database?param=value`
(native protocol by default; `?protocol=http` or an `http://` scheme selects
HTTP transport). `ParseDSN` returns a fully-populated `*Options` we pass
straight to `clickhouse.Open`. We do **not** mutate the returned `*Options`
in `NewCHSink` — the DSN (including its `/database` path segment) already
carries the database we operate on. Tests that need a fresh throwaway
database append a differently-named database in their own DSN.

## `driver.Conn`

```
go doc github.com/ClickHouse/clickhouse-go/v2/lib/driver Conn
```

```go
type Conn interface {
	Contributors() []string
	ServerVersion() (*ServerVersion, error)
	Select(ctx context.Context, dest any, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
	PrepareBatch(ctx context.Context, query string, opts ...PrepareBatchOption) (Batch, error)
	Exec(ctx context.Context, query string, args ...any) error
	QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error) // experimental, unused
	InsertFormat(ctx context.Context, format string, query string, data io.Reader) error               // experimental, unused
	AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error                        // deprecated, unused
	Ping(context.Context) error
	Stats() Stats
	Close() error
}
```

Doc comment calls out an important compatibility rule: **`Conn` is meant to
be consumed, not implemented** — new methods land in minor releases, which
is a compile break for hand-implementers. `CHSink` therefore stores a
`driver.Conn` field directly (never wraps it behind a narrower
hand-rolled interface), and if we ever need a test double we'd embed
`driver.Conn` rather than implement every method.

We use:
- `Exec(ctx, ddl)` for `CREATE DATABASE`/`CREATE TABLE`/`CREATE MATERIALIZED
  VIEW` statements (DDL, no result set).
- `PrepareBatch(ctx, "INSERT INTO usage_events")` for `InsertBatch`.
- `QueryRow(ctx, query, args...).Scan(&total)` for `MonthToDate`.
- `Ping(ctx)` once after `Open`, to fail `NewCHSink` fast on bad DSN/unreachable
  server rather than surfacing the error on the first real query.
- `Close()` for `Sink.Close()`.

## `driver.Batch`

```
go doc github.com/ClickHouse/clickhouse-go/v2/lib/driver Batch
```

```go
type Batch interface {
	Abort() error
	Append(v ...any) error
	AppendStruct(v any) error
	Column(int) BatchColumn
	Flush() error
	Send() error
	IsSent() bool
	Rows() int
	Columns() []column.Interface
	Close() error
}
```

Doc's own usage sketch:

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
if err != nil { ... }
defer batch.Close() // cleanup if Send is not reached
for ... {
	_ = batch.Append(...)
}
_ = batch.Send()
```

`InsertBatch` follows exactly this shape: `PrepareBatch` once per call with
column order matching the `INSERT INTO usage_events (...)` column list,
`Append(v...)` per `Row` (positional, in column order — clickhouse-go has no
named-arg struct tag support for `Append`; `AppendStruct` requires a struct
whose exported field order/names match column order via reflection, which is
more fragile than an explicit `Append(...)` call given `Row`'s field names
don't match snake_case DDL columns, so we use `Append`, not `AppendStruct`),
then `defer batch.Close()` immediately after `PrepareBatch` succeeds (safe
per doc: "It is safe (and recommended) to call Close via defer immediately
after PrepareBatch... Close does not guarantee that buffered rows are sent"),
then `Send()` to finalize.

## `driver.Row` / `driver.Rows` (for `MonthToDate` and MV assertions)

```go
type Row interface {
	Err() error
	Scan(dest ...any) error
	ScanStruct(dest any) error
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	ScanStruct(dest any) error
	ColumnTypes() []ColumnType
	Totals(dest ...any) error
	Columns() []string
	Close() error
	Err() error
	HasData() bool
}
```

`MonthToDate` uses `conn.QueryRow(ctx, query, keyID, month).Scan(&total)`.
`SELECT sum(x)` on zero matching rows returns SQL NULL in ClickHouse: we
scan into a `sql.NullInt64` (not a bare `int64`) to avoid a scan error when
a key has no usage yet, and return 0 when it's not valid.

## `Auth` / `Protocol`

```go
type Auth struct {
	Database string
	Username string
	Password string
}

type Protocol int
const Native Protocol = iota // ... (Native, HTTP)
```

Not constructed directly by us (see ParseDSN note above) — recorded here
only because the brief asked for it and because it explains why our DSNs
look like `clickhouse://default:@host:port/dbname` (native protocol,
`Auth.Database` = path segment after the host).

## Divergences from the brief's sketch

None material. The brief's sketch (`Exec`/`Query`/`PrepareBatch` on
`driver.Conn`, `Append`/`Send` on `driver.Batch`) matches the real v2.48.0
API. The one clarification worth flagging: `MonthToDate`'s `SELECT sum(...)`
must scan into a nullable type because ClickHouse returns NULL, not 0, for
`sum()` over zero rows.
