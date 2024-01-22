# ResilientNpgsqlConnection

A wrapper around NpgsqlConnection that handles retries and reconnects.

Works with Postgresql and CockroachDB.

## Use as main connection

Using `ResilientConnection` as your main connection enables the best recovery.

```csharp
var connStr = @"Server=127.0.0.1;Port=26299;Database=testdb;User Id=root;...";
using var conn = new ResilientConnection(connStr);


var result = subject.Query<string>("SELECT ...")...
```

## Use as wrapper around an existing connection

This will reduce the scope of errors that can be recovered

```csharp
var connStr = @"Server=127.0.0.1;Port=26299;Database=testdb;User Id=root;...";
var baseConnection = new NpgsqlConnection(connStr);

using var conn = new ResilientConnection(baseConnection);
...
```

## Logging

By default, `ResilientConnection` logs messages to the console.
This can be changed with the static `ResilientConnection.Log`:

```csharp
ResilientConnection.Log = new MyLogHook();
```