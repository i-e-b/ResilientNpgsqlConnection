using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace ResilientNpgsqlConnection;

/// <summary>
/// A wrapper for Npgsql database connections.
/// This adds retry logic for transient failures,
/// and logging for SQL syntax and schema errors.
/// </summary>
public class ResilientConnection: DbConnection
{
    /// <summary>
    /// Logging injector
    /// </summary>
    // ReSharper disable once FieldCanBeMadeReadOnly.Global
    // ReSharper disable once MemberCanBePrivate.Global
    public static ILogProvider Log = new ConsoleLogProvider();

    /// <summary>
    /// If set to <c>true</c>, retries will be skipped.
    /// </summary>
    public static bool TestMode { get; set; }

    /// <summary>
    /// Connection string used for queries and commands
    /// </summary>
    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value;
    }

    /// <summary>
    /// Timeout used for sql queries and commands
    /// </summary>
    public override int ConnectionTimeout { get; }

    /// <summary>
    /// Name of the database being targeted
    /// </summary>
    public override string Database => _database;

    /// <summary>
    /// The name of the database server to which to connect. The default value is an empty string.
    /// </summary>
    public override string DataSource => "";

    /// <summary>
    /// The version of the database. The default value is an empty string.
    /// </summary>
    public override string ServerVersion => "";

    /// <summary>
    /// Current connection state. This is managed automatically.
    /// </summary>
    public override ConnectionState State => _source.State;
    
    /// <summary>
    /// Maximum number of failures before a query is abandoned and and exception raised.
    /// </summary>
    public static int MaximumRetryCount { get; set; } = 9;
    
    /// <summary> Optional waiting action. Mainly used for testing </summary>
    private Action<int>? _waitAction;
    private static long _totalFailureCount;
    private static long _totalSuccessCount;
    private static readonly Random _random = new();
    
    private NpgsqlConnection _source;
    private bool _didDispose;
    /// <summary> Set to true if an open has ever worked. Used to detect invalid connection strings vs need to purge </summary>
    private bool _hasEverOpened;
    
    private readonly object _lock = new();
    private readonly List<RcCommandWrapper> _createdCommands;
    private string _database;
    private string _connectionString;


    /// <summary>
    /// Total number of database calls that have failed across all connections.
    /// This will include retries that also failed.
    /// </summary>
    public static long TotalFailures => _totalFailureCount;
        
    /// <summary>
    /// Total number of database calls that have succeeded across all connections.
    /// This will include successful retries after a failure.
    /// </summary>
    public static long TotalSuccesses => _totalSuccessCount;

    /// <summary>
    /// Wrap an existing Npgsql connection.
    /// <p/>
    /// <b>Note:</b> If the original connection string does not contain
    /// <c>Persist Security Info=true</c>, then connections may not re-establish correctly.
    /// </summary>
    public ResilientConnection(NpgsqlConnection source)
    {
        _didDispose = false;
        _source = source;
        _createdCommands = new List<RcCommandWrapper>();
        _connectionString = source.ConnectionString ?? throw new Exception("source connection does not hold a connection string");
        ConnectionTimeout = source.ConnectionTimeout;
        _database = source.Database;
    }
    
    /// <summary>
    /// Create a new connection using a connection string
    /// </summary>
    public ResilientConnection(string connectionString)
    {
        _didDispose = false;
        _source = new NpgsqlConnection(connectionString);
        _createdCommands = new List<RcCommandWrapper>();
        _connectionString = connectionString; // note: Npgsql may trim off the password if the connection has ever opened, so we store our own copy here.
        ConnectionTimeout = _source.ConnectionTimeout;
        _database = _source.Database;
    }

    /// <summary>
    /// Remove instance via GC
    /// </summary>
    ~ResilientConnection()
    {
        Dispose(false);
    }

    /// <summary>Dispose of underlying database connection, and any associated commands</summary>
    protected override void Dispose(bool disposing)
    {
        // ReSharper disable ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (_lock is null || _source is null) return;
        // ReSharper restore ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        lock (_lock)
        {
            if (_didDispose) return;
            try
            {
                _source.Close();
                _source.Dispose();
            }
            catch (Exception ex)
            {
                Log.Warn("Error during dispose: ", ex);
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            if (_createdCommands is not null) // this can go null in some weird edge cases
            {
                foreach (var cmd in _createdCommands)
                {
                    try
                    {
                        if (cmd is null) continue;
                        cmd.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Log.Warn("Error during dispose: ", ex);
                    }
                }

                _createdCommands.Clear();
            }

            _didDispose = true;
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Not supported.
    /// </summary>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotSupportedException($"Transactions not supported in {nameof(ResilientConnection)}.");
    }

    /// <summary>Changes the current database for an open <see langword="Connection" /> object.</summary>
    /// <param name="databaseName">The name of the database to use in place of the current database.</param>
    public override void ChangeDatabase(string databaseName)
    {
        _source.ChangeDatabase(databaseName);
        _database = databaseName;
    }

    /// <inheritdoc />
    public override void Open()
    {
        var originalCaller = new StackTrace();
        Retry(() => {
            if (_source.State != ConnectionState.Open) _source.Open();
            _hasEverOpened = true;
        }, "<open connection>", originalCaller);
    }

    /// <inheritdoc />
    public override void Close() {
        if (_source.State != ConnectionState.Closed) _source.Close();
    }

    /// <summary>
    /// Creates and returns a Command object associated with the connection.
    /// This will include a retry- and reconnect- wrapper.
    /// </summary>
    protected override DbCommand CreateDbCommand()
    {
        var cmd = new RcCommandWrapper(this);
        _createdCommands.Add(cmd);
        return cmd;
    }
    
    /// <summary>
    /// Returns true for codes that mean the exception can be safely retried
    /// (that is, we know no changes occurred)
    /// </summary>
    public static RetryType CanBeRetried(NpgsqlException ex)
    {
        if (TestMode) return RetryType.DoNotRetry;
        
        var sqlState = (ex as PostgresException)?.SqlState;
        var hResult = (uint)ex.HResult;

        if (sqlState == PostgresErrorCodes.SyntaxError ||
            sqlState == PostgresErrorCodes.InvalidSchemaName ||
            IsSyntaxErrorClass(sqlState))
        {
            return RetryType.CriticalFailure;
        }

        if ((sqlState is null && ex.InnerException is SocketException) ||
            
            sqlState == PostgresErrorCodes.AdminShutdown ||
            sqlState == PostgresErrorCodes.CrashShutdown ||
            sqlState == PostgresErrorCodes.CannotConnectNow ||
            sqlState == PostgresErrorCodes.SqlServerRejectedEstablishmentOfSqlConnection ||
            sqlState == PostgresErrorCodes.SqlClientUnableToEstablishSqlConnection ||
            sqlState == PostgresErrorCodes.ConnectionFailure || 
            sqlState == PostgresErrorCodes.ConnectionException
           ) // likely a fault took the server down. Clear Npgsql pool
        {
            return RetryType.PurgeAndRetry;
        }
        
        if (sqlState == PostgresErrorCodes.InvalidPassword ||
            sqlState == PostgresErrorCodes.InvalidAuthorizationSpecification ||
            (hResult == 0x80004005u && MatchesOneOf(ex.Message, "No password has been provided", "remote wall time is too far ahead", "operation \"get-user-session\" timed out")))
        {
            Log.Warn("Possible CockroachDB cluster fault", ex);
            return RetryType.PurgeAndRetry;
        }

        if (ex.IsTransient) return RetryType.CanBeRetried;
        
        if (ex.InnerException is EndOfStreamException) return RetryType.CanBeRetried;
        
        // Transactions and conflicts
        var canRetry =
            sqlState == PostgresErrorCodes.TransactionRollback ||
            sqlState == PostgresErrorCodes.TransactionIntegrityConstraintViolation ||
            sqlState == PostgresErrorCodes.SerializationFailure ||
            sqlState == PostgresErrorCodes.StatementCompletionUnknown ||
            sqlState == PostgresErrorCodes.DeadlockDetected ||
            // Admin intervention or server maintenance
            sqlState == PostgresErrorCodes.QueryCanceled ||
            sqlState == PostgresErrorCodes.OperatorIntervention ||
            sqlState == PostgresErrorCodes.AdminShutdown ||
            sqlState == PostgresErrorCodes.CrashShutdown ||
            sqlState == PostgresErrorCodes.CannotConnectNow ||
            // Transient overload
            sqlState == PostgresErrorCodes.TooManyConnections ||
            hResult == 0x80004005u ||
            ex.Message?.Contains("restart transaction") == true;
        
        return canRetry ? RetryType.CanBeRetried : RetryType.DoNotRetry;
    }

    private static bool MatchesOneOf(string? message, params string[] matches) => message is not null && matches.Any(message.Contains);

    /// <summary>
    /// Returns true if the SqlState string matches the Syntax Error class
    /// (see <see cref="Npgsql.PostgresErrorCodes"/>)
    /// </summary>
    private static bool IsSyntaxErrorClass(string? sqlState)
    {
        // See Npgsql.PostgresErrorCodes
        if (sqlState is null) return false;
        return sqlState.Length == 5 && sqlState.StartsWith("42");
    }

    /// <summary>
    /// Get the underlying NpgsqlConnection.
    /// If the connection is closed, it will be opened.
    /// If the connection is broken, it will be recreated.
    /// </summary>
    /// <returns></returns>
    public NpgsqlConnection GetRealConnection()
    {
        lock (_lock)
        {
            _source = RecreateAndOpenIfNeeded(_source);
            return _source;
        }
    }

    /// <summary>
    /// Reset success and failure counts to zero
    /// </summary>
    public static void ResetStatistics()
    {
        _totalFailureCount = 0;
        _totalSuccessCount = 0;
    }

    /// <summary>
    /// Replace the default wait (Thread.Sleep) with another action.
    /// If called with <c>null</c>, the default wait is restored.
    /// </summary>
    public void SetWaiter(Action<int>? waitAction)
    {
        _waitAction = waitAction;
    }

    /// <summary>
    /// Don't call this directly. Use <see cref="GetRealConnection"/>
    /// </summary>
    private NpgsqlConnection RecreateAndOpenIfNeeded(NpgsqlConnection oldConnection)
    {
        if (oldConnection.State == ConnectionState.Open) return oldConnection; // should be fine

        try
        {
            // See if it's just not opened yet
            oldConnection.Open();
            _hasEverOpened = true;
            return oldConnection;
        }
        catch
        {
            oldConnection.Dispose();
            var newConnection = new NpgsqlConnection(ConnectionString);
            newConnection.Open();
            _hasEverOpened = true;
            return newConnection;
        }
    }

    /// <summary>
    /// Sleep the current thread, with a scattered exponential wait time,
    /// based on the number of failed attempts so far
    /// </summary>
    private void BackoffWait(int attempts)
    {
        /*
round 0, 101..200
round 1, 201..300
round 2, 401..500
round 3, 801..900
round 4, 1601..1700
round 5, 3201..3300
round 6, 6401..6500
round 7, 12801..12900
round 8, 25601..25700*/
        if (attempts > 8) // stop growing here
        {
            Thread.Sleep(30_000 + _random.Next(30_000));
            return;
        }

        var sleepDuration = Math.Pow(2, attempts) * 100 + _random.Next(99) + 1;
        if (_waitAction is null) { Thread.Sleep((int)sleepDuration); }
        else { _waitAction((int)sleepDuration); }
    }
    
    /// <summary>
    /// Retry an action (with no return value)
    /// based on the configured retry count and wait process.
    /// </summary>
    private void Retry(Action action, string? queryString, StackTrace originalCaller)
    {
        Retry(()=>{
            action();
            return 1;
        }, queryString, originalCaller);
    }

    /// <summary>
    /// Retry a function, returning the value of the first successful call,
    /// based on the configured retry count and wait process.
    /// </summary>
    private T Retry<T>(Func<T> func, string? queryString, StackTrace originalCaller) {
        var lastException = new Exception("Unexpected state");
        
        for (int i = 0; i < MaximumRetryCount; i++)
        {
            try
            {
                var result = func();
                Interlocked.Increment(ref _totalSuccessCount);
                return result;
            }
            catch (NpgsqlException ex) // which includes `PostgresException` and `NpgsqlOperationInProgressException`
            {
                Interlocked.Increment(ref _totalFailureCount);
                lastException = ex;

                if (ex.InnerException is ObjectDisposedException)
                {
                    Log.Warn($"{nameof(ResilientConnection)} - Internal error occurred on connection to '{Database}', but can be retried 0x{ex.ErrorCode:X}; from {originalCaller}", ex);
                    RecreateConnection();
                }

                var errorType = CanBeRetried(ex);
                switch (errorType)
                {
                    case RetryType.CanBeRetried:
                        Log.Warn($"{nameof(ResilientConnection)} - SQL error occurred on '{Database}', but can be retried 0x{ex.ErrorCode:X}; from {originalCaller}", ex);
                        break;
                    
                    case RetryType.PurgeAndRetry:
                        NpgsqlConnection.ClearAllPools();
                        if (_hasEverOpened)
                        {
                            Log.Warn($"{nameof(ResilientConnection)} - SQL error occurred on '{Database}', but can be retried 0x{ex.ErrorCode:X} after connection purge; from {originalCaller}", ex);
                            BackoffWait(i); // Extra wait when connection has issues -- might be broken cluster
                        }
                        else
                        {
                            Log.Critical($"{nameof(ResilientConnection)} - Connection string might be invalid; from {originalCaller}", ex);
                        }

                        break;
                    
                    case RetryType.CriticalFailure:
                    {
                        if (string.IsNullOrWhiteSpace(queryString))
                        {
                            Log.Critical($"{nameof(ResilientConnection)} - An invalid query was run against '{Database}' from {originalCaller}", ex);
                        }
                        else
                        {
                            Log.Critical($"{nameof(ResilientConnection)} - An invalid query was run against '{Database}' from {originalCaller}:" +
                                         $"\r\n\r\n    {queryString.Replace("\n", "    \n")}\r\n", ex);
                        }

                        throw;
                    }
                    
                    case RetryType.DoNotRetry:
                    default:
                        Log.Error($"{nameof(ResilientConnection)} - Non retry error on '{Database}': 0x{ex.ErrorCode:X}; from {originalCaller}", ex);
                        throw new Exception($"{nameof(ResilientConnection)} - SQL error on '{Database}'. Code = 0x{ex.ErrorCode:X}. From {originalCaller}", ex);
                }
            }
            catch (TimeoutException tex)
            {
                Log.Warn($"{nameof(ResilientConnection)} - Timeout error occurred on '{Database}'; from {originalCaller}", tex);
            }
            catch (SocketException sockEx)
            {
                // This happens if there is a network issue between client and the SQL server.
                // Retry and hope it is resolved
                Log.Warn($"{nameof(ResilientConnection)} - Network error occurred on '{Database}', but can be retried {sockEx.Message}; from {originalCaller}", sockEx);
                RecreateConnection();
            }
            catch (InvalidOperationException ioEx)
            {
                // This happens if the connection state is messed up.
                // Retry and hope it settles down
                Log.Warn($"{nameof(ResilientConnection)} - Operation error occurred on '{Database}', but can be retried {ioEx.Message}; from {originalCaller}", ioEx);
            }
            catch (Exception oex)
            {
                if (LooksLikeNetworkFault(oex))
                {
                    Log.Warn($"{nameof(ResilientConnection)} - Suspected network error occurred on '{Database}', but can be retried; from {originalCaller}", oex);
                }
                else
                {
                    Interlocked.Increment(ref _totalFailureCount);
                    throw new Exception($"Unexpected error in {nameof(ResilientConnection)}. Type='{oex.GetType()?.Name}'. From {originalCaller}", oex);
                }
            }

            BackoffWait(i);
        } // for (int i = 0; i < MaximumRetryCount; i++)
        
        throw new Exception($"{nameof(ResilientConnection)} - SQL failed on '{Database}' after maximum retries ({MaximumRetryCount}) attempted. From {originalCaller}", lastException);
    }

    /// <summary>
    /// Second chance to detect network faults.
    /// This should be caught by <c>catch (SocketException sockEx)</c>
    /// </summary>
    private static bool LooksLikeNetworkFault(Exception ex)
    {
        if (ex.GetType()?.Name == "System.Net.Internals.SocketExceptionFactory+ExtendedSocketException") return true;
        if (ex.Message?.Contains("Resource temporarily unavailable") == true) return true;
        return false;
    }

    /// <summary>
    /// Dispose of old connection and create a new one
    /// </summary>
    private void RecreateConnection()
    {
        lock (_lock)
        {
            var oldConnection = _source;
            _source = new NpgsqlConnection(ConnectionString);
            try
            {
                oldConnection.Dispose();
            }
            catch
            {
                // ignore
            }
        }
    }

    /// <summary>
    /// Retry and reconnection wrapper for database commands
    /// </summary>
    /// <remarks>
    /// It is much simpler to implement only <see cref="IDbCommand"/>,
    /// but Dapper requires <see cref="DbCommand"/> for async calls.
    /// </remarks>
    private class RcCommandWrapper : DbCommand
    {
        private readonly ResilientConnection _parent;
        private readonly NpgsqlCommand _dummy;
        /// <summary>List of 'inner' commands we've generated. We will dispose of these at our dispose time</summary>
        private readonly List<NpgsqlCommand> _danglingCommands;
        private bool _didDispose;

        public override string CommandText {
            get => _dummy.CommandText;
            set => _dummy.CommandText = value;
        }
        public override int CommandTimeout {
            get => _dummy.CommandTimeout;
            set => _dummy.CommandTimeout = value;
        }
        public override CommandType CommandType {
            get => _dummy.CommandType;
            set => _dummy.CommandType = value;
        }
        protected override DbConnection? DbConnection {
            get => _parent;
            set => throw new Exception("Resilient command does not support switching connection");
        }
        protected override DbParameterCollection DbParameterCollection { get; }
        protected override DbTransaction? DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        
        /// <summary>
        /// Create a new command wrapper.
        /// This should NOT be called directly.
        /// Use <see cref="DbConnection.CreateCommand"/> to create a command.
        /// </summary>
        public RcCommandWrapper(ResilientConnection parent)
        {
            _didDispose = false;
            _parent = parent;
            _dummy = new NpgsqlCommand();
            DbParameterCollection = _dummy.Parameters;
            _danglingCommands = new();
        }

        /// <summary>
        /// Dispose of child objects at garbage collection, if not already disposed.
        /// </summary>
        ~RcCommandWrapper()
        {
            if (_didDispose) return;
            Dispose();
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            _didDispose = true;
            foreach (var cmd in _danglingCommands)
            {
                try
                {
                    if (cmd is null) continue;
                    cmd.Dispose();
                }
                catch (Exception ex)
                {
                    Log.Warn("Error during dispose: ", ex);
                }
            }
            _dummy.Dispose();
            _danglingCommands.Clear();

            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public override void Cancel()
        {
            foreach (var cmd in _danglingCommands)
            {
                cmd.Cancel();
            }
        }

        /// <summary>
        /// For most purposes, use <c>"AddParameter"</c> or <c>"AddArrayParameter"</c> instead of this.
        /// Creates a new instance of an <see cref="T:System.Data.IDbDataParameter" /> object.
        /// </summary>
        protected override DbParameter CreateDbParameter() => _dummy.CreateParameter();

        /// <inheritdoc />
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var originalCaller = new StackTrace();

            try
            {
                var result = _parent.Retry(() => ReconnectCommand().ExecuteReader(behavior), CommandText, originalCaller);
                return Task.FromResult((DbDataReader)result);
            }
            catch (Exception ex)
            {
                return Task.FromException<DbDataReader>(ex);
            }
        }

        /// <inheritdoc />
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var originalCaller = new StackTrace();
            return _parent.Retry(() => ReconnectCommand().ExecuteReader(behavior), CommandText, originalCaller);
        }
        
        /// <inheritdoc />
        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            var originalCaller = new StackTrace();
            try
            {
                var result = _parent.Retry(()=>ReconnectCommand().ExecuteScalar(), CommandText, originalCaller);
                return Task.FromResult(result);
            }
            catch (Exception ex)
            {
                return Task.FromException<object?>(ex);
            }
        }
        
        /// <inheritdoc />
        public override object? ExecuteScalar()
        {
            var originalCaller = new StackTrace();
            return _parent.Retry(()=>ReconnectCommand().ExecuteScalar(), CommandText, originalCaller);
        }
        
        /// <inheritdoc />
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            var originalCaller = new StackTrace();
            try
            {
                return Task.FromResult(_parent.Retry(()=>ReconnectCommand().ExecuteNonQuery(), CommandText, originalCaller));
            }
            catch (Exception ex)
            {
                return Task.FromException<int>(ex);
            }
        }

        /// <inheritdoc />
        public override int ExecuteNonQuery()
        {
            var originalCaller = new StackTrace();
            return _parent.Retry(() => ReconnectCommand().ExecuteNonQuery(), CommandText, originalCaller);
        }

        /// <inheritdoc />
        public override void Prepare()
        {
            throw new NotImplementedException("Prepared commands are not supported by the ResilientConnection adaptor");
        }

        /// <summary>
        /// Start a real command, copy details over from dummy. Re-connect and retry as needed.
        /// </summary>
        /// <returns></returns>
        private NpgsqlCommand ReconnectCommand()
        {
            var conn = _parent.GetRealConnection();
            var cmd = conn.CreateCommand();
            _danglingCommands.Add(cmd);
            
            cmd.CommandType = CommandType;
            cmd.CommandText = CommandText;
            
            var plist = _dummy.Parameters.ToArray();
            foreach (var p in plist)
            {
                cmd.Parameters.Add(p.Clone());
            }
            return cmd;
        }
    }


    private async ValueTask DisposeAsyncCore()
    {
        Dispose(true);
        await _source.DisposeAsync();
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        GC.SuppressFinalize(this);
    }
}