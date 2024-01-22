namespace ResilientNpgsqlConnection;

/// <summary>
/// Type of failure
/// </summary>
public enum RetryType
{
    /// <summary>
    /// Not a failure that should be retried
    /// </summary>
    DoNotRetry = 1,
    
    /// <summary>
    /// This query might succeed if we try again
    /// </summary>
    CanBeRetried = 2,
    
    /// <summary>
    /// There is a fault with the connection. Try to purge all cached connections and retry
    /// </summary>
    PurgeAndRetry = 3,
    
    /// <summary>
    /// This query is somehow invalid -- it can't
    /// work under any circumstances.
    /// We should notify the dev team to fix this.
    /// </summary>
    CriticalFailure = 99
}