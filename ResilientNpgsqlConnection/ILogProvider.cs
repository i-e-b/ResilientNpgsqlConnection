using System;

namespace ResilientNpgsqlConnection;

/// <summary>
/// Logging system injector for ResilientConnection
/// </summary>
public interface ILogProvider
{
    /// <summary>
    /// Message that indicates a possible fault condition that does not prevent operation.
    /// </summary>
    void Warn(string message, Exception ex);

    /// <summary>
    /// Message that indicates a fault condition that prevents operation.
    /// </summary>
    void Critical(string message, Exception ex);

    /// <summary>
    /// Message that indicates a fault condition that changes operation
    /// </summary>
    void Error(string message, Exception ex);
}