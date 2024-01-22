using System;

namespace ResilientNpgsqlConnection;

/// <summary>
/// Basic ResilientConnection logging that writes to the console
/// </summary>
public class ConsoleLogProvider : ILogProvider
{
    /// <inheritdoc />
    public void Warn(string message, Exception ex)
    {
        Console.WriteLine($"WARN: {message}\r\n{ex}");
    }

    /// <inheritdoc />
    public void Critical(string message, Exception ex)
    {
        Console.WriteLine($"CRITICAL: {message}\r\n{ex}");
    }

    /// <inheritdoc />
    public void Error(string message, Exception ex)
    {
        Console.WriteLine($"ERR: {message}\r\n{ex}");
    }
}