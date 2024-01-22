using System;
using System.Collections;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using Npgsql;
using NpgsqlTypes;

namespace ResilientNpgsqlConnection;

/// <summary>
/// IDbCommand, plus some helpers
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[SuppressMessage("ReSharper", "UnusedType.Global")]
public static class DbCommandNpgsqlExtensions
{
    /// <summary>
    /// Add a sql parameter to this command.
    /// This should be referenced by name in the SQL query, as in <c>WHERE col = :myName</c>
    /// </summary>
    /// <param name="cmd">Command that should take the new parameter</param>
    /// <param name="name">Name or alias, as used in the SQL query</param>
    /// <param name="value">C# values to pass with the query</param>
    public static void AddWithValue(this IDbCommand cmd, string name, object? value)
    {
        if (cmd.Parameters is null) throw new Exception("Invalid database command: no Parameters object");
        cmd.Parameters.Add(new NpgsqlParameter(name, value));
    }

    /// <summary>
    /// Add a sql parameter to this command.
    /// This should be referenced by name in the SQL query, as in <c>WHERE col = :myName</c>
    /// </summary>
    /// <param name="cmd">Command that should take the new parameter</param>
    /// <param name="name">Name or alias, as used in the SQL query</param>
    /// <param name="type">The database type equivalent of the value</param>
    /// <param name="value">C# values to pass with the query</param>
    public static void AddParameter(this IDbCommand cmd, string name, NpgsqlDbType type, object? value)
    {
        if (cmd.Parameters is null) throw new Exception("Invalid database command: no Parameters object");
        cmd.Parameters.Add(new NpgsqlParameter(name, type){Value = value});
    }

    /// <summary>
    /// Add a sql parameter to this command.
    /// This should be referenced by a SQL built-in:
    /// such as <c>WHERE col = ANY(:myName)</c>, <c>WHERE col = SOME(:myName)</c>, or <c>WHERE col = ALL(:myName)</c>
    /// </summary>
    /// <param name="cmd">Command that should take the new parameter</param>
    /// <param name="name">Name or alias, as used in the SQL query</param>
    /// <param name="type">The database type of elements in the array</param>
    /// <param name="value">Array of values to pass with the query</param>
    public static void AddArrayParameter(this IDbCommand cmd, string name, NpgsqlDbType type, IEnumerable value)
    {
        if (cmd.Parameters is null) throw new Exception("Invalid database command: no Parameters object");
        // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
        cmd.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Array | type){Value = value});
    }
}