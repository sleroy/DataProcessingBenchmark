using CommandLine;
using Microsoft.Data.SqlClient;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        Parser.Default.ParseArguments<Options>(args)
              .WithParsed<Options>(async opts => await RunBenchmark(opts));
    }

    static async Task RunBenchmark(Options opts)
    {
        if (opts.DataSource.Equals("SqlServer", StringComparison.OrdinalIgnoreCase))
        {
            await RunSqlServerBenchmark(opts);
        }
        else if (opts.DataSource.Equals("Redis", StringComparison.OrdinalIgnoreCase))
        {
            await RunRedisBenchmark(opts);
        }
        else
        {
            Console.WriteLine("Invalid data source specified. Use 'Redis' or 'SqlServer'.");
        }
    }

    static async Task RunSqlServerBenchmark(Options opts)
    {
        var connectionString = opts.ConnectionString;
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Create Table
        var createTableCmd = new SqlCommand("CREATE TABLE FinancialRecords (Id INT PRIMARY KEY IDENTITY, Amount DECIMAL(18, 2), Date DATETIME, Description NVARCHAR(255))", connection);
        await createTableCmd.ExecuteNonQueryAsync();

        // Insert Rows
        var tasks = new List<Task>();
        for (int i = 0; i < opts.Threads; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                var random = new Random();
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();
                for (int j = 0; j < opts.Rows / opts.Threads; j += opts.BatchSize)
                {
                    var cmdText = "INSERT INTO FinancialRecords (Amount, Date, Description) VALUES ";
                    for (int k = 0; k < opts.BatchSize; k++)
                    {
                        cmdText += $"({random.Next(1, 1000)}, '{DateTime.Now}', 'Sample Data {k}'),";
                    }
                    cmdText = cmdText.TrimEnd(',');
                    using var cmd = new SqlCommand(cmdText, conn);
                    await cmd.ExecuteNonQueryAsync();
                }
            }));
        }
        var stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"SQL Server: Inserted {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");

        // Process Rows
        var createProcessedTableCmd = new SqlCommand("CREATE TABLE ProcessedFinancialRecords (Id INT PRIMARY KEY IDENTITY, Amount DECIMAL(18, 2), Date DATETIME, Description NVARCHAR(255), ProcessedDate DATETIME)", connection);
        await createProcessedTableCmd.ExecuteNonQueryAsync();

        stopwatch.Restart();
        var processCmd = new SqlCommand("INSERT INTO ProcessedFinancialRecords (Amount, Date, Description, ProcessedDate) SELECT Amount, Date, Description, GETDATE() FROM FinancialRecords", connection);
        await processCmd.ExecuteNonQueryAsync();
        stopwatch.Stop();

        Console.WriteLine($"SQL Server: Processed {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");

        // Clear Table
        var clearTableCmd = new SqlCommand("TRUNCATE TABLE FinancialRecords", connection);
        await clearTableCmd.ExecuteNonQueryAsync();
    }

    static async Task RunRedisBenchmark(Options opts)
    {
        var redis = ConnectionMultiplexer.Connect(opts.ConnectionString);
        var db = redis.GetDatabase();

        // Create and Insert Rows
        var tasks = new List<Task>();
        for (int i = 0; i < opts.Threads; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                var random = new Random();
                for (int j = 0; j < opts.Rows / opts.Threads; j += opts.BatchSize)
                {
                    var batch = db.CreateBatch();
                    for (int k = 0; k < opts.BatchSize; k++)
                    {
                        var id = Guid.NewGuid().ToString();
                        await batch.HashSetAsync($"FinancialRecords:{id}", new HashEntry[]
                        {
                            new HashEntry("Amount", random.Next(1, 1000)),
                            new HashEntry("Date", DateTime.Now.ToString()),
                            new HashEntry("Description", $"Sample Data {k}")
                        });
                    }
                    batch.Execute();
                }
            }));
        }
        var stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"Redis: Inserted {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");

        // Process Rows
        tasks.Clear();
        var processedRecords = new List<Task>();
        stopwatch.Restart();
        var server = redis.GetServer(redis.GetEndPoints().First());
        var keys = server.Keys(pattern: "FinancialRecords:*");
        foreach (var key in keys)
        {
            tasks.Add(Task.Run(async () =>
            {
                var entries = await db.HashGetAllAsync(key);
                var id = Guid.NewGuid().ToString();
                await db.HashSetAsync($"ProcessedFinancialRecords:{id}", entries.Append(new HashEntry("ProcessedDate", DateTime.Now.ToString())).ToArray());
            }));
        }
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"Redis: Processed {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");

        // Clear Table
        foreach (var key in keys)
        {
            await db.KeyDeleteAsync(key);
        }
    }
}