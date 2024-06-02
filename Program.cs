using CommandLine;
using Microsoft.Data.SqlClient;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

public class FinancialRecord
{
    public decimal Amount { get; set; }
    public DateTime Date { get; set; }
    public string Description { get; set; }
}


class Program
{
    static async Task Main(string[] args)
    {
       await Parser.Default.ParseArguments<Options>(args)
                  .WithParsedAsync<Options>(RunBenchmark);
        Console.WriteLine("Program completed.");

    }

    static async Task RunBenchmark(Options opts)    
    {
        try
        {
            await RunBenchmarkImpl(opts);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static async Task RunBenchmarkImpl(Options opts)
    {
        Console.WriteLine($"Running benchmark with {opts.Threads} threads and {opts.Rows} rows");
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
        Console.WriteLine("Running SQL Server benchmark...");
        // Connect to SQL Server
        var connectionString = opts.ConnectionString;
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        Console.WriteLine("Connected to SQL Server");


        // Create Table
        var dropTableCmd = new SqlCommand("DROP TABLE FinancialRecords", connection);
        await dropTableCmd.ExecuteNonQueryAsync();
        
        var createTableCmd = new SqlCommand("CREATE TABLE FinancialRecords (Id INT PRIMARY KEY IDENTITY, Amount DECIMAL(18, 2), Date DATETIME, Description NVARCHAR(255))", connection);
        await createTableCmd.ExecuteNonQueryAsync();
        await new SqlCommand("TRUNCATE TABLE ProcessedFinancialRecords", connection).ExecuteNonQueryAsync();

        // Insert Rows with Transactions and Batch Inserts
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
                  using var transaction = (SqlTransaction) await conn.BeginTransactionAsync();
                  var cmdText = "INSERT INTO FinancialRecords (Amount, Date, Description) VALUES ";

                  for (int k = 0; k < opts.BatchSize; k++)
                  {
                      cmdText += $"({random.Next(1, 1000)}, '{DateTime.Now}', 'Sample Data {k}'),";
                  }

                  cmdText = cmdText.TrimEnd(',');

                  using var cmd = new SqlCommand(cmdText, conn, transaction);
                  try
                  {
                      await cmd.ExecuteNonQueryAsync();
                      await transaction.CommitAsync();
                  }
                  catch
                  {
                      await transaction.RollbackAsync();
                      throw;
                  }
              }
          }));
      }

        var stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"SQL Server: Inserted {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");

        // Process Rows
        var dropTableCmd2 = new SqlCommand("DROP TABLE ProcessedFinancialRecords", connection);
        await dropTableCmd2.ExecuteNonQueryAsync();

        var createProcessedTableCmd = new SqlCommand("CREATE TABLE ProcessedFinancialRecords (Id INT PRIMARY KEY IDENTITY, Amount DECIMAL(18, 2), Date DATETIME, Description NVARCHAR(255), ProcessedDate DATETIME)", connection);
        await createProcessedTableCmd.ExecuteNonQueryAsync();

        stopwatch.Restart();
        await ProcessRecordsAsync(opts.BatchSize, connection, connectionString);
        stopwatch.Stop();

        Console.WriteLine($"SQL Server: Processed {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");


    }


    public static async Task ProcessRecordsAsync(int batchSize,SqlConnection connection, String connectionString)
    {
        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        var records = await GetFinancialRecordsAsync(conn);

        var batches = SplitIntoBatches(records, batchSize);

        var tasks = batches.Select(batch => Task.Run(() => ProcessBatchAsync(batch, connectionString))).ToArray();

        await Task.WhenAll(tasks);
    }

    private static async Task<List<FinancialRecord>> GetFinancialRecordsAsync(SqlConnection connection)
    {
        var records = new List<FinancialRecord>();

        var query = "SELECT Amount, Date, Description FROM FinancialRecords";
        using (var cmd = new SqlCommand(query, connection))
        {
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    records.Add(new FinancialRecord
                    {
                        Amount = reader.GetDecimal(0),
                        Date = reader.GetDateTime(1),
                        Description = reader.GetString(2)
                    });
                }
            }
        }

        return records;
    }

    private static List<List<FinancialRecord>> SplitIntoBatches(List<FinancialRecord> records, int batchSize)
    {
        return records
            .Select((record, index) => new { record, index })
            .GroupBy(x => x.index / batchSize)
            .Select(g => g.Select(x => x.record).ToList())
            .ToList();
    }

    private static async Task ProcessBatchAsync(List<FinancialRecord> batch, String connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        var insertQuery = "INSERT INTO ProcessedFinancialRecords (Amount, Date, Description, ProcessedDate) VALUES (@Amount, @Date, @Description, GETDATE())";

        using (var transaction = connection.BeginTransaction())
        {
            using (var cmd = new SqlCommand(insertQuery, connection, transaction))
            {
                cmd.Parameters.Add(new SqlParameter("@Amount", System.Data.SqlDbType.Decimal));
                cmd.Parameters.Add(new SqlParameter("@Date", System.Data.SqlDbType.DateTime));
                cmd.Parameters.Add(new SqlParameter("@Description", System.Data.SqlDbType.NVarChar, 255));

                foreach (var record in batch)
                {
                    cmd.Parameters["@Amount"].Value = record.Amount;
                    cmd.Parameters["@Date"].Value = record.Date;
                    cmd.Parameters["@Description"].Value = record.Description;

                    await cmd.ExecuteNonQueryAsync();
                }
            }

            transaction.Commit();
        }
    }

    static async Task RunRedisBenchmark(Options opts)
    {
         var redis = await ConnectionMultiplexer.ConnectAsync(opts.ConnectionString);
         var server = redis.GetServer(redis.GetEndPoints().First());
         var db = redis.GetDatabase();

         // Clear Table
         await db.KeyDeleteAsync(server.Keys(pattern: "FinancialRecords:*").ToArray());
         await db.KeyDeleteAsync(server.Keys(pattern: "ProcessedFinancialRecords:*").ToArray());



         // Create and Insert Rows
         var tasks = new List<Task>();
         for (int i = 0; i < opts.Threads; i++)
         {
             tasks.Add(Task.Run(async () =>
             {
                 var random = new Random();
                 var batch = db.CreateBatch();
                 for (int j = 0; j < opts.Rows / opts.Threads; j += opts.BatchSize)
                 {
                     for (int k = 0; k < opts.BatchSize; k++)
                     {
                         var id = Guid.NewGuid().ToString();
                         batch.HashSetAsync($"FinancialRecords:{id}", new HashEntry[]
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
         var keys = server.Keys(pattern: "FinancialRecords:*").ToArray();
         Console.WriteLine($"Redis: obtained the FinancialRecords keys => {keys.Length}");
         stopwatch.Restart();
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
         // Retrieve and display the number of processed records
        var processedKeys = server.Keys(pattern: "ProcessedFinancialRecords:*").ToArray();
        Console.WriteLine($"Number of processed records: {processedKeys.Length}");

         Console.WriteLine($"Redis: Processed {opts.Rows} rows in {stopwatch.ElapsedMilliseconds} ms");
    }
}
