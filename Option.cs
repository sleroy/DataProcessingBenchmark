using CommandLine;

public class Options
{
    [Option('d', "dataSource", Required = true, HelpText = "Specify the data source: Redis or SqlServer.")]
    public string DataSource { get; set; }

    [Option('c', "connection", Required = true, HelpText = "Connection string for the data source.")]
    public string ConnectionString { get; set; }

    [Option('b', "batchSize", Default = 1000, HelpText = "Number of rows written at once.")]
    public int BatchSize { get; set; }

    [Option('t', "threads", Default = 4, HelpText = "Number of parallel threads.")]
    public int Threads { get; set; }

    [Option('r', "rows", Default = 10000, HelpText = "Total number of rows to be processed.")]
    public int Rows { get; set; }
}
