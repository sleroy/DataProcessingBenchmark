# DataProcessingBenchmark

DataProcessingBenchmark is a .NET console application that compares the processing speed between Redis and SQL Server. The application creates a table, fills it with a specified number of rows, processes them, and measures the performance in terms of rows processed per second and elapsed time.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Options](#options)
- [Examples](#examples)
- [License](#license)

## Prerequisites

- .NET SDK 6.0 or later
- Redis server (if testing with Redis)
- SQL Server (if testing with SQL Server)

## Installation

1. Clone the repository or download the source code.
2. Navigate to the project directory.

```bash
cd DataProcessingBenchmark
```

### Add the necessary packages.

```bash
dotnet add package StackExchange.Redis
dotnet add package Microsoft.Data.SqlClient
dotnet add package CommandLineParser
```

### Build the project

```bash
dotnet build
```

## Usage

Run the application with the appropriate command-line arguments to test the performance of Redis or SQL Server.

```bash
dotnet run -- [options]
```

### Options

* -d, --dataSource (required): Specify the data source: Redis or SqlServer.
* -c, --connection (required): Connection string for the data source.
* -b, --batchSize (default: 1000): Number of rows written at once.
* -t, --threads (default: 4): Number of parallel threads.
* -r, --rows (default: 10000): Total number of rows to be processed.

## Examples

### SQL Server

```bash
dotnet run -- -d SqlServer -c "Server=your_server;Database=your_db;User Id=your_user;Password=your_password;" -b 1000 -t 4 -r 10000
```

Replace your_server, your_db, your_user, and your_password with the appropriate values for your SQL Server instance.

### Redis

```bash
dotnet run -- -d Redis -c "localhost" -b 1000 -t 4 -r 10000
```

### License

This project is licensed under the MIT License. See the LICENSE file for details.


Feel free to customize the README further based on your specific needs and preferences.





