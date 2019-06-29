// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;
using SQLite;

namespace Serilog.Sinks.SQLite
{
    internal class SQLiteSink : BatchProvider, ILogEventSink
    {
        private readonly string _databasePath;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly uint _maxDatabaseSize;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly TimeSpan? _retentionPeriod;
        private readonly Timer _retentionTimer;
        private const long BytesPerMb = 1_048_576;
        private const long MaxSupportedPages = 5_242_880;
        private const long MaxSupportedPageSize = 4096;
        private const long MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        public SQLiteSink(
            string sqlLiteDbPath,
            string tableName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            TimeSpan? retentionPeriod,
            TimeSpan? retentionCheckInterval,
            uint batchSize = 100,
            uint maxDatabaseSize = 10,
            bool rollOver = true) : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _databasePath = sqlLiteDbPath;
            _tableName = tableName;
            _formatProvider = formatProvider;
            _storeTimestampInUtc = storeTimestampInUtc;
            _maxDatabaseSize = maxDatabaseSize;
            _rollOver = rollOver;

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                SelfLog.WriteLine($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
                throw new ArgumentException($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            InitializeDatabase();
           
            if (retentionPeriod.HasValue)
            {
                // impose a min retention period of 15 minute
                var retentionCheckMinutes = 15;
                if (retentionCheckInterval.HasValue)
                {
                    retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
                }

                // impose multiple of 15 minute interval
                retentionCheckMinutes = (retentionCheckMinutes / 15) * 15;

                _retentionPeriod = new[] { retentionPeriod, TimeSpan.FromMinutes(30) }.Max();

                // check for retention at this interval - or use retentionPeriod if not specified
                _retentionTimer = new Timer(
                    (x) => { ApplyRetentionPolicy(); },
                    null,
                    TimeSpan.FromMinutes(0),
                    TimeSpan.FromMinutes(retentionCheckMinutes));
            }
        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private async void InitializeDatabase()
        {
            SQLiteAsyncConnection conn = GetSqLiteConnection();
            await CreateSqlTable(conn);
            await conn.CloseAsync();
        }

        private SQLiteAsyncConnection GetSqLiteConnection()
        {
            var sqlConnection = new SQLiteAsyncConnection(_databasePath);
            return sqlConnection;
        }

        private async Task CreateSqlTable(SQLiteAsyncConnection sqlConnection)
        {
            await sqlConnection.CreateTableAsync<LogEntry>();
        }

        private async void ApplyRetentionPolicy()
        {
            var epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);
            var sqlConnection = GetSqLiteConnection();
            var query = sqlConnection.Table<LogEntry>().Where(s => s.Timestamp < (_storeTimestampInUtc ? epoch.ToUniversalTime() : epoch));
            SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
            await query.DeleteAsync();
            await sqlConnection.CloseAsync();
        }

        private async Task<int> TruncateLog(SQLiteAsyncConnection sqlConnection)
        {
          return await sqlConnection.DeleteAllAsync<LogEntry>();
        }


        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                    var sqlConnection = GetSqLiteConnection();
                    try
                    {
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);
                        return true;
                    }
                    catch (SQLiteException e)
                    {
                        SelfLog.WriteLine(e.Message);

                        if (e.Result != SQLite3.Result.Full)
                            return false;

                        if (_rollOver == false)
                        {
                            SelfLog.WriteLine("Discarding log excessive of max database");

                            return true;
                        }

                        var dbExtension = Path.GetExtension(_databasePath);

                        var newFilePath = Path.Combine(Path.GetDirectoryName(_databasePath) ?? "Logs",
                            $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_hhmmss.ff}{dbExtension}");
                         
                        File.Copy(_databasePath, newFilePath, true);

                        await TruncateLog(sqlConnection);
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        SelfLog.WriteLine($"Rolling database to {newFilePath}");
                        return true;
                    }
                    catch (Exception e)
                    {
                        SelfLog.WriteLine(e.Message);
                        return false;
                    }
                finally
                {
                    await sqlConnection.CloseAsync();
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(ICollection<LogEvent> logEventsBatch, SQLiteAsyncConnection sqlConnection)
        {
            List<LogEntry> loglist = new List<LogEntry>();
            foreach (var logEvent in logEventsBatch)
            {
                LogEntry newLogentry = new LogEntry();
                newLogentry.Timestamp = _storeTimestampInUtc ? logEvent.Timestamp.UtcDateTime: logEvent.Timestamp.LocalDateTime;
                newLogentry.Exception = logEvent.Exception?.ToString() ?? string.Empty;
                newLogentry.Level = logEvent.Level;
                newLogentry.RenderedMessage = logEvent.MessageTemplate.Text;
                newLogentry.Properties = logEvent.Properties.Count > 0 ? logEvent.Properties.Json() : string.Empty;
                loglist.Add(newLogentry);
            }
          await sqlConnection.InsertAllAsync(loglist);
        }
    }
}
