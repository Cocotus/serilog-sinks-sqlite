using Serilog.Events;
using SQLite;
using System;
using System.Collections.Generic;
using System.Text;

namespace Serilog.Sinks.SQLite
{
    [Table("Logs")]
    public class LogEntry
    {
        [PrimaryKey, AutoIncrement, Column("id")]
        public int Id { get; set; }

        /// <summary>
        /// The time at which the event occurred.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// The level of the event.
        /// </summary>
        [MaxLength(10)]
        public LogEventLevel Level { get; set; }

        /// <summary>
        /// An exception associated with the event, or null.
        /// </summary>
        [MaxLength(2000)]
        public string Exception { get; set; }

        /// <summary>
        /// The message template describing the event.
        /// </summary>
        [MaxLength(1000)]
        public string RenderedMessage { get; set; }

        /// <summary>
        ///  Properties associated with the event, including those presented in Serilog.Events.LogEvent.MessageTemplate.
        /// </summary>
        [MaxLength(500)]
        public string Properties { get; set; }

    }
}
