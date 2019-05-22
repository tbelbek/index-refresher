using System;
using System.Collections.Generic;
using System.Linq;
using QC = System.Data.SqlClient;  // System.Data.dll 
using System.Configuration;

namespace IndexRefresher
{
    class Program
    {
        static void Main(string[] args)
        {
            string connString = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;
            using (var connection = new QC.SqlConnection(connString))
            {
                QC.SqlCommand command = new QC.SqlCommand(
                    @"SELECT dbschemas.[name] as 'Schema',
                            dbtables.[name] as 'Table',
                            dbindexes.[name] as 'Index',
                            indexstats.avg_fragmentation_in_percent as 'FragmentationPercent',
                            indexstats.page_count as 'PageCount'
                                FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
                            INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]
                            INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]
                            INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
                            AND indexstats.index_id = dbindexes.index_id
                            WHERE indexstats.database_id = DB_ID()
                            ORDER BY indexstats.avg_fragmentation_in_percent desc",
                    connection)
                { CommandTimeout = 0 };
                connection.Open();

                QC.SqlDataReader reader = command.ExecuteReader();
                var list = new List<FragmentedIndexes>();

                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var t = new FragmentedIndexes
                        {
                            FragmentationPercent = reader["FragmentationPercent"].ToString(),
                            PageCount = reader["PageCount"].ToString(),
                            Schema = reader["Schema"].ToString(),
                            Index = reader["Index"].ToString(),
                            Table = reader["Table"].ToString()
                        };

                        if (!(Convert.ToDouble(t.FragmentationPercent) > 15)) continue;

                        list.Add(t);
                        Console.WriteLine($"{t.FragmentationPercent} - {t.Index} - {t.Table}");

                    }
                }
                else
                {
                    Console.WriteLine("No rows found.");
                }

                reader.Close();

                var cmdList = list.Where(t => string.IsNullOrEmpty(t.Index)).Select(item => $"ALTER INDEX ALL ON {item.Table} REBUILD").ToList();

                cmdList.AddRange(list.Where(t => !string.IsNullOrEmpty(t.Index)).Select(item => $"ALTER INDEX {item.Index} ON {item.Table} REBUILD"));


                var cmdStringForIndexUpdate = string.Join(Environment.NewLine, cmdList);

                QC.SqlCommand rebuildCommand =
                    new QC.SqlCommand(cmdStringForIndexUpdate, connection) { CommandTimeout = 0 };

                rebuildCommand.ExecuteNonQuery();

                Console.WriteLine(list.Count.ToString());

                Console.WriteLine("Press any key to finish...");
                Console.ReadKey(true);
            }
        }
    }

    public class FragmentedIndexes
    {
        public string Schema { get; set; }
        public string Table { get; set; }
        public string Index { get; set; }
        public string FragmentationPercent { get; set; }
        public string PageCount { get; set; }
    }
}
