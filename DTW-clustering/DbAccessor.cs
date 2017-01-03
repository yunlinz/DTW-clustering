using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DTW_clustering
{

    public class DbAccessor
    {
        public SQLiteConnection DbConnection {
            get; private set;
        }
        public DbAccessor(string source="reits.txt", string dbname = "PricesDb.sqlite")
        {
            var dbstring = $"Data Source={dbname};";
            if (!File.Exists(dbname))
            {
                SQLiteConnection.CreateFile(dbname);
                const string sqlCreate = "CREATE TABLE prices (date DATETIME, ticker varchar(10), price FLOAT);\n" +
                                          "CREATE INDEX date_index ON prices (date);\n" +
                                          "CREATE INDEX ticker_index ON prices (ticker);";
                using (var conn = new SQLiteConnection(dbstring))
                {
                    conn.Open();
                    using (var comm = new SQLiteCommand(sqlCreate, conn))
                    {
                        comm.ExecuteNonQuery();
                    }
                }
                using (var sr = new StreamReader(source))
                {
                    using (var conn = new SQLiteConnection(dbstring))
                    {
                        conn.Open();
                        var tickers = new List<string>();
                        while (sr.Peek() >= 0)
                        {
                            var line = sr.ReadLine().Trim();
                            if (line == "") break;
                            tickers.Add(line);
                        }
                        Parallel.ForEach(tickers, t =>
                        {
                            var yahooQuery =
                                $"http://ichart.yahoo.com/table.csv?s={t}&a=0&b=1&c=2016&d=11&e=31&f=2016&g=d&ignore=.csv";
                            using (var wc = new System.Net.WebClient())
                            {
                                var contents = wc.DownloadString(yahooQuery);
                                try
                                {
                                    var prices = contents.Trim().Split('\n')
                                        .Where(l => l != "")
                                        .Skip(1)
                                        .Select(l => l.Split(','))
                                        .Select(l => new
                                        {
                                            Date = DateTime.Parse(l[0]),
                                            Close = l[4]
                                        });

                                    foreach (var p in prices)
                                    {

                                        var insertSql =
                                            $"INSERT INTO prices VALUES (DATETIME(\'{p.Date:yyyy-MM-dd}\'), \'{t}\', {p.Close})";
                                        using (var comm = new SQLiteCommand(insertSql, conn))
                                        {
                                            try
                                            {
                                                comm.ExecuteNonQuery();
                                            }
                                            catch (Exception e)
                                            {
                                                Console.WriteLine($"Insertion failed for {t} on {p.Date:yyyy MMMM dd}");
                                                throw e;
                                            }
                                        }

                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.Write(e.StackTrace);
                                }
                        }
                            Console.WriteLine($"Persisting done for {t}");
                        });
                    }
                }

            }
            DbConnection = new SQLiteConnection(dbname);

        }

        public Tuple<double[], double[]> GetPrices(string ticker1, string ticker2)
        {

            return null;
        }
    }
}