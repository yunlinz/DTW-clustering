using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DTW_clustering
{

    public class DbAccessor
    {
        public SQLiteConnection PriceDb { get; }
        public string TickersFile { get; private set; }

        public DbAccessor(string source="reits.txt", string dbname = "PricesDb.sqlite")
        {
            TickersFile = source;
            var dbstring = $"Data Source={dbname};";
            if (!File.Exists(dbname))
            {
                SQLiteConnection.CreateFile(dbname);
                const string sqlCreate = "CREATE TABLE prices (date DATETIME, ticker varchar(10), price FLOAT);\n" +
                                          "CREATE INDEX date_index ON prices (date);\n" +
                                          "CREATE INDEX ticker_index ON prices (ticker);\n" +
                                          "CREATE TABLE dtw (ticker1 VARCHAR(10), ticker2 VARCHAR(10), dtw FLOAT);";

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
                            var line = sr.ReadLine()?.Trim();
                            if (line == "") break;
                            tickers.Add(line);
                        }
                        Parallel.ForEach(tickers, t =>
                        {
                            var yahooQuery =
                                $"http://ichart.yahoo.com/table.csv?s={t}"+
                                "&a=0&b=1&c=2016&d=11&e=31&f=2016&g=d&ignore=.csv";
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
                                            "INSERT INTO prices VALUES "+
                                            $"(DATETIME(\'{p.Date:yyyy-MM-dd}\'), \'{t}\', {p.Close})";
                                        // ReSharper disable once AccessToDisposedClosure
                                        using (var comm = new SQLiteCommand(insertSql, conn))
                                        {
                                            try
                                            {
                                                comm.ExecuteNonQuery();
                                            }
                                            catch (Exception)
                                            {
                                                Console.WriteLine($"Insertion failed for {t} on {p.Date:yyyy MMMM dd}");
                                                throw;
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
            PriceDb = new SQLiteConnection(dbstring);
            PriceDb.Open();
        }

        public double[] GetPrices(string ticker)
        {
            var sql = $"SELECT * FROM prices WHERE ticker = '{ticker}' ORDER BY date";
            var priceList = new List<PriceObject>();
            using (var comm = new SQLiteCommand(sql, PriceDb))
            {
                using (var rd = comm.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        priceList.Add(new PriceObject()
                        {
                            Date = rd.GetDateTime(0),
                            Ticker = rd.GetString(1),
                            Price = rd.GetDouble(2)
                        });
                    }
                }
            }
            return priceList.Select(p => p.Price).ToArray();
        }

        public void InsertDtw(string ticker1, string ticker2, double value)
        {
            var sql = $"INSERT INTO dtw VALUES ('{ticker1}','{ticker2}', {value})";
            using (var comm = new SQLiteCommand(sql, PriceDb))
            {
                comm.ExecuteNonQuery();
            }
        }
    }

    public class PriceObject
    {
        public DateTime Date { get; set; }
        public string Ticker { get; set; }
        public double Price { get; set; }
    }
}