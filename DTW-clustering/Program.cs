using System;
using System.Collections.Generic;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace DTW_clustering
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var db = new DbAccessor();
            var tickers = new List<string>();
            using (var sr = new StreamReader(db.TickersFile))
            {
                while (sr.Peek() >= 0)
                {
                    var line = sr.ReadLine().Trim();
                    if (line == "") break;
                    tickers.Add(line);
                }
            }

            Parallel.ForEach(tickers, t1 =>
                {
                    var ts1 = PriceToReturns(db.GetPrices(t1));
                    Parallel.ForEach(tickers, t2 =>
                    {
                        if (t1 == t2) return;
                        var ts2 = PriceToReturns(db.GetPrices(t2));
                        var dtw = DTW.CalculateDTW(ts1, ts2);
                        Console.WriteLine($"DTW for {t1} and {t2} is {dtw}");
                        db.InsertDtw(t1, t2, dtw);
                    });
                }
            );
        }

        private static double[] PriceToReturns(double[] price)
        {
            var l = price.Length;
            var returns = new double[l];
            for (var i = 1; i < l; i++)
            {
                returns[i - 1] = price[i] / price[i - 1] - 1;
            }
            return returns;
        }
    }


}
