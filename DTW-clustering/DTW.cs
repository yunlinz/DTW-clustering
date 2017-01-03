using System;
using System.Linq;
using System.Security.Cryptography;
using NUnit.Framework;

namespace DTW_clustering
{
    public class DTW
    {
        public static double CalculateDTW(double[] ts1, double[] ts2)
        {
            var l1 = ts1.Length;
            var l2 = ts2.Length;

            var memo = new double[l1 + 1, l2 + 1];
            for (var i = 0; i <= l1; i++) memo[i, 0] = double.PositiveInfinity;
            for (var i = 0; i <= l2; i++) memo[0, i] = double.PositiveInfinity;
            memo[0, 0] = 0;

            for (var i = 1; i <= l1; i++)
            {
                for (var j = 1; j <= l2; j++)
                {
                    var dist = Math.Abs(ts1[i - 1] - ts2[j - 1]);
                    memo[i, j] = dist + Math.Min(Math.Min(memo[i - 1, j], memo[i, j - 1]), memo[i - 1, j - 1]);
                }
            }
            return memo[l1, l2] / Math.Sqrt(Math.Pow(l1, 2) + Math.Pow(l2, 2));
        }

        [Test]
        public void TimingTest()
        {
            var rng = new Random();
            for (var i = 1; i <= 10000; i *= 10)
            {
                for (var j = i; j <= 10000; j *= 10)
                {
                    var ts1 = Enumerable.Repeat(0, i).Select(k =>
                    {
                        var u1 = rng.NextDouble();
                        var u2 = rng.NextDouble();
                        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Sin(2.0 * Math.PI * u2);
                    }).ToArray();
                    var ts2 = Enumerable.Repeat(0, j).Select(k =>
                    {
                        var u1 = rng.NextDouble();
                        var u2 = rng.NextDouble();
                        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Sin(2.0 * Math.PI * u2);
                    }).ToArray();
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    var ans = CalculateDTW(ts1, ts2);
                    Console.WriteLine($"i = {i}, j = {j}, T = {watch.ElapsedMilliseconds}ms, a = {ans}");
                    watch.Reset();
                }
            }
        }
    }
}