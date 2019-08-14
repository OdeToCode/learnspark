using System;
using Microsoft.Spark.Sql;

namespace HelloSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("***********************Start!");
            var spark = SparkSession.Builder().GetOrCreate();
            var dataFrames = spark.Read().Json("people.json");
            dataFrames.Show();
        }
    }
}
