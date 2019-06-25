using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;

namespace ExecutionPlanReuse
{
    class Program
    {

        static void Main(string[] args)
        {

            //Define a list of countries to search for Data in AdventureWorks
            string[] valuesToSearchFor = { "Australia", "Germany", "France", "United Kingdom", "United States" };

            if (args.Length > 0)
            {
                //Clear the Proc cache and see how it works with parameterised SQL
                ClearProcCache();
                TestExecutionPlanReuse(valuesToSearchFor);
                ViewExecutionPlanCache();
            }
            else
            {
                //Clear the proc cache and see how it works with dynamic SQL
                ClearProcCache();
                TestExecutionPlanCaching(valuesToSearchFor);
                ViewExecutionPlanCache();
            }
            Console.WriteLine("Press Enter to exit.");
            Console.ReadLine();
        }

        private static void TestExecutionPlanCaching(string[] valuesToSearchFor)
        {

            List<long> resultTimes = new List<long>();

            for (int i = 0; i < valuesToSearchFor.Length; i++)
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                string result = GetOrderValueByCountry(valuesToSearchFor[i], false);
                timer.Stop();
                resultTimes.Add(timer.ElapsedMilliseconds);

                Console.WriteLine($"The total order value for {valuesToSearchFor[i]} is {result}" + Environment.NewLine);
            }
            Console.WriteLine($"The result times were: {string.Join(",", resultTimes)}");
        }

        private static void TestExecutionPlanReuse(string[] valuesToSearchFor)
        {

            List<long> resultTimes = new List<long>();

            Console.WriteLine("With Parameters");
            for (int i = 0; i < valuesToSearchFor.Length; i++)
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                string result = GetOrderValueByCountry(valuesToSearchFor[i], true);
                timer.Stop();
                resultTimes.Add(timer.ElapsedMilliseconds);

                Console.WriteLine($" The total order value for {valuesToSearchFor[i]} is  {result}" + Environment.NewLine);
            }
            Console.WriteLine($"The result times were: {string.Join(",", resultTimes)}");
        }

        private static void ViewExecutionPlanCache()
        {
            using (SqlConnection connection = new SqlConnection())
            {

                Console.WriteLine("Querying execution plans:");
                connection.ConnectionString = GetConnectionString();

                connection.Open();

                string query = "SELECT objtype, usecounts, size_in_bytes, text FROM sys.dm_exec_cached_plans outer APPLY sys.dm_exec_sql_text(plan_handle) outer APPLY sys.dm_exec_plan_attributes(plan_handle) where attribute = 'set_options' and text like '%Country%' and text not like '%usecounts%' and text not like '%SCHEMABINDING%'";

                var cmd = new SqlCommand(query, connection);

                var reader = cmd.ExecuteReader();

                Console.WriteLine("Ran the following query:" + query + Environment.NewLine);

                while (reader.Read())
                {
                    Console.WriteLine($"The {reader.GetString(0)} plan was executed {reader.GetInt32(1)} times and has a size of {reader.GetInt32(2)}: ' {reader.GetString(3)} '");
                }
            }
        }

        private static string GetOrderValueByCountry(string valueToSearchFor, bool usesParams)
        {
            using (SqlConnection connection = new SqlConnection())
            {
                connection.ConnectionString = GetConnectionString();

                connection.Open();

                var cmd = new SqlCommand(GetBaseCommand(), connection);

                if (usesParams)
                {

                    cmd.CommandText +=
                        @" WHERE r.Name = @country AND h.OrderDate > '2012-01-01 00:00:00.000' GROUP BY r.Name HAVING sum(h.TotalDue) > 1500000 ";

                    SqlParameter param = new SqlParameter() { ParameterName = "@country", Value = valueToSearchFor, Size = 50 };

                    cmd.Parameters.Add(param);
                }
                else
                {
                    //Purely for testing purposes. We would not be doing this if a user entered this value!!!!
                    cmd.CommandText +=
                        @" WHERE r.Name = '" + valueToSearchFor + "' AND h.OrderDate > '2012-01-01 00:00:00.000' GROUP BY r.Name HAVING sum(h.TotalDue) > 1500000 ";
                }

                var reader = cmd.ExecuteReader();

                if (reader.HasRows)
                {
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    return dt.Rows[0]["Total"].ToString();
                }
                else
                {
                    return "0";
                }
            }
        }

        private static string GetConnectionString()
        {
            return "Data Source=.;Initial Catalog=AdventureWorks2016;Integrated Security=true;";
        }

        private static string GetBaseCommand()
        {
            return "SELECT r.Name [Country], sum(h.TotalDue) [Total] FROM Person.Person p inner join Person.BusinessEntityAddress b on p.BusinessEntityID = b.BusinessEntityID  inner join Person.Address a on b.AddressID = a.AddressID  inner join Person.StateProvince s on a.StateProvinceID = s.StateProvinceID inner join Person.CountryRegion r on r.CountryRegionCode = s.CountryRegionCode inner join Sales.SalesOrderHeader h on h.CustomerID = p.BusinessEntityID ";
        }

        private static void ClearProcCache()
        {
            using (SqlConnection connection = new SqlConnection())
            {
                connection.ConnectionString = GetConnectionString();

                connection.Open();

                var cmd = new SqlCommand("DBCC FREEPROCCACHE DBCC DROPCLEANBUFFERS", connection);

                cmd.ExecuteNonQuery();
            }
        }
    }
}
