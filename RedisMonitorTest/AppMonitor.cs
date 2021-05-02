using System;
using System.Collections.Generic;
using System.Text;

namespace RedisMonitorTest
{
    class AppMonitor
    {
        static void Main(string[] args)
        {
            Console.WriteLine("TEST MONITOR FROM REDIS ....\r\n"); 

            var redis = new RedisOnlyMonitor("localhost", 1001);
            if (!redis.SelectDb(1))
                throw new Exception("CANNOT CONNECT TO REDIS...");

            redis.Subcribe();

            Console.WriteLine("DONE");
            Console.ReadLine();
        }
    }
}
