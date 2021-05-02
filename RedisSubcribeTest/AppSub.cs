using System;
using System.Collections.Generic;
using System.Text;

namespace RedisSubcribeTest
{
    class AppSub
    {
        static void Main(string[] args)
        {
            Console.WriteLine("TEST SUBCRIBE FROM REDIS ....\r\n");

            var redis = new RedisOnlySubcribe("localhost", 1001);
            if (!redis.SelectDb(1))
                throw new Exception("CANNOT CONNECT TO REDIS...");

            redis.Subcribe();

            Console.WriteLine("DONE");
            Console.ReadLine();
        }
    }
}
