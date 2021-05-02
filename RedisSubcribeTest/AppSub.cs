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

            var r1 = new RedisOnlySubcribe("localhost", 1001);
            if (!r1.SelectDb(1))
                throw new Exception("CANNOT CONNECT TO REDIS...");

            r1.Subcribe(r1.__MONITOR_CHANNEL, (obj) =>
            {
                string s = Encoding.UTF8.GetString(obj.Buffer);
                Console.WriteLine("----> [MONITOR] {0}: {1}", obj.Channel, s);
            });

            r1.Subcribe("C1", (obj) =>
            {
                string s = Encoding.UTF8.GetString(obj.Buffer);
                Console.WriteLine("----> {0}: {1}", obj.Channel, s);
            });

            //var r2 = new RedisOnlySubcribe("localhost", 1002);
            //if (!r2.SelectDb(1))
            //    throw new Exception("CANNOT CONNECT TO REDIS...");
            //r2.Subcribe();

            Console.WriteLine("LISTENING ........");
            Console.ReadLine();
        }
    }
}
