using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace RedisClientWriteTest
{
    class AppWrite
    {
        static void Main(string[] args)
        {
            Console.WriteLine("TEST WRITE TO REDIS ....\r\n");

            var redis = new RedisOnlyWrite("localhost", 1000);
            if (!redis.SelectDb(1))
                throw new Exception("CANNOT CONNECT TO REDIS...");

            Thread.Sleep(300);
            redis.PUBLISH(redis.__MONITOR_CHANNEL, "12345");

            string cmd = Console.ReadLine();
            while (cmd != "exit")
            {
                if (cmd.StartsWith("c1")) redis.PUBLISH("C1", cmd);
                else redis.PUBLISH(redis.__MONITOR_CHANNEL, cmd);
                cmd = Console.ReadLine();
            }


            bool ok1 = false, ok2 = false, ok3 = false, ok4 = false, ok5 = false;

            ok2 = redis.SET("key-1", Guid.NewGuid().ToString());
            ok2 = redis.SET("key-2", Guid.NewGuid().ToString());

            ok3 = redis.SET("image-1", File.ReadAllBytes(@"C:\Users\nvt3\Pictures\logo.png"));
            ok1 = redis.HMSET("test", new Dictionary<string, string>()
            {
                {"f1", Guid.NewGuid().ToString() },
                {"f2", Guid.NewGuid().ToString() },
            });
            ok4 = redis.BGSAVE();

            ok5 = redis.PUBLISH("PSI__PDF_IMAGE_BY_FILE", "123");

            Console.WriteLine("{0}> {1} - {2} - {3} - {4} - {5}", "", ok1, ok2, ok3, ok4, ok5);

            redis.PUBLISH("MESSAGE_WRITTEN", "");

            Console.WriteLine("DONE");
            Console.ReadLine();
        }

    }
}
