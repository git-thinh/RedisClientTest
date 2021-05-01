using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Text;

namespace RedisClientTest
{
    class App
    {
        static void Main(string[] args)
        {
            //test_write();
            test_read();
            Console.WriteLine("DONE");
            Console.ReadLine();
        }

        static void test_read()
        {
            var redis = new RedisOnlyRead();
            redis.Db = 15;

            string key1 = redis.GET("key-1");
            Console.WriteLine("key-1 = {0}", key1);

            var stream = redis.GET_STREAM("image-1");
            if (stream != null)
            {
                Console.WriteLine("image-1 = {0}", stream.Length);
                //new Bitmap(stream).Save(@"c:\1.png", ImageFormat.Png);
            }

            string f1 = redis.HGET("test", "f1");
            Console.WriteLine("test > f1 = {0}", f1);

            var keys = redis.HKEYS("test");
            Console.WriteLine("test = {0}", string.Join(",", keys));


        }

        static void test_write()
        {
            var redis = new RedisOnlyWrite();
            redis.Db = 15;

            bool ok1 = false, ok2 = false, ok3 = false, ok4 = false;

            ok1 = redis.HMSET("test", new Dictionary<string, string>()
            {
                {"f1", Guid.NewGuid().ToString() },
                {"f2", Guid.NewGuid().ToString() },
            });

            ok2 = redis.SET("key-1", Guid.NewGuid().ToString());
            ok3 = redis.SET("image-1", File.ReadAllBytes(@"C:\Users\nvt3\Pictures\logo.png"));

            ok4 = redis.BGSAVE();

            Console.WriteLine("{0}> {1} - {2} - {3} - {4}", "", ok1, ok2, ok3, ok4);
        }
    }
}
