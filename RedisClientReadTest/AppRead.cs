using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace RedisClientReadTest
{
    class AppRead
    {
        static void Main(string[] args)
        {
            Console.WriteLine("TEST READ FROM REDIS ....\r\n");
            //Thread.Sleep(1000);

            var redis = new RedisOnlyRead("localhost", 1001);
            redis.Connect();
            redis.SelectDb(15);

            string key1 = redis.GET("key-1");
            Console.WriteLine("key-1 = {0}", key1);

            //string f1 = redis.HGET("test", "f1");
            //Console.WriteLine("test > f1 = {0}", f1);

            //var keys = redis.HKEYS("test");
            //Console.WriteLine("test = {0}", string.Join(",", keys));


            //var stream = redis.GET_STREAM("image-1");
            //if (stream != null)
            //{
            //    Console.WriteLine("image-1 = {0}", stream.Length);
            //    //new Bitmap(stream).Save(@"c:\1.png", ImageFormat.Png);
            //}



            string key2 = redis.GET("key-2");
            Console.WriteLine("key-2 = {0}", key2);


            Console.WriteLine("DONE");
            Console.ReadLine();
        }
    }
}
