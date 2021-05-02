using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

public class RedisOnlyMonitor : RedisBase
{
    long __id = 0;
    Thread ___threadPool = null;
    Queue<long> __queue_ids = new Queue<long>();
    AutoResetEvent __queue_event = new AutoResetEvent(false);

    public RedisOnlyMonitor(
        string host = "localhost",
        int port = 6379,
        string password = "") : base(host, port, password, 5000, 60 * 1000, 8 * 1024)
    {
        if (___threadPool == null)
        {
            ___threadPool = new Thread(__monitorDataFromRedis);
            ___threadPool.IsBackground = true;
            ___threadPool.Start();
        }
    }

    void __monitorDataFromRedis()
    {
        __queue_event.WaitOne();

        var reader = new StreamReader(m_stream);

        string s = string.Empty;
        var bs = new List<int>();
        int b = 0;
        while (true)
        {
            if (!reader.EndOfStream)
                b = m_stream.ReadByte();

            if (b == 0)
            {
                Console.WriteLine("\t\t=> {0}", bs.Count);
                bs.Clear();

                Thread.Sleep(1000);
                continue;
            }
            bs.Add(b);
        }

    }

    public void Subcribe()
    {
        PSUBSCRIBE(__MONITOR_CHANNEL);
        __queue_event.Set();
    }

    bool PSUBSCRIBE(string channel)
    {
        if (string.IsNullOrEmpty(channel)) return false;

        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*2\r\n");
            sb.Append("$10\r\nPSUBSCRIBE\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", channel.Length, channel);

            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            var ok = SendBuffer(buf);
            var lines = ReadMultiString();
            Console.WriteLine("\r\n\r\n{0}\r\n\r\n", string.Join(Environment.NewLine, lines));
            return ok;
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    ~RedisOnlyMonitor()
    {
        try
        {
            if (___threadPool != null)
                ___threadPool.Abort();
        }
        catch { }

        __queue_event.Close();
        __queue_ids.Clear();

        Dispose();
    }
}
