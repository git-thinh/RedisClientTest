using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

public class RedisOnlySubcribe : RedisBase
{
    Thread ___thread = null;
    AutoResetEvent __event = new AutoResetEvent(false);
    Dictionary<string, Action<byte[]>> __channels = new Dictionary<string, Action<byte[]>>();

    public RedisOnlySubcribe(
        string host = "localhost",
        int port = 6379,
        string password = "") : base(REDIS_TYPE.ONLY_SUBCRIBE, host, port, password, 5000, 60 * 1000, 8 * 1024)
    {
        if (___thread == null)
        {
            ___thread = new Thread(__monitorDataFromRedis);
            ___thread.IsBackground = true;
            ___thread.Start();
        }
    }

    void __monitorDataFromRedis()
    {
        __event.WaitOne();

        string s = string.Empty;
        var bs = new List<byte>();
        byte b = 0;
        while (true)
        {
            if (!m_stream.DataAvailable)
            {
                if (bs.Count > 0)
                {
                    s = Encoding.UTF8.GetString(bs.ToArray());
                    Console.WriteLine("\t\t=> {0}", s);
                    bs.Clear();
                }
                Thread.Sleep(1000);
                continue;
            }

            b = (byte)m_stream.ReadByte();
            bs.Add(b);
        }
    }

    public bool Subcribe(string channel, Action<byte[]> action)
    {
        if (action == null || string.IsNullOrEmpty(channel))
            return false;

        bool ok = PSUBSCRIBE(channel);
        if (ok)
        {
            lock (__channels)
            {
                __channels.Add(channel, action);
                if (__channels.Count == 1) __event.Set();
            }
        }
        return ok;
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

    ~RedisOnlySubcribe()
    {
        try
        {
            if (___thread != null)
                ___thread.Abort();
        }
        catch { }

        __event.Close();

        Dispose();
    }
}
