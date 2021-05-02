using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

public class RedisOnlySubcribe : RedisBase
{
    const string MESSAGE_SPLIT = "}>\r\n$";
    const int TIME_OUT_WAITING_DATA = 100; // miliseconds

    Thread ___threadStream = null;
    Dictionary<string, Action<byte[]>> __channels = new Dictionary<string, Action<byte[]>>();

    Thread ___threadAction = null;
    AutoResetEvent __event = new AutoResetEvent(false);
    Queue<byte[]> __queue = new Queue<byte[]>();

    object __lockMonitor = new object();
    Action<byte[]> __actionMonitor = null;


    public RedisOnlySubcribe(
        string host = "localhost",
        int port = 6379,
        string password = "") : base(REDIS_TYPE.ONLY_SUBCRIBE, host, port, password, 5000, 60 * 1000, 8 * 1024)
    {
        ___threadStream = new Thread(__receiveDataChannel);
        ___threadStream.IsBackground = true;
        ___threadStream.Start();

        ___threadAction = new Thread(__actionDataChannel);
        ___threadAction.IsBackground = true;
        ___threadAction.Start();
    }

    void __actionDataChannel()
    {
        string[] a;
        byte[] buf;
        string s;
        int len;
        int pos;
        while (true)
        {
            if (__queue.Count == 0)
                __event.WaitOne();

            len = 0;
            pos = 0;

            lock (__queue) buf = __queue.Dequeue();
            len = buf.Length;
            if (buf.Length > 1000) len = 1000;

            s = Encoding.ASCII.GetString(buf, 0, len);
            a = s.Split(new string[] { MESSAGE_SPLIT }, StringSplitOptions.None);
            if (a.Length > 2)
            {
                for (int i = 0; i < a.Length - 1; i++) pos += a[i].Length + MESSAGE_SPLIT.Length;
                pos += a[a.Length - 1].Split('\r')[0].Length + 2;

                if (pos <= buf.Length - 2)
                {
                    len = buf.Length - pos - 2;
                    byte[] bs = new byte[len];
                    for (int i = pos; i < buf.Length - 2; i++) bs[i - pos] = buf[i];

                    if (__actionMonitor != null)
                    {
                        var t = new Thread(new ParameterizedThreadStart((o) =>
                        {
                            lock (__lockMonitor)
                                __actionMonitor((byte[])o);
                        }));
                        t.IsBackground = true;
                        t.Start(bs);
                    }
                }
            }
        }
    }

    void __receiveDataChannel()
    {
        string s = string.Empty;
        var bs = new List<byte>();
        byte b = 0;
        while (true)
        {
            if (!m_stream.DataAvailable)
            {
                if (bs.Count > 0)
                {
                    lock (__queue) __queue.Enqueue(bs.ToArray());
                    bs.Clear();
                    lock (__event) __event.Set();
                }

                Thread.Sleep(TIME_OUT_WAITING_DATA);
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

        bool ok = PSUBSCRIBE(channel, action);
        if (ok)
        {
            lock (__channels)
            {
                if (__channels.ContainsKey(channel))
                    __channels[channel] = action;
                else
                    __channels.Add(channel, action);
            }
        }
        return ok;
    }

    bool PSUBSCRIBE(string channel, Action<byte[]> action)
    {
        if (string.IsNullOrEmpty(channel)) return false;
        try
        {
            if (channel == __MONITOR_CHANNEL)
                lock (__lockMonitor) 
                    __actionMonitor = action;
            else
                channel = "<{" + channel + "}>";

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
            if (___threadStream != null) ___threadStream.Abort();
            if (___threadAction != null) ___threadAction.Abort();
        }
        catch { }

        __event.Close();

        Dispose();
    }
}
