using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

public class oRedisNotify
{
    public IDictionary<string, object> Paramenters { get; }
    public string Channel { get; }
    public byte[] Buffer { get; }
    public oRedisNotify(string channel, byte[] buf, IDictionary<string, object> paramenters = null)
    {
        this.Channel = channel;
        this.Buffer = buf;
        this.Paramenters = paramenters;
    }
}

public class RedisOnlySubcribe : RedisBase
{
    const string MESSAGE_SPLIT_END = "}>\r\n$";
    const string MESSAGE_SPLIT_BEGIN = "\r\n<{";
    const int TIME_OUT_WAITING_DATA = 100; // miliseconds
    const int BUFFER_HEADER_MAX_SIZE = 1000;

    Thread ___threadStream = null;
    Dictionary<string, Action<oRedisNotify>> __channels = new Dictionary<string, Action<oRedisNotify>>();
    Dictionary<string, IDictionary<string, object>> __paramenters = new Dictionary<string, IDictionary<string, object>>();

    Thread ___threadAction = null;
    AutoResetEvent __event = new AutoResetEvent(false);
    Queue<byte[]> __queue = new Queue<byte[]>();

    object __lockMonitor = new object();
    Action<oRedisNotify> __actionMonitor = null;

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
            if (buf.Length > BUFFER_HEADER_MAX_SIZE) len = BUFFER_HEADER_MAX_SIZE;

            s = Encoding.ASCII.GetString(buf, 0, len);
            a = s.Split(new string[] { MESSAGE_SPLIT_END }, StringSplitOptions.None);
            if (a.Length > 2)
            {
                for (int i = 0; i < a.Length - 1; i++) pos += a[i].Length + MESSAGE_SPLIT_END.Length;
                pos += a[a.Length - 1].Split('\r')[0].Length + 2;

                if (pos <= buf.Length - 2)
                {
                    len = buf.Length - pos - 2;
                    byte[] bs = new byte[len];
                    for (int i = pos; i < buf.Length - 2; i++) bs[i - pos] = buf[i];

                    a = a[a.Length - 2].Split(new string[] { MESSAGE_SPLIT_BEGIN }, StringSplitOptions.None);
                    string channel = a[a.Length - 1];

                    oRedisNotify noti;
                    Action<oRedisNotify> call = null;
                    IDictionary<string, object> para = null;

                    if (!string.IsNullOrEmpty(channel))
                    {
                        string channelKey = "<{" + channel.ToUpper() + "}>";
                        if (channelKey != __MONITOR_CHANNEL)
                        {
                            lock (__channels) if (__channels.ContainsKey(channelKey)) __channels.TryGetValue(channelKey, out call);
                            lock (__paramenters) if (__paramenters.ContainsKey(channelKey)) __paramenters.TryGetValue(channelKey, out para);
                        }

                        noti = new oRedisNotify(channel, bs, para);

                        if (call != null)
                        {
                            var ta = new Thread(new ParameterizedThreadStart((o) =>
                            {
                                lock (call)
                                    call((oRedisNotify)o);
                            }));
                            ta.IsBackground = true;
                            ta.Start(noti);
                        }

                        if (__actionMonitor != null)
                        {
                            var tm = new Thread(new ParameterizedThreadStart((o) =>
                            {
                                lock (__lockMonitor)
                                    __actionMonitor((oRedisNotify)o);
                            }));
                            tm.IsBackground = true;
                            tm.Start(noti);
                        }
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

    public bool Subcribe(string channel, Action<oRedisNotify> action, IDictionary<string, object> paramenters = null)
    {
        if (action == null || string.IsNullOrEmpty(channel)) return false;

        if (channel != __MONITOR_CHANNEL) channel = "<{" + channel + "}>";
        channel = channel.ToUpper();

        bool ok = PSUBSCRIBE(channel);
        if (ok)
        {
            if (channel == __MONITOR_CHANNEL)
                lock (__lockMonitor)
                    __actionMonitor = action;
            else
            {
                lock (__channels)
                {
                    if (__channels.ContainsKey(channel))
                        __channels[channel] = action;
                    else
                        __channels.Add(channel, action);
                }

                if (paramenters != null && paramenters.Count > 0)
                {
                    lock (__paramenters)
                    {
                        if (__paramenters.ContainsKey(channel))
                            __paramenters[channel] = paramenters;
                        else
                            __paramenters.Add(channel, paramenters);
                    }
                }
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
            //Console.WriteLine("\r\n\r\n{0}\r\n\r\n", string.Join(Environment.NewLine, lines));
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
        __paramenters.Clear();
        __channels.Clear();

        Dispose();
    }
}
