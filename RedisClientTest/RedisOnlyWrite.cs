using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

public class RedisOnlyWrite : IDisposable
{
    Socket socket;
    BufferedStream bstream;
    const int BUFFER_SIZE_READ = 255; // 255b || 16kb || 64kb
    static readonly byte[] _END_DATA = new byte[] { 13, 10 }; //= \r\n

    long __id = 0;
    Dictionary<long, string> __notifier = new Dictionary<long, string>();
    Dictionary<long, AutoResetEvent> __signals = new Dictionary<long, AutoResetEvent>();
    Dictionary<long, byte[]> __buffers = new Dictionary<long, byte[]>();
    Dictionary<long, byte> __results = new Dictionary<long, byte>();
    AutoResetEvent __queue_event = new AutoResetEvent(false);
    Queue<long> __queue_ids = new Queue<long>();
    Thread ___threadPool = null;

    public string Host { get; private set; }
    public int Port { get; private set; }
    public int SendTimeout { get; set; }
    public int DatabaseNumber { get; set; }
    public string Password { get; set; }

    public RedisOnlyWrite(string host = "localhost", int port = 6379,
        string password = "", int sendTimeout = 60 * 1000)
    {
        this.Host = host;
        this.Port = port;
        this.SendTimeout = sendTimeout;
        this.Password = password;

    }

    public void Connect()
    {
        socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.NoDelay = true;
        socket.SendTimeout = SendTimeout;
        socket.SendBufferSize = int.MaxValue;
        socket.Connect(Host, Port);
        if (!socket.Connected)
        {
            socket.Close();
            socket = null;
            return;
        }
        bstream = new BufferedStream(new NetworkStream(socket), BUFFER_SIZE_READ);
        //if (Password != null)
        //    SendExpectSuccess("AUTH", Password);

        if (___threadPool == null)
        {
            ___threadPool = new Thread(__writeDataToRedis);
            ___threadPool.IsBackground = true;
            ___threadPool.Start();
        }
    }

    void __writeDataToRedis()
    {
        while (true)
        {
            if (__queue_ids.Count == 0)
                __queue_event.WaitOne();

            long id = 0;
            byte[] buf = null;
            AutoResetEvent sig = null;
            byte ok = 0;
            string noti = string.Empty;

            lock (__queue_ids) id = __queue_ids.Dequeue();
            lock (__buffers) buf = __buffers[id];
            lock (__buffers) sig = __signals[id];

            lock (__notifier)
                if (__notifier.ContainsKey(id))
                    noti = __notifier[id];

            if (socket == null) Connect();
            if (socket != null)
            {
                try
                {
                    socket.Send(buf);

                    // :1 :0 +OK +Background saving started
                    string line = ReadLine();
                    if (!string.IsNullOrEmpty(line) && (line[0] == '+' || line[0] == ':'))
                        ok = 1;

                    if (!string.IsNullOrEmpty(noti))
                    {
                        var arr = noti.Split('^');
                        if (arr.Length > 1)
                        {
                            byte[] notiBuf = null;
                            string msg = noti.Substring(arr[0].Length + 1, noti.Length - 1 - arr[0].Length) +
                                DateTime.Now.ToString("^yyyyMMddHHmmss");
                            if (arr[0].Length > 0)
                            {
                                notiBuf = __notifyBodyCreate(arr[0], msg);
                                socket.Send(notiBuf);
                            }
                            notiBuf = __notifyBodyCreate("__LOG_ALL", msg);
                            socket.Send(notiBuf);
                        }
                    }
                }
                catch (SocketException ex)
                {
                    socket.Close();
                    socket = null;
                }
            }

            lock (__results) __results.Add(id, ok);
            sig.Set();
        }
    }

    int db;
    public int Db
    {
        get
        {
            return db;
        }

        set
        {
            bool ok = selectDb(value);
            if (ok)
                db = value;
        }
    }

    bool selectDb(int indexDb)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*2\r\n");
            sb.Append("$6\r\nSELECT\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", indexDb.ToString().Length, indexDb);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            return Send(buf, REDIS_CMD.SELECT, "", "");
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    bool Send(byte[] buf, REDIS_CMD cmd, string key, string notify)
    {
        long id = Interlocked.Increment(ref __id);
        var sig = new AutoResetEvent(false);

        string noti = string.Format("{0}^{1}^{2}", notify, cmd, key);
        lock (__notifier) __notifier.Add(id, noti);

        lock (__buffers) __buffers.Add(id, buf);
        lock (__signals) __signals.Add(id, sig);
        lock (__queue_ids) __queue_ids.Enqueue(id);

        __queue_event.Set();
        sig.WaitOne();

        sig.Close();
        lock (__buffers) __buffers.Remove(id);
        lock (__signals) __signals.Remove(id);
        lock (__notifier) if (__notifier.ContainsKey(id)) __notifier.Remove(id);

        bool ok = false;
        lock (__results)
        {
            ok = __results[id] == 1;
            __results.Remove(id);
        }

        ////if (socket == null) Connect();
        ////if (socket == null) return false;
        ////try
        ////{
        ////    socket.Send(buf);
        ////}
        ////catch (SocketException ex)
        ////{
        ////    socket.Close();
        ////    socket = null;
        ////    return false;
        ////}

        return ok;
    }



    public bool BGSAVE(string notify = "")
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*1\r\n");
            sb.Append("$6\r\nBGSAVE\r\n");
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            var ok = Send(buf, REDIS_CMD.BGSAVE, "", notify);
            return ok;
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    static byte[] __notifyBodyCreate(string channel, string value)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("*3\r\n");
        sb.Append("$7\r\nPUBLISH\r\n");
        sb.AppendFormat("${0}\r\n{1}\r\n", channel.Length, channel);
        sb.AppendFormat("${0}\r\n{1}\r\n", value.Length, value);
        byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
        return buf;
    }

    public bool PUBLISH(string channel, string value)
    {
        try
        {
            byte[] buf = __notifyBodyCreate(channel, value);
            return Send(buf, REDIS_CMD.PUBLISH, "", "");
        }
        catch (Exception ex)
        {
        }
        return false;
    }



    public bool SET(string key, string value, string notify = "")
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*3\r\n");
            sb.Append("$3\r\nSET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);
            sb.AppendFormat("${0}\r\n{1}\r\n", value.Length, value);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            return Send(buf, REDIS_CMD.SET, key, notify);
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    public bool SET(string key, byte[] value, string notify = "")
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*3\r\n");
            sb.Append("$3\r\nSET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);

            sb.AppendFormat("${0}\r\n", value.Length);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());

            var arr = __combine(buf.Length + value.Length + 2, buf, value, _END_DATA);
            bool ok = Send(arr, REDIS_CMD.SET, key, notify);
            return ok;
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    public bool HSET(string key, string field, byte[] value, string notify = "")
        => HMSET(key, new Dictionary<string, byte[]>() { { field, value } }, notify);

    public bool HSET(string key, string field, string value, string notify = "")
        => HMSET(key, new Dictionary<string, string>() { { field, value } }, notify);

    public bool HMSET(string key, IDictionary<string, string> fields, string notify = "")
    {
        var dic = new Dictionary<string, byte[]>();
        foreach (var kv in fields)
            dic.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));
        return HMSET(key, dic, notify);
    }

    public bool HMSET(string key, IDictionary<string, byte[]> fields, string notify = "")
    {
        if (fields == null || fields.Count == 0) return false;
        try
        {
            StringBuilder bi = new StringBuilder();
            bi.AppendFormat("*{0}\r\n", 2 + fields.Count * 2);
            bi.Append("$5\r\nHMSET\r\n");
            bi.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);

            using (MemoryStream ms = new MemoryStream())
            {
                byte[] buf = Encoding.UTF8.GetBytes(bi.ToString());
                ms.Write(buf, 0, buf.Length);

                string keys_ = key;
                if (fields != null && fields.Count > 0)
                {
                    foreach (var data in fields)
                    {
                        buf = Encoding.UTF8.GetBytes(string.Format("${0}\r\n{1}\r\n", data.Key.Length, data.Key));
                        ms.Write(buf, 0, buf.Length);
                        buf = Encoding.UTF8.GetBytes(string.Format("${0}\r\n", data.Value.Length));
                        ms.Write(buf, 0, buf.Length);
                        ms.Write(data.Value, 0, data.Value.Length);
                        ms.Write(_END_DATA, 0, 2);
                        keys_ += "|" + data.Key;
                    }
                }
                return Send(ms.ToArray(), REDIS_CMD.HMSET, keys_, notify);
            }
        }
        catch (Exception ex)
        {
        }
        return false;
    }





    static byte[] __combine(int size, params byte[][] arrays)
    {
        byte[] rv = new byte[size];
        int offset = 0;
        foreach (byte[] array in arrays)
        {
            System.Buffer.BlockCopy(array, 0, rv, offset, array.Length);
            offset += array.Length;
        }
        return rv;
    }

    string ReadLine()
    {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = bstream.ReadByte()) != -1)
        {
            if (c == '\r')
                continue;
            if (c == '\n')
                break;
            sb.Append((char)c);
        }
        string s = sb.ToString().Trim();
        //Console.WriteLine(s);
        return s;
    }

    ~RedisOnlyWrite()
    {
        try
        {
            if (___threadPool != null)
                ___threadPool.Abort();
        }
        catch { }

        __signals.Clear();
        __buffers.Clear();
        __results.Clear();
        __queue_event.Close();
        __queue_ids.Clear();

        Dispose(false);
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            socket.Close();
            socket = null;
        }
    }
}

public enum REDIS_CMD
{
    SELECT,
    BGSAVE,
    PUBLISH,

    SET,
    HSET,
    HMSET,
}