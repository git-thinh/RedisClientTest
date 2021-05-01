using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

public class RedisOnlyWrite : IDisposable
{
    Socket socket;
    BufferedStream bstream;
    const int BUFFER_SIZE_READ = 255; // 255b || 16kb || 64kb
    static readonly byte[] _END_DATA = new byte[] { 13, 10 }; //= \r\n

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

    void Connect()
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
            if (ok && ReadLine() == "+OK")
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
            return Send(buf);
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    bool Send(byte[] buf)
    {
        if (socket == null) Connect();
        if (socket == null) return false;

        try { socket.Send(buf); }
        catch (SocketException ex)
        {
            // timeout;
            socket.Close();
            socket = null;
            return false;
        }
        return true;
    }

    public bool BGSAVE()
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*1\r\n");
            sb.Append("$6\r\nBGSAVE\r\n");
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            var ok = Send(buf);
            ReadLine(); //+Background saving started
            return ok;
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    public bool SET(string key, string value)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*3\r\n");
            sb.Append("$3\r\nSET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);
            sb.AppendFormat("${0}\r\n{1}\r\n", value.Length, value);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            return Send(buf) && ReadLine() == "+OK";
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    public bool SET(string key, byte[] value)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*3\r\n");
            sb.Append("$3\r\nSET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);

            sb.AppendFormat("${0}\r\n", value.Length);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());

            bool ok = Send(buf);
            ok = ok & Send(value);
            ok = ok & Send(_END_DATA);

            return ok && ReadLine() == "+OK";
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    public bool HSET(string key, string field, byte[] value)
        => HMSET(key, new Dictionary<string, byte[]>() { { field, value } });

    public bool HSET(string key, string field, string value)
        => HMSET(key, new Dictionary<string, string>() { { field, value } });

    public bool HMSET(string key, IDictionary<string, string> fields)
    {
        var dic = new Dictionary<string, byte[]>();
        foreach (var kv in fields)
            dic.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));
        return HMSET(key, dic);
    }

    public bool HMSET(string key, IDictionary<string, byte[]> fields)
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
                    }
                }
                return Send(ms.ToArray()) && ReadLine() == "+OK";
            }
        }
        catch (Exception ex)
        {
        }
        return false;
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

    ~RedisOnlyWrite() => Dispose(false);
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