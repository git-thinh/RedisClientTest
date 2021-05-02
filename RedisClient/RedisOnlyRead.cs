using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;

public class RedisOnlyRead : RedisBase
{
    public RedisOnlyRead(
        string host = "localhost",
        int port = 6379,
        string password = "",
        int sendTimeout = 5000, // 5 seconds
        int recieveTimeout = 3 * 60 * 1000, // 3 minus        
        int bufferSizeRead = 512 * 1024) // 8kb || 64kb
        : base(host, port, password, sendTimeout, recieveTimeout, bufferSizeRead)
    {
    }

    public string GET(string key)
    {
        var buf = GET_BUFFER(key);
        if (buf == null) return string.Empty;
        else return Encoding.UTF8.GetString(buf);
    }

    public Bitmap GET_BITMAP(string key)
    {
        var buf = GET_BUFFER(key);
        if (buf == null) return null;
        else
        {
            var ms = new MemoryStream(buf);
            return new Bitmap(ms);
        }
    }

    public Stream GET_STREAM(string key)
    {
        var buf = GET_BUFFER(key);
        if (buf == null) return null;
        else return new MemoryStream(buf);
    }

    public byte[] GET_BUFFER(string key)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*2\r\n");
            sb.Append("$3\r\nGET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());

            bool ok = SendBuffer(buf);
            var rs = ReadBuffer();

            return rs;
        }
        catch (Exception ex)
        {
        }
        return null;
    }


    public string HGET(string key, string field)
    {
        var buf = HGET_BUFFER(key, field);
        if (buf == null) return string.Empty;
        else return Encoding.UTF8.GetString(buf);
    }

    public byte[] HGET_BUFFER(string key, string field)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*3\r\n");
            sb.Append("$4\r\nHGET\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);
            sb.AppendFormat("${0}\r\n{1}\r\n", field.Length, field);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());

            bool ok = SendBuffer(buf);
            var rs = ReadBuffer();

            return rs;
        }
        catch (Exception ex)
        {
        }
        return null;
    }

    public string[] HKEYS(string key)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*2\r\n");
            sb.Append("$5\r\nHKEYS\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", key.Length, key);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());

            bool ok = SendBuffer(buf);
            var keys = ReadMultiString();
            return keys;
        }
        catch (Exception ex)
        {
        }
        return null;
    }


    ~RedisOnlyRead() { Dispose(); }
}
