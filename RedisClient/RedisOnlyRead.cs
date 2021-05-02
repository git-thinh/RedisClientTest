using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;

public class RedisOnlyRead : IDisposable
{
    Socket socket;
    BufferedStream bstream;
    const int BUFFER_SIZE_READ = 64 * 1024; // 16kb || 64kb
    static readonly byte[] _END_DATA = new byte[] { 13, 10 }; //= \r\n

    public string Host { get; private set; }
    public int Port { get; private set; }
    public int SendTimeout { get; set; }
    public int DatabaseNumber { get; set; }
    public string Password { get; set; }

    public RedisOnlyRead(string host = "localhost", int port = 6379,
        string password = "", int sendTimeout = 15 * 60 * 1000)
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
        socket.ReceiveTimeout = SendTimeout;
        socket.ReceiveBufferSize = int.MaxValue;
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
        else { 
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

            bool ok = Send(buf);
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

            bool ok = Send(buf);
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

            bool ok = Send(buf);
            var keys = ReadMultiString();
            return keys;
        }
        catch (Exception ex)
        {
        }
        return null;
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
        return sb.ToString();
    }

    string ReadString()
    {
        var result = ReadBuffer();
        if (result != null)
            return Encoding.UTF8.GetString(result);
        return null;
    }

    string[] ReadMultiString()
    {
        string r = ReadLine();
        //Log(string.Format("R: {0}", r));
        if (r.Length == 0)
            throw new Exception("Zero length respose");

        char c = r[0];
        if (c == '-')
            throw new Exception(r.StartsWith("-ERR") ? r.Substring(5) : r.Substring(1));

        List<string> result = new List<string>();

        if (c == '*')
        {
            int n;
            if (Int32.TryParse(r.Substring(1), out n))
                for (int i = 0; i < n; i++)
                {
                    result.Add(ReadString());
                }
        }
        return result.ToArray();
    }

    byte[] ReadBuffer()
    {
        string s = ReadLine();
        //Log("S", s);
        if (s.Length == 0)
            throw new ResponseException("Zero length respose");

        char c = s[0];
        if (c == '-')
            throw new ResponseException(s.StartsWith("-ERR ") ? s.Substring(5) : s.Substring(1));

        if (c == '$')
        {
            if (s == "$-1")
                return null;
            int n;

            if (Int32.TryParse(s.Substring(1), out n))
            {
                byte[] retbuf = new byte[n];

                int bytesRead = 0;
                do
                {
                    int read = bstream.Read(retbuf, bytesRead, n - bytesRead);
                    if (read < 1)
                        throw new ResponseException("Invalid termination mid stream");
                    bytesRead += read;
                }
                while (bytesRead < n);
                if (bstream.ReadByte() != '\r' || bstream.ReadByte() != '\n')
                    throw new ResponseException("Invalid termination");
                return retbuf;
            }
            throw new ResponseException("Invalid length");
        }

        /* don't treat arrays here because only one element works -- use DataArray!
		//returns the number of matches
		if (c == '*') {
			int n;
			if (Int32.TryParse(s.Substring(1), out n)) 
				return n <= 0 ? new byte [0] : ReadData();			
			throw new ResponseException ("Unexpected length parameter" + r);
		}
		*/

        throw new ResponseException("Unexpected reply: " + s);
    }

    ~RedisOnlyRead() => Dispose(false);
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

public class ResponseException : Exception
{
    public ResponseException(string code) : base("Response error") => Code = code;
    public string Code { get; private set; }
}