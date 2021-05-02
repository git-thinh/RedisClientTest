﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

public class RedisBase : IDisposable
{
    static readonly byte[] _END_DATA = new byte[] { 13, 10 }; //= \r\n

    private Socket socket;
    private BufferedStream bstream;

    internal int BufferSizeRead { get; } = 16 * 1024; // 1kb || 16kb || 64kb

    public string Host { get; }
    public int Port { get; }
    public int SendTimeout { get; }
    public int DatabaseNumber { get; }
    public string Password { get; }

    internal RedisBase(string host, int port,
        string password, int sendTimeout, 
        int bufferSizeRead)
    {
        this.Host = host;
        this.Port = port;
        this.SendTimeout = sendTimeout;
        this.Password = password;

        this.BufferSizeRead = bufferSizeRead;

        socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.NoDelay = true;
        socket.ReceiveTimeout = SendTimeout;
        socket.ReceiveBufferSize = int.MaxValue;
    }

    public void Connect()
    {
        socket.Connect(Host, Port);
        if (!socket.Connected)
        {
            socket.Close();
            socket = null;
            return;
        }
        bstream = new BufferedStream(new NetworkStream(socket), this.BufferSizeRead);
    }

    public bool SelectDb(int indexDb)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("*2\r\n");
            sb.Append("$6\r\nSELECT\r\n");
            sb.AppendFormat("${0}\r\n{1}\r\n", indexDb.ToString().Length, indexDb);
            byte[] buf = Encoding.UTF8.GetBytes(sb.ToString());
            bool ok = SendBuffer(buf);
            string line = ReadLine();
            return ok && !string.IsNullOrEmpty(line) && line[0] == '+';
        }
        catch (Exception ex)
        {
        }
        return false;
    }

    internal bool SendBuffer(byte[] buf)
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

    internal string ReadLine()
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

    internal string ReadString()
    {
        var result = ReadBuffer();
        if (result != null)
            return Encoding.UTF8.GetString(result);
        return null;
    }

    internal string[] ReadMultiString()
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

    internal byte[] ReadBuffer()
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

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (socket != null)
        {
            socket.Close();
            socket = null;
        }
    }
}