using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net.Sockets;
using System.Text;

public class RedisOnlySubcribe : RedisBase
{
    public RedisOnlySubcribe(
        string host = "localhost",
        int port = 6379,
        string password = "",
        int sendTimeout = 5000,
        int recieveTimeout = 5000,
        int bufferSizeRead = 64 * 1024) // 8kb = 8192 bytes
        : base(host, port, password, sendTimeout, recieveTimeout, bufferSizeRead)
    {
    }


}
