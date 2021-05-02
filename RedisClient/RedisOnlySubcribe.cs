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
        int sendTimeout = 15 * 60 * 1000,
        // 255 byte
        int bufferSizeRead = 255)
        : base(host, port, password, sendTimeout, bufferSizeRead) {    
    }


}
