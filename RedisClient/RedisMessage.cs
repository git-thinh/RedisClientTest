using System.Collections.Generic;

public class RedisMessage
{
    public IDictionary<string, object> Paramenters { get; }
    public string Channel { get; }
    public byte[] Buffer { get; }
    public RedisMessage(string channel, byte[] buf, IDictionary<string, object> paramenters = null)
    {
        this.Channel = channel;
        this.Buffer = buf;
        this.Paramenters = paramenters;
    }
}