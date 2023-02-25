using Confluent.Kafka;
using Google.Protobuf;

namespace redpanda_playground.infrastructure;

public class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>, new()
{
    public byte[] Serialize(T data, SerializationContext context) => (data as IMessage<T>).ToByteArray();
}

public class ProtobufAsyncSerializer<T> : IAsyncSerializer<T> where T : IMessage<T>, new()
{
    public async Task<byte[]> SerializeAsync(T data, SerializationContext context) => await Task.FromResult<byte[]>((data as IMessage<T>).ToByteArray());
}