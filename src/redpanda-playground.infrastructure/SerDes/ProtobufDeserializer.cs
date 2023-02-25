using Confluent.Kafka;
using Google.Protobuf;

namespace redpanda_playground.infrastructure;

public class ProtobufDeserializer<T> : IDeserializer<T> where T : class, IMessage<T>, new()
{
    private static readonly MessageParser _parser = new MessageParser<T>(() => Activator.CreateInstance<T>());

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        var buffer = data.ToArray();
        using var stream = new MemoryStream(buffer);

        stream.Write(buffer, 0, buffer.Length);
        stream.Seek(0, SeekOrigin.Begin);

        return (T)_parser.ParseFrom(stream);
    }
}

public class ProtobufAsyncDeserializer<T> : IAsyncDeserializer<T> where T : class, IMessage<T>, new()
{
    private static readonly MessageParser _parser = new MessageParser<T>(() => Activator.CreateInstance<T>());

    public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
    {
                var buffer = data.ToArray();
        using var stream = new MemoryStream(buffer);

        stream.Write(buffer, 0, buffer.Length);
        stream.Seek(0, SeekOrigin.Begin);

        return await Task.FromResult<T>((T)_parser.ParseFrom(stream));
    }
}