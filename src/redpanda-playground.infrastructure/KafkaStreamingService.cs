using redpanda_playground.domain;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace redpanda_playground.infrastructure;

public abstract class KafkaStreamingService<T> : IStreamingService<T>, IDisposable
    where T : class, IMessage<T>, new()
{
    private bool disposed = false;

    private readonly IDeserializer<T> _deserializer = null!;

    private readonly IConsumer<Ignore, T> _consumer = null!;

    public KafkaStreamingService(ConsumerConfig consumerConfig)
    {
    
        _deserializer = new ProtobufDeserializer<T>();

        _consumer = new ConsumerBuilder<Ignore, T>(consumerConfig)
            .SetValueDeserializer(_deserializer)
            .SetErrorHandler((_, e) => Console.WriteLine($"Error:", e.Reason))
            .Build();
    }

    public void Subscribe(string topics, bool moveToTail = true)
    {
        if (_consumer is null) throw new ArgumentNullException("Consumer cannot be null or empty");

        _consumer.Assign(new TopicPartitionOffset(topics, new Partition(0), moveToTail ? Offset.End : Offset.Beginning));
    }

    public async IAsyncEnumerable<T> GetData([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_consumer is null) throw new ArgumentNullException("Consumer cannot be null or empty");
        if (cancellationToken.IsCancellationRequested)
            yield return null!;

        while (!cancellationToken.IsCancellationRequested)
        {
            T value = null!;
            try
            {
                var result = _consumer.Consume(cancellationToken);
                value = result.Message.Value as T;

                await Task.Yield();
            }
            catch (OperationCanceledException ex)
            {
                var content = ex.InnerException is null ? ex.Message : ex.InnerException.Message;
                Console.WriteLine(content);
            }
            catch (ConsumeException ex)
            {
                var content = ex.InnerException is null ? ex.Message : ex.InnerException.Message;
                Console.WriteLine(content);
            }
            catch (Exception ex)
            {
                var content = ex.InnerException is null ? ex.Message : ex.InnerException.Message;
                Console.WriteLine(content);
            }


            if (!cancellationToken.IsCancellationRequested)
                yield return value;
        }

        _consumer.Unsubscribe();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposed)
        {
            if (disposing)
            {
                _consumer?.Dispose();
            }

            disposed = true;
        }
    }

    public void Dispose()
    {
        // Non modificare questo codice. Inserire il codice di pulizia nel metodo 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

}
