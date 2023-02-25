using redpanda_playground.domain;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace redpanda_playground.infrastructure;

public class KafkaStreamingService<T, TData> : IStreamingService<TData>, IDisposable
    where T : class, IMessage<T>, new()
    where TData : class
{
    private bool disposed = false;
    private readonly ConsumerConfig _consumerConfig;
    private readonly ILogger<KafkaStreamingService<T, TData>> _logger;

    public KafkaStreamingService(ConsumerConfig consumerConfig,
                                 ILogger<KafkaStreamingService<T, TData>> logger)
    {
        _consumerConfig = consumerConfig;
        _logger = logger;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposed)
        {
            if (disposing)
            {
                // TODO: eliminare lo stato gestito (oggetti gestiti)
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

    public void Subscribe(string topics, bool moveToTail = true)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<TData> GetData(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}
