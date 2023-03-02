using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using redpanda_playground.domain;
using RedpandaPlayground.Contracts;

namespace redpanda_playground.infrastructure.integration_tests;

public class OHLCVStreamingService : KafkaStreamingService<OHLCV>
{
    public OHLCVStreamingService(ConsumerConfig consumerConfig) : base(consumerConfig)
    {
    }
}

public class KafkaStreamTest
{
    private readonly ConsumerConfig _consumerConfig;
    private readonly ProducerConfig _producerConfig;

    private readonly AdminClientConfig _adminClientConfig = null!;

    private readonly IProducer<Null, OHLCV> _producer = null!;

    private readonly IStreamingService<OHLCV> _streamingService = null!;

    private readonly string[] _topics = new string[] { "ohlcv" };

    private ILogger<T> GetLogger<T>() => NullLoggerFactory.Instance.CreateLogger<T>();

    public KafkaStreamTest()
    {

        _producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:29092",
            EnableDeliveryReports = true,
            LingerMs = 0,
            BatchSize = 1048576,
            EnableIdempotence = true,
            CompressionType = CompressionType.Gzip,
            CompressionLevel = 4
        };

        _adminClientConfig = new AdminClientConfig { BootstrapServers = _producerConfig.BootstrapServers };
        _producer = new ProducerBuilder<Null, OHLCV>(_producerConfig)
            .SetValueSerializer(new ProtobufSerializer<OHLCV>())
            .Build();


        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:29092",
            GroupId = "quantum_trader",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SocketKeepaliveEnable = true,
            SocketTimeoutMs = 60000
        };

        _streamingService = new OHLCVStreamingService(_consumerConfig);
    }

    private async Task CreateTopicAsync()
    {
        using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();
        try
        {
            try
            {
                await adminClient.DeleteTopicsAsync(_topics, new DeleteTopicsOptions { }).ConfigureAwait(false);
            }
            catch (Exception) { }
            finally
            {
                var topics = _topics.Select(t => new TopicSpecification { Name = t });
                await adminClient.CreateTopicsAsync(topics);
            }
        }
        catch (CreateTopicsException) { }
        finally
        {
            await adminClient.DeleteTopicsAsync(_topics, null);
        }
    }

    [Fact]
    public async Task Test1()
    {
        /// Arrange
        var tick = new OHLCV
        {
            Symbol = "RACE.MI",
            Time = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
            Open = 246.50F,
            High = 247.70F,
            Low = 243.80F,
            Close = 244.80F,
            Volume = 278.445F
        };
        //await CreateTopicAsync();
        _streamingService.Subscribe(_topics[0]);
        IAsyncEnumerator<OHLCV> e = _streamingService.GetData().GetAsyncEnumerator();

        /// Act
        if (_producer is not null)
        {
            await _producer.ProduceAsync(topic: _topics[0], new Message<Null, OHLCV> { Value = tick })
                .ContinueWith(t => t.IsFaulted ? $"{t.Exception?.Message}" : $"{t.Result.TopicPartitionOffset}").ConfigureAwait(false);            
        }
        while (await e.MoveNextAsync()) { break; }

        /// Assert
        Assert.Equal("quantum_trader", _consumerConfig.GroupId);
        Assert.Equal(CompressionType.Gzip, _producerConfig.CompressionType);
        Assert.Equal(tick.Time, e.Current?.Time);
    }
}