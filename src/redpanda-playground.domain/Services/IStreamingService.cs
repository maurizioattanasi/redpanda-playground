namespace redpanda_playground.domain;

public interface IStreamingService<out TData> where TData : class
{
    /// <summary>
    /// Subscribes a topic (oor more than one)
    /// </summary>
    /// <param name="topics">A string containing a comma separated list of the topics to subscribeß</param>
    /// <param name="moveToTail">if true moves the stream pointer to the tail fo the stream (the latest element)</param>
    void Subscribe(string topics, bool moveToTail = true);

    /// <summary>
    /// The stream itself
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    IAsyncEnumerable<TData> GetData(CancellationToken cancellationToken = default);
}
