using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using ECommerce.Models;
using Elastic.Clients.Elasticsearch;

public class ConsumerWorker(
    IConsumer<string, string> consumer,
    ElasticsearchClient elasticClient,
    JsonSerializerOptions jsonOptions,
    ILogger<ConsumerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        consumer.Subscribe("ecommerce-events");
        logger.LogInformation("Consumer started, subscribed to ecommerce-events topic");
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (consumeResult?.Message?.Value is { } messageValue)
                    {
                        ECommerceEvent? eventData = null;
                        
                        // Try camelCase first (new format)
                        try
                        {
                            eventData = JsonSerializer.Deserialize<ECommerceEvent>(
                                messageValue, jsonOptions
                            );
                        }
                        catch (JsonException)
                        {
                            // Fallback to PascalCase (old format)
                            try
                            {
                                var fallbackOptions = new JsonSerializerOptions
                                {
                                    Converters = { new JsonStringEnumConverter() }
                                };
                                eventData = JsonSerializer.Deserialize<ECommerceEvent>(
                                    messageValue, fallbackOptions
                                );
                                logger.LogWarning("Processed legacy message format for event: {MessageValue}", messageValue);
                            }
                            catch (JsonException ex)
                            {
                                logger.LogError(ex, "Failed to deserialize message in both formats: {MessageValue}", messageValue);
                                continue;
                            }
                        }
                        
                        if (eventData is not null)
                        {
                            var response = await elasticClient.IndexAsync(eventData,
                            idx => idx.Index("ecommerce-events")
                                .Id(Guid.NewGuid().ToString()),
                                stoppingToken);
                            if (response.IsSuccess())
                            {
                                consumer.Commit(consumeResult);
                                logger.LogInformation(
                                    "Processed event: {EventType} from {UserId} - ES ID: {DocumentId}",
                                    eventData.EventType,
                                    eventData.UserId,
                                    response.Id
                                );

                            }
                            else
                            {
                                logger.LogError(
                                    "Failed to index to Elasticsearch: {Error}",
                                    response.DebugInformation
                                );
                            }
                        }
                    }
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Kafka consume error: {Error}", ex.Error.Reason);

                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing message");
                }
            }
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("Consumer stopped");
        }
    }
    public override void Dispose()
    {
        consumer?.Dispose();
        base.Dispose();
    }
}