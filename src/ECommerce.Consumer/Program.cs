using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using ECommerce.Consumer;
using ECommerce.Models;
using Elastic.Clients.Elasticsearch;

var builder = Host.CreateApplicationBuilder(args);

var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Converters = { new JsonStringEnumConverter() }
};

builder.Services.AddSingleton(provider =>
{
    var config = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "ecommerce-analytics-group",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SessionTimeoutMs = 3000
    };

    return new ConsumerBuilder<string, string>(config).Build();
});


builder.Services.AddSingleton<ElasticsearchClient>(provider =>
{
    var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"))
        .DefaultIndex("ecommerce-events");

    return new ElasticsearchClient(settings);
});

builder.Services.AddSingleton(jsonOptions);
builder.Services.AddHostedService<ConsumerWorker>();

// builder.Services.AddHostedService<Worker>();

var host = builder.Build();
await host.RunAsync();

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
                        var eventData = JsonSerializer.Deserialize<ECommerceEvent>(
                            messageValue, jsonOptions
                        );
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

