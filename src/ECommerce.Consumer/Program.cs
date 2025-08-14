using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
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
        SessionTimeoutMs = 10000
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

var host = builder.Build();
await host.RunAsync();