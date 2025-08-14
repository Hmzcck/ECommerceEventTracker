using System.Text.Json.Serialization;
using System.Text.Json;
using Confluent.Kafka;
using ECommerce.Models;
using Elastic.Clients.Elasticsearch;
using Microsoft.AspNetCore.Mvc;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// Configure JSON options to handle enums as strings and consistent naming
var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Converters = { new JsonStringEnumConverter() }
};

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

builder.Services.AddSingleton(jsonOptions);

builder.Services.AddSingleton<IProducer<string, string>>(provider =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        Acks = Acks.Leader,
        EnableIdempotence = false,
        MaxInFlight = 5,
        BatchSize = 16384,
        LingerMs = 10,
        MessageTimeoutMs = 5000
    };
    return new ProducerBuilder<string, string>(config).Build();
});

builder.Services.AddSingleton<ElasticsearchClient>(provider =>
{
    var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"))
        .DefaultIndex("ecommerce-events");
    return new ElasticsearchClient(settings);
});

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapOpenApi();
app.MapScalarApiReference();

var eventsGroup = app.MapGroup("/events").WithTags("Events");

eventsGroup.MapPost("/track", async (
    [FromBody] ECommerceEvent eventData,
    [FromServices] IProducer<string, string> producer,
    [FromServices] JsonSerializerOptions jsonOptions,
    HttpContext context) =>
    {
        try
        {
            var ipAddress = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";

            var eventWithDeatils = eventData with
            {
                IpAddress = ipAddress,
                Timestamp = DateTime.UtcNow
            };


            var message = new Message<string, string>
            {
                Key = eventWithDeatils.UserId.ToString(),
                Value = JsonSerializer.Serialize(eventWithDeatils, jsonOptions)
            };


            var deliveryResult = await producer.ProduceAsync("ecommerce-events", message);

            return Results.Ok(new
            {
                success = true,
                message = "Event tracked successfully",
                partition = deliveryResult.Partition.Value,
                Offset = deliveryResult.Offset.Value,
            });

        }
        catch (ProduceException<string, string> ex)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = $"Kafka error: {ex.Error.Reason}",
            });
        }
        catch (Exception ex)
        {
            return Results.Problem($"Unexpected error: {ex.Message}");
        }

    }).WithName("TrackEvent")
      .WithOpenApi()
      .WithSummary("Track a single ecommerce event");

eventsGroup.MapPost("/track-batch", async (
    [FromBody] ECommerceEvent[] events,
    [FromServices] IProducer<string, string> producer,
    [FromServices] JsonSerializerOptions jsonOptions,
    HttpContext context) =>
    {
        try
        {
            var ipAddress = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
            var tasks = new List<Task<DeliveryResult<string, string>>>();

            foreach (var eventData in events)
            {
                var eventWithDetails = eventData with
                {
                    IpAddress = ipAddress,
                    Timestamp = DateTime.UtcNow
                };

                var message = new Message<string, string>
                {
                    Key = eventWithDetails.UserId.ToString(),
                    Value = JsonSerializer.Serialize(eventWithDetails, jsonOptions)
                };

                tasks.Add(producer.ProduceAsync("ecommerce-events", message));
            }

            var results = await Task.WhenAll(tasks);

            return Results.Ok(new
            {
                success = true,
                message = $"Batch of {events.Length} events tracked",
                processedCount = results.Length
            });

        }
        catch (AggregateException ex)
        {
            var kafkaErrors = ex.InnerExceptions
                .OfType<ProduceException<string, string>>()
                .Select(e => e.Error.Reason);

            return Results.BadRequest(new
            {
                success = false,
                message = "Batch processing failed",
                errors = kafkaErrors
            });
        }
        catch (ProduceException<string, string> ex)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = $"Kafka error: {ex.Error.Reason}"
            });
        }
        catch (Exception ex)
        {
            return Results.Problem($"Batch processing error: {ex.Message}");
        }
    })
.WithName("TrackBatchEvents")
.WithOpenApi();

eventsGroup.MapPost("/generate-test-data", async (
    IProducer<string, string> producer,
    JsonSerializerOptions jsonOptions,
    int count = 100) =>
{
    if (!app.Environment.IsDevelopment())
    {
        return Results.Forbid();
    }

    try
    {
        var random = new Random();
        var userIds = Enumerable.Range(1, 50).Select(i => Guid.NewGuid()).ToArray();
        var productIds = Enumerable.Range(1, 20).Select(i => $"prod_{i}").ToArray();
        var categories = new[] { "Electronics", "Clothing", "Books", "Home", "Sports" };
        var eventTypes = Enum.GetValues<EventTypes>();

        var tasks = new List<Task>();

        for (int i = 0; i < count; i++)
        {
            var eventData = new ECommerceEvent
            {
                UserId = userIds[random.Next(userIds.Length)],
                SessionId = Guid.NewGuid().ToString()[..8],
                EventType = eventTypes[random.Next(eventTypes.Length)],
                ProductId = productIds[random.Next(productIds.Length)],
                Category = categories[random.Next(categories.Length)],
                Price = random.Next(10, 500),
                Timestamp = DateTime.UtcNow.AddMinutes(-random.Next(0, 1440)),
                IpAddress = $"192.168.1.{random.Next(1, 255)}"
            };

            var message = new Message<string, string>
            {
                Key = eventData.UserId.ToString(),
                Value = JsonSerializer.Serialize(eventData, jsonOptions)
            };

            tasks.Add(producer.ProduceAsync("ecommerce-events", message));
        }

        await Task.WhenAll(tasks);

        return Results.Ok(new
        {
            success = true,
            message = $"Generated {count} test events"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Test data generation error: {ex.Message}");
    }
})
.WithName("GenerateTestData")
.WithOpenApi();

app.UseHttpsRedirection();

app.Run();