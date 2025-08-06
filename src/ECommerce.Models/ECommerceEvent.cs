namespace ECommerce.Models;

public record ECommerceEvent
{
    public required Guid UserId { get; init; }
    public required string SessionId { get; init; }
    public required EventTypes EventType { get; init; }
    public string? ProductId { get; init; }
    public string? Category { get; init; }
    public decimal? Price { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public required string IpAddress { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }

    public ECommerceEvent() { }
}

public enum EventTypes
{
    PageView,
    ProductSearch,
    AddToCart,
    Purchase,
    RemoveFromCart,
}