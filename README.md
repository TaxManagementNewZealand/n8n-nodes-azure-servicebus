
# Azure Service Bus Node for n8n

Custom n8n node for working with Azure Service Bus using the official `@azure/service-bus` library.

## Features

- ✅ Send messages to queues
- ✅ Send messages to topics
- ✅ Receive messages from queues
- ✅ Support for custom properties
- ✅ Multiple receive modes (Peek Lock, Receive and Delete)
- ✅ Support for Azure Service Bus Connection String

## Configuration

### Add credentials in n8n:

Connection String:
```
Endpoint=sb://xxx.servicebus.windows.net/;SharedAccessKeyName=xxx;SharedAccessKey=xxx;EntityPath=xxx
```

### Using the node:

1. Select Resource: Queue
2. Queue Name: `xxx`
3. Operation: Send Message or Receive Messages

## Example usage:

```json
{
  "messageBody": "{\"productId\": 123, \"name\": \"Test Product\"}",
  "contentType": "application/json"
}
```


