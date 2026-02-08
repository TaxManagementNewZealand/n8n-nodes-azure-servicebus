"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AzureServiceBusApi = void 0;
class AzureServiceBusApi {
    constructor() {
        this.name = 'azureServiceBusApi';
        this.displayName = 'Azure Service Bus API';
        this.documentationUrl = 'https://docs.microsoft.com/en-us/azure/service-bus-messaging/';
        this.properties = [
            {
                displayName: 'Connection String',
                name: 'connectionString',
                type: 'string',
                typeOptions: {
                    password: true,
                },
                default: '',
                placeholder: 'Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key',
                description: 'Azure Service Bus connection string',
            },
        ];
        this.authenticate = {
            type: 'generic',
            properties: {},
        };
    }
}
exports.AzureServiceBusApi = AzureServiceBusApi;
