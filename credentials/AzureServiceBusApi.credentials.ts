import {
	IAuthenticateGeneric,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class AzureServiceBusApi implements ICredentialType {
	name = 'azureServiceBusApi';
	displayName = 'Azure Service Bus API';
	documentationUrl = 'https://docs.microsoft.com/en-us/azure/service-bus-messaging/';

	properties: INodeProperties[] = [
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

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {},
	};
}
