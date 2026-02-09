/**
 * Integration tests for Azure Service Bus node
 * These tests mock the Azure SDK to test the execute function behavior
 */

import { AzureServiceBus } from '../nodes/AzureServiceBus/AzureServiceBus.node';
import type { IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';

// Mock the Azure Service Bus SDK
jest.mock('@azure/service-bus', () => {
	const mockSender = {
		sendMessages: jest.fn().mockResolvedValue(undefined),
		close: jest.fn().mockResolvedValue(undefined),
	};

	const mockReceivedMessage = {
		messageId: 'test-message-id-123',
		body: { testData: 'hello world' },
		contentType: 'application/json',
		enqueuedTimeUtc: new Date('2026-02-05T12:00:00Z'),
		applicationProperties: { customProp: 'value' },
		deliveryCount: 1,
		sequenceNumber: BigInt(1),
		sessionId: undefined,
	};

	const mockReceiver = {
		receiveMessages: jest.fn().mockResolvedValue([mockReceivedMessage]),
		completeMessage: jest.fn().mockResolvedValue(undefined),
		close: jest.fn().mockResolvedValue(undefined),
	};

	const mockSessionReceiver = {
		...mockReceiver,
		sessionId: 'test-session-123',
		getSessionState: jest.fn().mockResolvedValue({ state: 'active' }),
		setSessionState: jest.fn().mockResolvedValue(undefined),
	};

	return {
		ServiceBusClient: jest.fn().mockImplementation(() => ({
			createSender: jest.fn().mockReturnValue(mockSender),
			createReceiver: jest.fn().mockReturnValue(mockReceiver),
			acceptSession: jest.fn().mockResolvedValue(mockSessionReceiver),
			acceptNextSession: jest.fn().mockResolvedValue(mockSessionReceiver),
			close: jest.fn().mockResolvedValue(undefined),
		})),
		__mockSender: mockSender,
		__mockReceiver: mockReceiver,
		__mockSessionReceiver: mockSessionReceiver,
		__mockReceivedMessage: mockReceivedMessage,
	};
});

// Mock node-fetch
jest.mock('node-fetch', () => jest.fn());

// Mock ws
jest.mock('ws', () => jest.fn());

describe('AzureServiceBus Integration Tests', () => {
	let node: AzureServiceBus;
	let mockExecuteFunctions: jest.Mocked<IExecuteFunctions>;
	const { ServiceBusClient, __mockSender, __mockReceiver } = jest.requireMock('@azure/service-bus');

	const createMockExecuteFunctions = (params: Record<string, unknown> = {}): jest.Mocked<IExecuteFunctions> => {
		const defaultParams: Record<string, unknown> = {
			resource: 'queue',
			operation: 'sendMessage',
			protocol: 'sdk',
			queueName: 'test-queue',
			messageBody: '{"test": "data"}',
			contentType: 'application/json',
			messageId: '',
			sessionId: '',
			messageProperties: {},
			maxMessageCount: 10,
			maxWaitTimeInSeconds: 60,
			receiveMode: 'peekLock',
			sessionMode: 'none',
			...params,
		};

		return {
			getInputData: jest.fn().mockReturnValue([{ json: {} }]),
			getNodeParameter: jest.fn().mockImplementation((name: string, _index: number, fallback?: unknown) => {
				return defaultParams[name] ?? fallback;
			}),
			getCredentials: jest.fn().mockResolvedValue({
				connectionString: 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey123',
			}),
			getNode: jest.fn().mockReturnValue({ name: 'Azure Service Bus' }),
		} as unknown as jest.Mocked<IExecuteFunctions>;
	};

	beforeEach(() => {
		node = new AzureServiceBus();
		jest.clearAllMocks();
	});

	describe('Send Message Operation', () => {
		it('should send a message to a queue successfully', async () => {
			mockExecuteFunctions = createMockExecuteFunctions();
			
			const result = await node.execute.call(mockExecuteFunctions);

			expect(result).toBeDefined();
			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toMatchObject({
				success: true,
				queueName: 'test-queue',
			});
			expect(__mockSender.sendMessages).toHaveBeenCalledTimes(1);
			expect(__mockSender.close).toHaveBeenCalledTimes(1);
		});

		it('should send a message with session ID', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				sessionId: 'session-123',
			});

			const result = await node.execute.call(mockExecuteFunctions);

			expect(result[0][0].json.success).toBe(true);
			expect(__mockSender.sendMessages).toHaveBeenCalledWith(
				expect.objectContaining({
					sessionId: 'session-123',
				})
			);
		});

		it('should send a message with custom properties', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				messageProperties: {
					property: [
						{ key: 'customKey', value: 'customValue' },
					],
				},
			});

			const result = await node.execute.call(mockExecuteFunctions);

			expect(result[0][0].json.success).toBe(true);
			expect(__mockSender.sendMessages).toHaveBeenCalledWith(
				expect.objectContaining({
					applicationProperties: { customKey: 'customValue' },
				})
			);
		});

		it('should handle multiple input items', async () => {
			mockExecuteFunctions = createMockExecuteFunctions();
			mockExecuteFunctions.getInputData.mockReturnValue([
				{ json: {} },
				{ json: {} },
				{ json: {} },
			]);

			const result = await node.execute.call(mockExecuteFunctions);

			expect(result[0]).toHaveLength(3);
			expect(__mockSender.sendMessages).toHaveBeenCalledTimes(3);
		});

		it('should throw error for empty message body', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				messageBody: '',
			});

			await expect(node.execute.call(mockExecuteFunctions)).rejects.toThrow('Message Body cannot be empty');
		});
	});

	describe('Receive Messages Operation', () => {
		it('should receive messages from a queue', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				operation: 'receiveMessages',
				maxMessageCount: 10,
				maxWaitTimeInSeconds: 30,
				receiveMode: 'peekLock',
				sessionMode: 'none',
			});

			const result = await node.execute.call(mockExecuteFunctions);

			expect(result).toBeDefined();
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toMatchObject({
				messageId: 'test-message-id-123',
				body: { testData: 'hello world' },
				contentType: 'application/json',
			});
			expect(__mockReceiver.receiveMessages).toHaveBeenCalledWith(10, {
				maxWaitTimeInMs: 30000,
			});
			expect(__mockReceiver.completeMessage).toHaveBeenCalledTimes(1);
		});
	});

	describe('Topic Operations', () => {
		it('should send a message to a topic', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				resource: 'topic',
				operation: 'sendMessage',
				topicName: 'test-topic',
			});

			const result = await node.execute.call(mockExecuteFunctions);

			expect(result[0][0].json).toMatchObject({
				success: true,
				topicName: 'test-topic',
			});
		});
	});

	describe('Error Handling', () => {
		it('should throw error when connection string is missing', async () => {
			mockExecuteFunctions = createMockExecuteFunctions();
			mockExecuteFunctions.getCredentials.mockResolvedValue({
				connectionString: '',
			});

			await expect(node.execute.call(mockExecuteFunctions)).rejects.toThrow('connection string is required');
		});

		it('should throw error for HTTP protocol on receive', async () => {
			mockExecuteFunctions = createMockExecuteFunctions({
				operation: 'receiveMessages',
				protocol: 'http',
			});

			await expect(node.execute.call(mockExecuteFunctions)).rejects.toThrow('only supported with Azure SDK protocol');
		});
	});
});

describe('AzureServiceBus HTTP Protocol Tests', () => {
	let node: AzureServiceBus;
	let mockExecuteFunctions: jest.Mocked<IExecuteFunctions>;
	const mockFetch = jest.requireMock('node-fetch') as jest.Mock;

	beforeEach(() => {
		node = new AzureServiceBus();
		jest.clearAllMocks();
		
		// Setup successful HTTP response
		mockFetch.mockResolvedValue({
			ok: true,
			status: 201,
			statusText: 'Created',
			text: jest.fn().mockResolvedValue(''),
		});
	});

	const createMockExecuteFunctions = (params: Record<string, unknown> = {}): jest.Mocked<IExecuteFunctions> => {
		const defaultParams: Record<string, unknown> = {
			resource: 'queue',
			operation: 'sendMessage',
			protocol: 'http',
			queueName: 'test-queue',
			messageBody: '{"test": "data"}',
			contentType: 'application/json',
			messageId: 'msg-123',
			sessionId: '',
			messageProperties: {},
			...params,
		};

		return {
			getInputData: jest.fn().mockReturnValue([{ json: {} }]),
			getNodeParameter: jest.fn().mockImplementation((name: string, _index: number, fallback?: unknown) => {
				return defaultParams[name] ?? fallback;
			}),
			getCredentials: jest.fn().mockResolvedValue({
				connectionString: 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdGtleTEyMw==',
			}),
			getNode: jest.fn().mockReturnValue({ name: 'Azure Service Bus' }),
		} as unknown as jest.Mocked<IExecuteFunctions>;
	};

	it('should send message via HTTP REST API', async () => {
		mockExecuteFunctions = createMockExecuteFunctions();

		const result = await node.execute.call(mockExecuteFunctions);

		expect(result[0][0].json.success).toBe(true);
		expect(mockFetch).toHaveBeenCalledWith(
			expect.stringContaining('https://test.servicebus.windows.net/test-queue/messages'),
			expect.objectContaining({
				method: 'POST',
				body: '{"test": "data"}',
			})
		);
	});

	it('should include session ID in broker properties for HTTP', async () => {
		mockExecuteFunctions = createMockExecuteFunctions({
			sessionId: 'http-session-456',
		});

		await node.execute.call(mockExecuteFunctions);

		expect(mockFetch).toHaveBeenCalledWith(
			expect.any(String),
			expect.objectContaining({
				headers: expect.objectContaining({
					BrokerProperties: expect.stringContaining('http-session-456'),
				}),
			})
		);
	});

	it('should handle HTTP error responses', async () => {
		mockFetch.mockResolvedValue({
			ok: false,
			status: 401,
			statusText: 'Unauthorized',
			text: jest.fn().mockResolvedValue('Invalid signature'),
		});

		mockExecuteFunctions = createMockExecuteFunctions();

		await expect(node.execute.call(mockExecuteFunctions)).rejects.toThrow('HTTP 401');
	});
});
