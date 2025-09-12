//
//  Application+Consumer.swift
//  RevenuePilot
//
//  Created by Peter Vu on 12/9/25.
//

import Foundation

extension RevenuePilot {
    @MainActor
    struct CDPMessageConsumer: MessageConsumer {
        let configuration: Configuration
        private let queueManager = SwiftQueueManagerBuilder(creator: SendBatchingMessageJobCreator()).build()

        func consume(messages: [Message]) async throws {
            let params: SendBatchingMessageJobParams = .init(messages: messages,
                                                             configuration: configuration)
            let encoder = JSONEncoder()
            let paramsEncoded = try encoder.encode(params)
            
            guard let paramsJSON = try JSONSerialization.jsonObject(with: paramsEncoded) as? [String: Any] else {
                throw NSError(domain: "co.unstatic.revflow", code: 1000)
            }
            
            JobBuilder(type: SendBatchingMessageJob.type)
                .internet(atLeast: .any)
                .persist()
                .service(quality: .background)
                .with(params: paramsJSON)
                .schedule(manager: queueManager)
        }
    }
    
    private struct SendBatchingMessageJobParams: Codable {
        let messages: [Message]
        let configuration: Configuration
    }
    
    private struct SendBatchingMessageJobCreator: JobCreator {
        func create(type: String, params: [String : Any]?) -> any Job {
            guard let params else {
                return EmptyJob()
            }
            
            do {
                let decoder = JSONDecoder()
                let paramsData = try JSONSerialization.data(withJSONObject: params)
                let params: SendBatchingMessageJobParams = try decoder.decode(SendBatchingMessageJobParams.self, from: paramsData)
                return SendBatchingMessageJob.init(params: params)
            } catch {
                return EmptyJob()
            }
        }
    }
    
    private struct SendBatchingMessageJob: Job {
        static let type = "SendBatchingMessageJob"
        
        let params: SendBatchingMessageJobParams
        private let defaultDateFormatter: ISO8601DateFormatter = {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime,
                                       .withFractionalSeconds]
            return formatter
        }()
        
        func onRun(callback: any JobResult) {
            let batchEndpoint = params.configuration.serverUrl.appendingPathComponent("batch")
            var urlRequest = URLRequest(url: batchEndpoint)
            urlRequest.httpMethod = "POST"
            urlRequest.setValue(params.configuration.apiKey, forHTTPHeaderField: "X-API-Key")
            urlRequest.setValue("application/json", forHTTPHeaderField: "Content-Type")
            
            do {
                let now: Date = Date()
                let messagesData = try jsonEncoder.encode(params.messages)
                let messagesJSONArray = (try JSONSerialization.jsonObject(with: messagesData) as? [[String: Any]]) ?? []
                
                let modifiedMessagesToSend = messagesJSONArray.map { originalMessageJSON -> [String: Any] in
                    var modifiedMessage = originalMessageJSON
                    modifiedMessage["sentAt"] = defaultDateFormatter.string(from: now)
                    return modifiedMessage
                }
                
                let modifiedMessagesData = try JSONSerialization.data(withJSONObject: [
                    "batch": modifiedMessagesToSend
                ])
                
                urlRequest.httpBody = modifiedMessagesData
                
                let dataTask = URLSession.shared.dataTask(with: urlRequest) { data, response, error in
                    if let httpResponse = response as? HTTPURLResponse {
                        switch httpResponse.statusCode {
                        case 200..<300:
                            callback.done(.success)
                        case 300...:
                            callback.done(.fail(NSError(domain: "co.unstatic.revflow", code: 1000)))
                        default:
                            callback.done(.fail(NSError(domain: "co.unstatic.revflow", code: 1000)))
                        }
                    } else if let error {
                        callback.done(.fail(error))
                    }
                }
                
                dataTask.resume()
            } catch {
                callback.done(.fail(error))
            }
        }
        
        private var jsonEncoder: JSONEncoder {
            let encoder = JSONEncoder()
            encoder.dateEncodingStrategy = .custom { date, encoder in
                var container = encoder.singleValueContainer()
                let dateString = defaultDateFormatter.string(from: date)
                try container.encode(dateString)
            }
            
            return encoder
        }
        
        func onRetry(error: any Error) -> RetryConstraint {
            return .exponential(initial: 5)
        }
        
        func onRemove(result: JobCompletion) {
            
        }
    }
    
    private struct EmptyJob: Job {
        func onRetry(error: any Error) -> RetryConstraint {
            return .cancel
        }
        
        func onRun(callback: any JobResult) {
            callback.done(.success)
        }
        
        func onRemove(result: JobCompletion) {
            
        }
    }
}
