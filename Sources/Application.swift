import Combine
import Foundation

public protocol AnalyticEvent {
    var eventName: String { get }
    var attributes: [String: any Sendable]? { get }
}

public protocol UserAttribute {
    associatedtype Value
    var attributeName: String { get }
}

struct UserEmailAttribute: UserAttribute {
    typealias Value = String
    let attributeName: String = "__email"
}

public struct Configuration: Codable, Sendable {
    public var apiKey: String
    public var flushQueueSize: Int = 10
    public var flushInterval: TimeInterval
    public var optOut: Bool
    public var useBatch: Bool
    let serverUrl: URL
    var logger: any RevenuePilotLogger {
        RevenuePilotConsoleLogger()
    }

    public var flushEventsOnClose: Bool

    public init(
        apiKey: String,
        flushInterval: TimeInterval = 30,
        optOut: Bool = false,
        useBatch: Bool = true,
        flushEventsOnClose: Bool = true
    ) {
        self.apiKey = apiKey
        self.flushInterval = flushInterval
        self.optOut = optOut
        self.useBatch = useBatch
        serverUrl = URL(string: "https://cdp-api.revflow.dev")!
        self.flushEventsOnClose = flushEventsOnClose
    }
}

public final class RevenuePilot: @unchecked Sendable {
    static let sdkVersion: String = "1.0.0"
    static let apiVersion: String = "1"

    public let configuration: Configuration
    private let queue: MessageQueue
    private let consumer: CDPMessageConsumer
    private let queueRunLoopTask: Task<Void, Never>

    public init(configuration: Configuration) {
        let consumer = CDPMessageConsumer(configuration: configuration)
        self.configuration = configuration
        self.consumer = consumer
        let queue = MessageQueue(consumer: consumer)
        self.queue = queue

        queueRunLoopTask = Task(
            priority: .background,
            operation: { [weak queue] in
                await queue?.startRunloop()
            }
        )
    }

    public func track<E: AnalyticEvent>(event: E) {
        track(event: event.eventName, attributes: event.attributes)
    }

    public func track(event: String, attributes: [String: any Sendable]? = nil) {
        Task(priority: .background) {
            await queue.emit(
                .track(
                    event: event,
                    userId: currentUserId,
                    anonymousId: anonymousId,
                    apiVersion: Self.apiVersion,
                    properties: attributes,
                    context: Self.currentContext
                ))
        }
    }

    public func identify(userId: String, traits _: [String: Any]? = nil) {
        currentUserId = userId
    }

    deinit {
        queueRunLoopTask.cancel()
    }
}

private extension RevenuePilot {
    var anonymousId: String {
        if let previouslySavedId = UserDefaults.standard.string(forKey: "__revflowAnonymousId") {
            return previouslySavedId
        } else {
            let newID = UUID().uuidString
            UserDefaults.standard.set(newID, forKey: "__revflowAnonymousId")
            return newID
        }
    }

    var currentUserId: String? {
        get {
            UserDefaults.standard.string(forKey: "__revflowUserId")
        }
        set {
            if let newValue {
                UserDefaults.standard.set(newValue, forKey: "__revflowUserId")
            } else {
                UserDefaults.standard.removeObject(forKey: "__revflowUserId")
            }
        }
    }
}
