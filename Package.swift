// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "RevenuePilot",
    platforms: [
        .iOS(.v13),
        .macOS(.v12)
    ],
    products: [
        .library(
            name: "RevenuePilot",
            targets: ["RevenuePilot"]),
    ],
    dependencies: [],
    targets: [
        .target(
            name: "RevenuePilot",
            path: "Sources",
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .testTarget(
            name: "RevenuePilotTests",
            dependencies: ["RevenuePilot"],
            path: "Tests"
        )
    ]
)
