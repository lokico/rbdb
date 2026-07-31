// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
	name: "RBDB",
	platforms: [
		.macOS(.v13),
		.iOS(.v12),
	],
	products: [
		// Products define the executables and libraries a package produces, making them visible to other packages.
		.library(
			name: "RBDB",
			targets: ["RBDB"]),
		.library(
			name: "Datalog",
			targets: ["Datalog"]),
		.executable(
			// Suffixed with "1" because Xcode 26.6 gets confused by product names that differ only in case, though the `swift` CLI has no problem with it.
			name: "rbdb1",
			targets: ["CLI"]),
	],
	dependencies: [
		.package(url: "https://github.com/apple/swift-docc-plugin.git", from: "1.4.5"),
		.package(url: "https://github.com/pointfreeco/swift-parsing.git", from: "0.14.1"),
	],
	targets: [
		// Targets are the basic building blocks of a package, defining a module or a test suite.
		// Targets can depend on other targets in this package and products from dependencies.
		.target(
			name: "RBDB",
			dependencies: [
				.byName(name: "SQLite3", condition: .when(platforms: [.linux]))
			],
			resources: [
				.embedInCode("schema.sql")
			]
		),
		.target(
			name: "Datalog",
			dependencies: [
				"RBDB",
				.product(name: "Parsing", package: "swift-parsing"),
				.byName(name: "SQLite3", condition: .when(platforms: [.linux])),
			]
		),
		.executableTarget(
			name: "CLI",
			dependencies: ["RBDB", "Datalog"]
		),
		.testTarget(
			name: "RBDBTests",
			dependencies: ["RBDB"]
		),
		.testTarget(
			name: "DatalogTests",
			dependencies: ["Datalog", "RBDB"]
		),
		.systemLibrary(name: "SQLite3"),
	]
)
