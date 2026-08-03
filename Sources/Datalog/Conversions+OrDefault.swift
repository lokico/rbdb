import Parsing

extension Conversion {
	@inlinable
	public static func orDefault<T: Equatable>(_ defaultValue: T, printIfDefault: Bool = false)
		-> Self
	where Self == Conversions.OrDefault<T> {
		return .init(defaultValue: defaultValue, printIfDefault: printIfDefault)
	}
}

extension Conversions {
	public struct OrDefault<T: Equatable>: Conversion {
		public let defaultValue: T
		public let printIfDefault: Bool

		@inlinable
		public init(defaultValue: T, printIfDefault: Bool) {
			self.defaultValue = defaultValue
			self.printIfDefault = printIfDefault
		}

		@inlinable
		public func apply(_ input: T?) throws -> T {
			return input ?? defaultValue
		}

		@inlinable
		public func unapply(_ output: T) -> T? {
			return (output == defaultValue && !printIfDefault) ? nil : output
		}
	}
}
