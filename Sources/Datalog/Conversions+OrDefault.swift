import Parsing

extension Conversion {
	@inlinable
	public static func orDefault<T: Equatable>(_ defaultValue: T) -> Self
	where Self == Conversions.OrDefault<T> {
		return .init(defaultValue: defaultValue)
	}
}

extension Conversions {
	public struct OrDefault<T: Equatable>: Conversion {
		public let defaultValue: T

		@inlinable
		public init(defaultValue: T) {
			self.defaultValue = defaultValue
		}

		@inlinable
		public func apply(_ input: T?) throws -> T {
			return input ?? defaultValue
		}

		@inlinable
		public func unapply(_ output: T) -> T? {
			return output == defaultValue ? nil : output
		}
	}
}
