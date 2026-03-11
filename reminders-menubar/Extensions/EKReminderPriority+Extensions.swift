import EventKit

extension EKReminderPriority {
    var systemImage: String? {
        switch self {
        case .high:
            "exclamationmark.3"
        case .medium:
            "exclamationmark.2"
        case .low:
            "exclamationmark"
        default:
            nil
        }
    }

    var nextPriority: EKReminderPriority {
        switch self {
        case .low:
            .medium
        case .medium:
            .high
        case .high:
            .none
        default:
            .low
        }
    }
}
