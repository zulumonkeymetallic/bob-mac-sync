import Foundation

enum ReminderInterval: String, Codable, CaseIterable {
    case due
    case today
    case week
    case month
    case all

    var title: String {
        switch self {
        case .due:
            rmbLocalized(.upcomingRemindersDueTitle)
        case .today:
            rmbLocalized(.upcomingRemindersTodayTitle)
        case .week:
            rmbLocalized(.upcomingRemindersInAWeekTitle)
        case .month:
            rmbLocalized(.upcomingRemindersInAMonthTitle)
        case .all:
            rmbLocalized(.upcomingRemindersAllTitle)
        }
    }

    var endingDate: Date? {
        switch self {
        case .due:
            Date()
        case .today:
            Calendar.current.endOfDay(for: Date())
        case .week:
            Calendar.current.date(byAdding: .weekOfMonth, value: 1, to: Date())
        case .month:
            Calendar.current.date(byAdding: .month, value: 1, to: Date())
        case .all:
            nil
        }
    }
}
