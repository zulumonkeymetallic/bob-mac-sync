enum RmbMenuBarCounterType: String, Codable, CaseIterable {
    case due
    case today
    case allReminders
    case disabled

    var title: String {
        switch self {
        case .due:
            rmbLocalized(.showMenuBarDueCountOptionButton)
        case .today:
            rmbLocalized(.showMenuBarTodayCountOptionButton)
        case .allReminders:
            rmbLocalized(.showMenuBarAllRemindersCountOptionButton)
        case .disabled:
            rmbLocalized(.showMenuBarNoCountOptionButton)
        }
    }
}
