import EventKit

struct ReminderList: Identifiable, Equatable {
    let id: String
    let calendar: EKCalendar
    let reminders: LabeledReminders

    init(for calendar: EKCalendar, with reminderItems: [ReminderItem]) {
        id = calendar.calendarIdentifier
        self.calendar = calendar
        reminders = LabeledReminders(for: reminderItems)
    }
}
