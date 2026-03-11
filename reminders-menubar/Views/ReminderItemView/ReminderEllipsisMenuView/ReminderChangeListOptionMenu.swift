import EventKit
import SwiftUI

struct ReminderChangeListOptionMenu: View {
    @EnvironmentObject var remindersData: RemindersData

    var reminder: EKReminder
    var reminderHasChildren: Bool

    var body: some View {
        if !reminderHasChildren {
            Menu {
                ForEach(remindersData.calendars, id: \.calendarIdentifier) { calendar in
                    Button(action: {
                        reminder.calendar = calendar
                        RemindersService.shared.save(reminder: reminder)
                    }) {
                        let isSelected = calendar.calendarIdentifier == reminder.calendar.calendarIdentifier
                        SelectableView(
                            title: calendar.title,
                            isSelected: isSelected,
                            color: Color(calendar.color)
                        )
                    }
                }
            } label: {
                HStack {
                    Image(systemName: "folder")
                    Text(rmbLocalized(.changeReminderListMenuOption))
                }
            }
        }
    }
}

#Preview {
    var reminder: EKReminder {
        let calendar = EKCalendar(for: .reminder, eventStore: .init())
        calendar.color = .systemTeal

        let reminder = EKReminder(eventStore: .init())
        reminder.title = "Look for awesome projects on GitHub"
        reminder.isCompleted = false
        reminder.calendar = calendar
        reminder.dueDateComponents = Date().dateComponents(withTime: true)
        reminder.ekPriority = .high

        return reminder
    }

    ReminderChangeListOptionMenu(reminder: reminder, reminderHasChildren: false)
        .environmentObject(RemindersData())
}
