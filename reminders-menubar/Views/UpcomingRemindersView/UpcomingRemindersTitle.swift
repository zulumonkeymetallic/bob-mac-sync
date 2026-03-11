import SwiftUI

struct UpcomingRemindersTitle: View {
    @ObservedObject var userPreferences = UserPreferences.shared
    @Environment(\.colorSchemeContrast) private var colorSchemeContrast

    @State var intervalButtonIsHovered = false

    var body: some View {
        HStack(alignment: .center) {
            Text(rmbLocalized(.upcomingRemindersTitle))
                .font(.headline)
                .foregroundColor(.red)
                .padding(.bottom, 5)
                .scaledToFit()
                .minimumScaleFactor(0.8)

            Spacer()

            Menu {
                ForEach(ReminderInterval.allCases, id: \.rawValue) { interval in
                    Button(action: { userPreferences.upcomingRemindersInterval = interval }) {
                        let isSelected = interval == userPreferences.upcomingRemindersInterval
                        SelectableView(title: interval.title, isSelected: isSelected)
                    }
                }

                Divider()

                Button(action: {
                    userPreferences.filterUpcomingRemindersByCalendar.toggle()
                }) {
                    SelectableView(
                        title: rmbLocalized(.filterUpcomingRemindersByCalendarOptionButton),
                        isSelected: userPreferences.filterUpcomingRemindersByCalendar
                    )
                }
            } label: {
                Label(userPreferences.upcomingRemindersInterval.title, systemImage: "calendar")
            }
            .menuStyle(BorderlessButtonMenuStyle())
            .padding(.vertical, 5)
            .padding(.horizontal, 10)
            .background(intervalButtonIsHovered ? Color.rmbColor(for: .buttonHover, and: colorSchemeContrast) : nil)
            .cornerRadius(6)
            .onHover { isHovered in
                intervalButtonIsHovered = isHovered
            }
            .padding(.trailing, 1)
            .fixedSize(horizontal: true, vertical: true)
            .help(rmbLocalized(.upcomingRemindersIntervalSelectionHelp))
        }
    }
}

struct UpcomingRemindersTitle_Previews: PreviewProvider {
    static var previews: some View {
        UpcomingRemindersTitle()
    }
}
