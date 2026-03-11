import Combine
import EventKit
import SwiftUI

@MainActor
class RemindersData: ObservableObject {
    private var cancellationTokens: [AnyCancellable] = []

    init() {
        addObservers()
        Task {
            await update()
        }
    }

    // swiftlint:disable:next function_body_length
    private func addObservers() {
        NotificationCenter.default.publisher(for: .EKEventStoreChanged)
            .sink { [weak self] _ in
                Task {
                    await self?.update()
                }
            }
            .store(in: &cancellationTokens)

        NotificationCenter.default.publisher(for: .NSCalendarDayChanged)
            .sink { [weak self] _ in
                Task {
                    await self?.update()
                }
            }
            .store(in: &cancellationTokens)

        UserPreferences.shared.$menuBarCounterType
            .dropFirst()
            .sink { [weak self] _ in
                Task {
                    guard let self else { return }
                    await self.updateMenuBarCount(with: self.getMenuBarCount())
                }
            }
            .store(in: &cancellationTokens)

        UserPreferences.shared.$filterMenuBarCountByCalendar
            .dropFirst()
            .sink { [weak self] _ in
                Task {
                    guard let self else { return }
                    await self.updateMenuBarCount(with: self.getMenuBarCount())
                }
            }
            .store(in: &cancellationTokens)

        UserPreferences.shared.$upcomingRemindersInterval
            .dropFirst()
            .sink { [weak self] _ in
                Task {
                    guard let self else { return }
                    self.upcomingReminders = await self.getUpcomingReminders()
                }
            }
            .store(in: &cancellationTokens)

        UserPreferences.shared.$filterUpcomingRemindersByCalendar
            .dropFirst()
            .sink { [weak self] _ in
                Task {
                    guard let self else { return }
                    self.upcomingReminders = await self.getUpcomingReminders()
                }
            }
            .store(in: &cancellationTokens)

        $calendarIdentifiersFilter
            .dropFirst()
            .sink { [weak self] calendarIdentifiersFilter in
                Task {
                    guard let self else { return }
                    self.filteredReminderLists = await RemindersService.shared.getReminders(
                        of: calendarIdentifiersFilter
                    )

                    self.upcomingReminders = await self.getUpcomingReminders()
                    await self.updateMenuBarCount(with: self.getMenuBarCount())
                }
            }
            .store(in: &cancellationTokens)
    }

    @Published var calendars: [EKCalendar] = []

    @Published var upcomingReminders: [ReminderItem] = []

    @Published var filteredReminderLists: [ReminderList] = []

    @Published var calendarIdentifiersFilter: [String] = {
        guard let identifiers = UserPreferences.shared.preferredCalendarIdentifiersFilter else {
            // NOTE: On first use it will load all reminder lists.
            let allCalendars = RemindersService.shared.getCalendars()
            return allCalendars.map(\.calendarIdentifier)
        }

        return identifiers
    }() {
        didSet {
            UserPreferences.shared.preferredCalendarIdentifiersFilter = calendarIdentifiersFilter
        }
    }

    @Published var calendarForSaving: EKCalendar? = {
        guard RemindersService.shared.hasFullRemindersAccess() else {
            return nil
        }

        guard let identifier = UserPreferences.shared.preferredCalendarIdentifierForSaving,
              let calendar = RemindersService.shared.getCalendar(withIdentifier: identifier)
        else {
            return RemindersService.shared.getDefaultCalendar()
        }

        return calendar
    }() {
        didSet {
            let identifier = calendarForSaving?.calendarIdentifier
            UserPreferences.shared.preferredCalendarIdentifierForSaving = identifier
        }
    }

    func update() async {
        let calendars = RemindersService.shared.getCalendars()

        let calendarsSet = Set(calendars.map(\.calendarIdentifier))
        let calendarIdentifiersFilter = calendarIdentifiersFilter.filter {
            // NOTE: Checking if calendar in filter still exist
            calendarsSet.contains($0)
        }

        self.calendars = calendars
        self.calendarIdentifiersFilter = calendarIdentifiersFilter
        upcomingReminders = await getUpcomingReminders()
        await updateMenuBarCount(with: getMenuBarCount())
    }

    private func getUpcomingReminders() async -> [ReminderItem] {
        let calendarFilter = UserPreferences.shared.filterUpcomingRemindersByCalendar
            ? calendarIdentifiersFilter
            : nil

        return await RemindersService.shared.getUpcomingReminders(
            UserPreferences.shared.upcomingRemindersInterval,
            for: calendarFilter
        )
    }

    private func getMenuBarCount() async -> Int {
        let calendarFilter = UserPreferences.shared.filterMenuBarCountByCalendar
            ? calendarIdentifiersFilter
            : nil

        switch UserPreferences.shared.menuBarCounterType {
        case .due:
            return await RemindersService.shared.getUpcomingReminders(.due, for: calendarFilter).count
        case .today:
            return await RemindersService.shared.getUpcomingReminders(.today, for: calendarFilter).count
        case .allReminders:
            return await RemindersService.shared.getUpcomingReminders(.all, for: calendarFilter).count
        case .disabled:
            return -1
        }
    }

    private func updateMenuBarCount(with count: Int) {
        AppDelegate.shared?.updateMenuBarTodayCount(to: count)
    }
}
