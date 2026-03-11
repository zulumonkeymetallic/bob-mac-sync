import Foundation

extension Date {
    var isPast: Bool {
        timeIntervalSinceNow < 0
    }

    var isToday: Bool {
        Calendar.current.isDateInToday(self)
    }

    var isYesterday: Bool {
        Calendar.current.isDateInYesterday(self)
    }

    var isDayBeforeYesterday: Bool {
        let dayBeforeYesterday = Calendar.current.date(byAdding: .day, value: -2, to: Date()) ?? self
        return isSameDay(as: dayBeforeYesterday)
    }

    var isThisYear: Bool {
        Calendar.current.isDate(self, equalTo: Date(), toGranularity: .year)
    }

    var elapsedTimeInterval: TimeInterval {
        Date().timeIntervalSince(self)
    }

    static func nextExactHour(of date: Date = Date(), allowDayChange: Bool = false) -> Date {
        let calendar = Calendar.current
        let now = Date()
        guard let nextHourFromNow = calendar.date(byAdding: .hour, value: 1, to: now) else { return date }
        let isNextHourChangingDay = !calendar.isDateInToday(nextHourFromNow)

        var hourComponent = calendar.dateComponents([.hour], from: now)
        if allowDayChange || !isNextHourChangingDay {
            hourComponent.hour = (hourComponent.hour ?? 0) + 1
        }

        guard let dateWithoutTime = calendar.date(bySettingHour: 0, minute: 0, second: 0, of: date) else { return date }
        return calendar.date(byAdding: hourComponent, to: dateWithoutTime) ?? dateWithoutTime
    }

    static func nextYear(of date: Date = Date()) -> Date {
        Calendar.current.date(byAdding: .year, value: 1, to: date) ?? date
    }

    func isSameDay(as otherDate: Date) -> Bool {
        Calendar.current.isDate(self, inSameDayAs: otherDate)
    }

    func relativeDateDescription(withTime showTimeDescription: Bool) -> String {
        let relativeDateFormatter = DateFormatter()
        relativeDateFormatter.timeStyle = showTimeDescription ? .short : .none
        relativeDateFormatter.dateStyle = .medium
        relativeDateFormatter.locale = rmbCurrentLocale()
        relativeDateFormatter.doesRelativeDateFormatting = true

        return relativeDateFormatter.string(from: self)
    }

    func dateComponents(withTime: Bool) -> DateComponents {
        var components: Set<Calendar.Component> = [.calendar, .era, .year, .month, .day]
        if withTime {
            components.formUnion([.timeZone, .hour, .minute, .second])
        }
        return Calendar.current.dateComponents(components, from: self)
    }
}
