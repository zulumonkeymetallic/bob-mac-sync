import Foundation
import EventKit

#if canImport(FirebaseFirestore) && canImport(FirebaseAuth)
import FirebaseFirestore
import FirebaseAuth
#if canImport(FirebaseFunctions)
import FirebaseFunctions
#endif

struct FbTask {
    let id: String
    let title: String
    let dueDate: Double?
    let createdAt: Date?
    let completedAt: Date?
    let deleteAfter: Date?
    let reminderId: String?
    let iosReminderId: String?
    let status: Any?
    let storyId: String?
    let goalId: String?
    let reference: String?
    let sourceRef: String?
    let externalId: String?
    let updatedAt: Date?
    let serverUpdatedAt: Date?
    let reminderListId: String?
    let reminderListName: String?
    let tags: [String]
    // Optional flags/fields used for sync behavior
    let convertedToStoryId: String?
    let deletedFlag: Any?
    let reminderSyncDirective: String?
    let priority: Int?
}

// swiftlint:disable cyclomatic_complexity function_body_length type_body_length file_length large_tuple
actor FirebaseSyncService {
    static let shared = FirebaseSyncService()
    
    private init() {}

    private var reportedPermissionContexts: Set<String> = []
    private var lastThemeMappingRefresh: Date?
    private let themeMappingThrottle: TimeInterval = 300
    private var cachedRemoteThemeNames: [String] = []
    private let syncInstanceId = UserPreferences.shared.syncInstanceId
    // TTL applied to completed items before they are hard-deleted from Firestore (milliseconds)
    private let completedTaskTTL: Double = 30.0 * 24.0 * 60.0 * 60.0 * 1000.0

    struct DedupeDiagnostics {
        let processed: Int
        let groups: Int
        let keyGroups: Int
        let ridGroups: Int
        let error: String?
    }

    private struct StoryContext {
        var storyRef: String?
        var themeName: String?
        var sprintId: String?
        var sprintName: String?
        var goalRef: String?
    }

    private struct GoalContext {
        var ref: String?
        var themeName: String?
    }

    private let isoFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    // Resolve sprint by a due date: find sprint owned by user where startDate <= due <= endDate
    private func resolveSprintForDueDate(due: Date, ownerUid: String, db: Firestore) async -> (id: String, name: String)? {
        func toDate(_ any: Any?) -> Date? {
            if let ts = any as? Timestamp { return ts.dateValue() }
            if let dateValue = any as? Date { return dateValue }
            if let ms = any as? Double { return Date(timeIntervalSince1970: ms / 1_000.0) }
            if let str = any as? String { return isoFormatter.date(from: str) }
            if let num = any as? NSNumber { return Date(timeIntervalSince1970: num.doubleValue / 1_000.0) }
            return nil
        }
        do {
            let query = db.collection("sprints")
                .whereField("ownerUid", isEqualTo: ownerUid)
                .whereField("startDate", isLessThanOrEqualTo: due)
                .order(by: "startDate", descending: true)
                .limit(to: 5)
            let snap = try await query.getDocuments()
            for doc in snap.documents {
                let data = doc.data()
                let end = toDate(data["endDate"]) ?? toDate(data["end"]) ?? toDate(data["end_at"]) ?? Date.distantPast
                if end >= due {
                    let rawName = (data["name"] as? String) ?? (data["title"] as? String)
                    let resolvedName = rawName ?? doc.documentID
                    return (doc.documentID, resolvedName)
                }
            }
        } catch {
            recordPermissionIfNeeded(error, context: "sprints(byDue)")
        }
        return nil
    }

    // Lightweight local triage classification to avoid extra target wiring.
    private enum RmbPersona { case personal, work, unknown }

    private struct TriageResult { let persona: RmbPersona; let confidence: Double; let suggestedTheme: String? }

    private func classifyTriage(
        title: String,
        notes: String?,
        tags: [String]
    ) -> TriageResult {
        // Tag overrides
        let loweredTags = Set(tags.map { $0.lowercased() })
        if loweredTags.contains("work") {
            return TriageResult(persona: .work, confidence: 0.95, suggestedTheme: nil)
        }
        if loweredTags.contains("personal") {
            return TriageResult(persona: .personal, confidence: 0.95, suggestedTheme: nil)
        }

        let text = "\(title)\n\(notes ?? "")".lowercased()
        let all = text + "\n" + tags.joined(separator: " ").lowercased()

        let workKeywords: [(String, Double)] = [
            ("jira", 1.4), ("ticket", 1.2), ("deploy", 1.3), ("production", 1.3), ("prod", 1.1),
            ("oncall", 1.3), ("pagerduty", 1.3), ("client", 1.2), ("customer", 1.1), ("meeting", 1.0),
            ("standup", 1.2), ("sprint", 1.2), ("story", 1.0), ("epic", 1.0), ("bug", 1.0),
            ("pr ", 1.2), ("pull request", 1.2), ("merge", 1.0), ("release", 1.0),
            ("okr", 1.1), ("quarter", 1.0), ("roadmap", 1.0), ("production issue", 1.5),
            ("work", 1.0), ("office", 1.0), ("shift", 1.0), ("invoice", 1.1)
        ]
        let personalKeywords: [(String, Double)] = [
            ("wash", 1.2), ("washing machine", 1.6), ("laundry", 1.3), ("grocer", 1.1), ("shopping", 1.0),
            ("gym", 1.1), ("workout", 1.1), ("dentist", 1.3), ("doctor", 1.2), ("appointment", 1.0),
            ("kids", 1.2), ("school", 1.0), ("family", 1.0), ("home", 1.0), ("garden", 1.0),
            ("rent", 1.0), ("mortgage", 1.0), ("car", 1.0), ("oil change", 1.3), ("pharmacy", 1.1),
            ("vacation", 1.0), ("travel", 1.0), ("birthday", 1.0), ("cook", 1.0), ("meal", 1.0)
        ]
        func score(_ dict: [(String, Double)]) -> Double {
            dict.reduce(0.0) { partial, pair in
                let (needle, weight) = pair
                return partial + (all.contains(needle) ? weight : 0.0)
            }
        }
        let workScore = score(workKeywords)
        let personalScore = score(personalKeywords)
        let total = workScore + personalScore
        if total <= 0 {
            return TriageResult(persona: .unknown, confidence: 0.0, suggestedTheme: nil)
        }
        if workScore >= personalScore {
            return TriageResult(
                persona: .work,
                confidence: workScore / max(total, 1.0),
                suggestedTheme: nil
            )
        }
        // Suggest a rough personal theme for convenience
        let suggested: String? = {
            let pairs: [(needles: [String], theme: String)] = [
                (["wash", "washing machine", "laundry"], "Home"),
                (["dentist", "doctor", "pharmacy", "health"], "Health"),
                (["gym", "workout", "run", "exercise"], "Fitness"),
                (["rent", "mortgage", "invoice", "bill"], "Finance"),
                (["car", "oil change", "tyre", "tire", "garage"], "Car"),
                (["vacation", "trip", "flight", "travel"], "Travel"),
                (["garden", "yard", "lawn"], "Garden"),
                (["grocer", "shopping"], "Shopping")
            ]
            for (needles, theme) in pairs where needles.contains(where: { all.contains($0) }) {
                return theme
            }
            return nil
        }()
        return TriageResult(
            persona: .personal,
            confidence: personalScore / max(total, 1.0),
            suggestedTheme: suggested
        )
    }

    private func inferItemType(calendarTitle: String, tags: [String]) -> String? {
        let lowerTitle = calendarTitle.lowercased()
        let lowerTags = tags.map { $0.lowercased() }
        if lowerTitle.contains("chore") || lowerTags.contains(where: { $0 == "chore" }) {
            return "chore"
        }
        if lowerTitle.contains("routine") || lowerTags.contains(where: { $0 == "routine" }) {
            return "routine"
        }
        return nil
    }

    private func recurrencePayload(for reminder: EKReminder) -> [String: Any]? {
        guard let rules = reminder.recurrenceRules, !rules.isEmpty else { return nil }
        // For now, take the first rule as the primary recurrence
        guard let rule = rules.first else { return nil }

        func freqString(_ frequency: EKRecurrenceFrequency) -> String {
            switch frequency {
            case .daily:
                return "daily"
            case .weekly:
                return "weekly"
            case .monthly:
                return "monthly"
            case .yearly:
                return "yearly"
            @unknown default:
                return "unknown"
            }
        }

        func weekdayAbbrev(_ weekday: Int) -> String {
            // 1=Sunday ... 7=Saturday (EventKit)
            switch weekday {
            case 1:
                return "sun"
            case 2:
                return "mon"
            case 3:
                return "tue"
            case 4:
                return "wed"
            case 5:
                return "thu"
            case 6:
                return "fri"
            case 7:
                return "sat"
            default:
                return String(weekday)
            }
        }

        var payload: [String: Any] = [
            "frequency": freqString(rule.frequency),
            "interval": rule.interval
        ]

        if let days = rule.daysOfTheWeek, !days.isEmpty {
            payload["daysOfWeek"] = days.map { weekdayAbbrev($0.dayOfTheWeek.rawValue) }
        }
        if let dom = rule.daysOfTheMonth, !dom.isEmpty {
            payload["daysOfMonth"] = dom.map { $0.intValue }
        }
        if let moy = rule.monthsOfTheYear, !moy.isEmpty {
            payload["monthsOfYear"] = moy.map { $0.intValue }
        }

        if let end = rule.recurrenceEnd {
            var endPayload: [String: Any] = [:]
            let count = end.occurrenceCount
            if count > 0 { endPayload["count"] = count }
            if let date = end.endDate { endPayload["until"] = isoFormatter.string(from: date) }
            if !endPayload.isEmpty { payload["end"] = endPayload }
        }

        return payload
    }

    private func recordPermissionIfNeeded(_ error: Error, context: String) {
        guard let nsError = error as NSError?, nsError.domain == FirestoreErrorDomain else { return }
        guard let code = FirestoreErrorCode.Code(rawValue: nsError.code), code == .permissionDenied else { return }
        if reportedPermissionContexts.insert(context).inserted {
            SyncLogService.shared.logEvent(
                tag: "firestore",
                level: "ERROR",
                message: "Permission denied for \(context). Bob token may lack read access."
            )
        }
    }

    private func isoNow() -> String { isoFormatter.string(from: Date()) }

    private func isoString(forMillis millis: Double) -> String {
        isoFormatter.string(from: Date(timeIntervalSince1970: millis / 1_000.0))
    }

    private func stringValue(for value: Any?) -> String? {
        guard let value else { return nil }
        if let str = value as? String {
            let trimmed = str.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        }
        if let number = value as? NSNumber {
            return number.stringValue
        }
        if let dict = value as? [String: Any] {
            let keys = ["list", "calendar", "title", "name", "value"]
            for key in keys {
                if let extracted = stringValue(for: dict[key]) {
                    return extracted
                }
            }
        }
        if let array = value as? [Any] {
            for element in array {
                if let extracted = stringValue(for: element) {
                    return extracted
                }
            }
        }
        return nil
    }

    private func nonEmpty(_ value: String?) -> String? {
        guard let value, !value.isEmpty else { return nil }
        return value
    }

    private func makeSprintTag(from sprintName: String?) -> String? {
        guard let name = sprintName?.trimmingCharacters(in: .whitespacesAndNewlines), !name.isEmpty else { return nil }
        let digits = name.compactMap { $0.isNumber ? String($0) : nil }.joined()
        if !digits.isEmpty {
            return "sprint\(digits)"
        }
        let lowered = name.lowercased().replacingOccurrences(of: " ", with: "")
        return lowered.hasPrefix("sprint") ? lowered : "sprint\(lowered)"
    }

    private func parseBobNote(notes: String?) -> (meta: [String: String], userLines: [String]) {
        guard let notes, !notes.isEmpty else { return ([:], []) }
        let lines = notes.components(separatedBy: "\n")
        var linkMeta: [String: String] = [:]

        func captureLink(_ key: String, value: String) {
            guard !value.isEmpty, linkMeta[key] == nil else { return }
            linkMeta[key] = value
        }

        func extractIdentifier(from urlString: String) -> String? {
            let trimmed = urlString.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let components = URLComponents(string: trimmed), !components.path.isEmpty else { return nil }
            let parts = components.path.split(separator: "/")
            guard let last = parts.last else { return nil }
            let candidate = String(last)
            return candidate.isEmpty ? nil : candidate
        }

        func isBobLinkLine(_ line: String) -> Bool {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty {
                return false
            }
            let lowered = trimmed.lowercased()

            let labelMappings: [(prefix: String, key: String)] = [
                ("task:", "taskRef"),
                ("story:", "storyRef"),
                ("goal:", "goalRef"),
                ("sprint:", "sprintId"),
                ("activity:", "taskRef")
            ]
            for mapping in labelMappings {
                if lowered.hasPrefix(mapping.prefix) {
                    let start = trimmed.index(trimmed.startIndex, offsetBy: mapping.prefix.count)
                    let value = trimmed[start...].trimmingCharacters(in: .whitespacesAndNewlines)
                    captureLink(mapping.key, value: value)
                    return true
                }
            }

            if lowered.hasPrefix("http://") || lowered.hasPrefix("https://") {
                if lowered.contains("bob.jc1.tech/tasks/"), let id = extractIdentifier(from: trimmed) {
                    captureLink("taskRef", value: id)
                    return true
                }
                if lowered.contains("bob.jc1.tech/stories/"), let id = extractIdentifier(from: trimmed) {
                    captureLink("storyRef", value: id)
                    return true
                }
                if lowered.contains("bob.jc1.tech/goals/"), let id = extractIdentifier(from: trimmed) {
                    captureLink("goalRef", value: id)
                    return true
                }
                if lowered.contains("bob.jc1.tech/sprints/"), let id = extractIdentifier(from: trimmed) {
                    captureLink("sprintId", value: id)
                    return true
                }
            }

            return false
        }

        guard let metadataStart = lines.lastIndex(where: { $0.hasPrefix("BOB:") }) else {
            var userLines = lines
            userLines.removeAll(where: isBobLinkLine)
            return (linkMeta, userLines)
        }

        var metadataEnd = metadataStart
        var scanIndex = metadataStart + 1
        while scanIndex < lines.count {
            let line = lines[scanIndex]
            if line.hasPrefix("#") || line.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines).isEmpty {
                metadataEnd = scanIndex
                scanIndex += 1
            } else {
                break
            }
        }

        var prefixEnd = metadataStart
        if prefixEnd > 0, lines[prefixEnd - 1] == "-------" {
            prefixEnd -= 1
            if prefixEnd > 0, lines[prefixEnd - 1].trimmingCharacters(in: CharacterSet.whitespacesAndNewlines).isEmpty {
                prefixEnd -= 1
            }
        }

        var userLines: [String] = []
        if prefixEnd > 0 {
            userLines.append(contentsOf: lines[..<prefixEnd])
        }
        if scanIndex < lines.count {
            userLines.append(contentsOf: lines[scanIndex...])
        }

        userLines.removeAll(where: isBobLinkLine)

        let metadataLines = Array(lines[metadataStart...metadataEnd])
        guard let header = metadataLines.first, header.hasPrefix("BOB:") else {
            return (linkMeta, userLines)
        }

        var meta: [String: String] = [:]
        let tokens = header.dropFirst(4).split(separator: " ")
        for token in tokens {
            let pair = token.split(separator: "=", maxSplits: 1)
            guard pair.count == 2 else { continue }
            let key = String(pair[0]).trimmingCharacters(in: .whitespaces)
            let value = String(pair[1])
            if !key.isEmpty, !value.isEmpty { meta[key] = value }
        }

        for line in metadataLines.dropFirst() {
            guard line.hasPrefix("#") else { continue }
            if line.hasPrefix("#sprint: ") {
                meta["sprint"] = String(line.dropFirst("#sprint: ".count))
            } else if line.hasPrefix("#theme: ") {
                meta["theme"] = String(line.dropFirst("#theme: ".count))
            } else if line.hasPrefix("#story: ") {
                meta["storyRef"] = String(line.dropFirst("#story: ".count))
            } else if line.hasPrefix("#task: ") {
                meta["taskRef"] = String(line.dropFirst("#task: ".count))
            } else if line.hasPrefix("#goal: ") {
                meta["goalRef"] = String(line.dropFirst("#goal: ".count))
            } else if line.hasPrefix("#tags: ") {
                meta["tags"] = String(line.dropFirst("#tags: ".count))
            } else if line.hasPrefix("#listId: ") {
                meta["listId"] = String(line.dropFirst("#listId: ".count))
            } else if line.hasPrefix("#list: ") {
                let raw = String(line.dropFirst("#list: ".count))
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                if let urlRange = trimmed.range(of: "http://") ?? trimmed.range(of: "https://") {
                    let name = String(trimmed[..<urlRange.lowerBound]).trimmingCharacters(in: .whitespacesAndNewlines)
                    meta["list"] = name
                } else {
                    meta["list"] = trimmed
                }
            }
        }

        for (key, value) in linkMeta where meta[key] == nil {
            meta[key] = value
        }

        return (meta, userLines)
    }

    private func shouldIncludeBobMetadataInNotes() async -> Bool {
        await MainActor.run {
            UserPreferences.shared.showBobMetadataInNotes
        }
    }

    private func composeBobNote(meta: [String: String], userLines: [String], includeMetadataBlock: Bool = true) -> String {
        var lines: [String] = []
        if !userLines.isEmpty {
            lines.append(contentsOf: userLines)
        }

        func ensureBlankLineBeforeGeneratedContent() {
            guard let last = lines.last else { return }
            if !last.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                lines.append("")
            }
        }

        func appendTaskLink() -> Bool {
            guard let taskRef = meta["taskRef"], !taskRef.isEmpty, let taskURL = taskDeepLink(for: taskRef) else {
                return false
            }
            lines.append(taskURL.absoluteString)
            return true
        }

        if includeMetadataBlock {
            if !lines.isEmpty {
                ensureBlankLineBeforeGeneratedContent()
            }

            var tokens: [String] = []
            let orderedKeys = [
                "taskRef",
                "storyRef",
                "goalRef",
                "status",
                "due",
                "synced",
                "list"
            ]
            for key in orderedKeys {
                if let value = meta[key], !value.isEmpty {
                    tokens.append("\(key)=\(value)")
                }
            }
            let header = tokens.isEmpty ? "BOB:" : "BOB: " + tokens.joined(separator: " ")

            var metadataLines: [String] = [header]
            if let sprint = meta["sprint"], !sprint.isEmpty { metadataLines.append("#sprint: \(sprint)") }
            if let theme = meta["theme"], !theme.isEmpty { metadataLines.append("#theme: \(theme)") }
            if let story = meta["storyRef"], !story.isEmpty { metadataLines.append("#story: \(story)") }
            if let taskRef = meta["taskRef"], !taskRef.isEmpty { metadataLines.append("#task: \(taskRef)") }
            if let goalRef = meta["goalRef"], !goalRef.isEmpty { metadataLines.append("#goal: \(goalRef)") }
            if let tags = meta["tags"], !tags.isEmpty { metadataLines.append("#tags: \(tags)") }
            // Do not include listId in the note metadata lines to keep it human-friendly
            if let listName = meta["list"], !listName.isEmpty { metadataLines.append("#list: \(listName)") }

            _ = appendTaskLink()
            if let story = meta["storyRef"], !story.isEmpty {
                lines.append("https://bob.jc1.tech/stories/\(story)")
            }
            if let goal = meta["goalRef"], !goal.isEmpty {
                lines.append("https://bob.jc1.tech/goals/\(goal)")
            }
            if let sprintId = meta["sprintId"], !sprintId.isEmpty {
                lines.append("https://bob.jc1.tech/sprints/\(sprintId)")
            }
            if let last = lines.last, !last.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                lines.append("")
            }
            lines.append("-------")
            lines.append(contentsOf: metadataLines)
            return lines.joined(separator: "\n")
        } else {
            if !lines.isEmpty {
                ensureBlankLineBeforeGeneratedContent()
            }
            let appendedTask = appendTaskLink()
            if !appendedTask {
                if let story = meta["storyRef"], !story.isEmpty {
                    lines.append("https://bob.jc1.tech/stories/\(story)")
                } else if let goal = meta["goalRef"], !goal.isEmpty {
                    lines.append("https://bob.jc1.tech/goals/\(goal)")
                } else if let sprintId = meta["sprintId"], !sprintId.isEmpty {
                    lines.append("https://bob.jc1.tech/sprints/\(sprintId)")
                }
            }
            return lines.joined(separator: "\n")
        }
    }

    nonisolated private func taskDeepLink(for taskRef: String) -> URL? {
        return URL(string: "https://bob.jc1.tech/tasks/\(taskRef)")
    }

    nonisolated private func activityDeepLink(for taskRef: String) -> URL? {
        // Assuming the task page supports an activity tab
        return URL(string: "https://bob.jc1.tech/tasks/\(taskRef)?tab=activity")
    }

    func refreshThemeMappingFromRemote(force: Bool = false) async {
        let now = Date()
        if !force, let last = lastThemeMappingRefresh, now.timeIntervalSince(last) < themeMappingThrottle {
            return
        }

        guard let db = FirebaseManager.shared.firestore else { return }
        guard let user = Auth.auth().currentUser else { return }

        do {
            let snapshot = try await db.collection("themes").whereField("ownerUid", isEqualTo: user.uid).getDocuments()
            guard !snapshot.isEmpty else {
                lastThemeMappingRefresh = now
                cachedRemoteThemeNames = []
                SyncLogService.shared.logEvent(tag: "themes", level: "INFO", message: "No Bob themes available; retaining existing theme→list mappings")
                return
            }

            var nameToList: [String: String] = [:]
            var allNames: Set<String> = []
            let listKeys = ["reminderList", "reminderListName", "list", "listName", "remindersList", "remindersListName", "calendar", "calendarName"]
            for doc in snapshot.documents {
                let data = doc.data()
                let themeNameRaw = stringValue(for: data["name"]) ?? stringValue(for: data["title"]) ?? stringValue(for: data["reference"]) ?? doc.documentID
                let themeName = themeNameRaw.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
                if themeName.isEmpty { continue }
                allNames.insert(themeName)

                var listName: String?
                for key in listKeys {
                    if let value = data[key] {
                        listName = stringValue(for: value)
                    }
                    if listName != nil { break }
                }
                if listName == nil, let metadata = data["metadata"] as? [String: Any] {
                    for key in listKeys {
                        if let value = metadata[key] {
                            listName = stringValue(for: value)
                        }
                        if listName != nil { break }
                    }
                }
                if let listName, !listName.isEmpty {
                    nameToList[themeName] = listName
                }
            }

            cachedRemoteThemeNames = allNames.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
            var calendars = await MainActor.run { RemindersService.shared.getCalendars() }
            guard !calendars.isEmpty else {
                lastThemeMappingRefresh = now
                SyncLogService.shared.logEvent(tag: "themes", level: "WARN", message: "Skipped remote theme mappings: no Reminders lists available")
                return
            }

            var resolved: [String: String] = [:]
            var unresolved: [String: String] = [:]
            for (theme, listName) in nameToList {
                if let calendar = calendars.first(where: { $0.title.caseInsensitiveCompare(listName) == .orderedSame }) {
                    resolved[theme] = calendar.calendarIdentifier
                    continue
                }

                let created = await MainActor.run { RemindersService.shared.ensureCalendar(named: listName) }
                if let created {
                    resolved[theme] = created.calendarIdentifier
                    calendars.append(created)
                    SyncLogService.shared.logEvent(tag: "themes", level: "INFO", message: "Created Reminders list \(listName) for theme \(theme)")
                } else {
                    unresolved[theme] = listName
                }
            }

            if !resolved.isEmpty {
                let resolvedSnapshot = resolved
                await MainActor.run {
                    var combined = UserPreferences.shared.themeCalendarMap
                    combined.merge(resolvedSnapshot) { _, new in new }
                    UserPreferences.shared.themeCalendarMap = combined
                }
                SyncLogService.shared.logEvent(tag: "themes", level: "INFO", message: "Loaded \(resolved.count) theme→list mappings from Firestore")
            } else {
                SyncLogService.shared.logEvent(tag: "themes", level: "WARN", message: "No remote theme mappings matched local Reminders lists")
            }

            if !unresolved.isEmpty {
                let sample = Array(unresolved.prefix(3)).map { "\($0.key)->\($0.value)" }.joined(separator: ", ")
                SyncLogService.shared.logEvent(tag: "themes", level: "WARN", message: "Unresolved theme mappings: \(unresolved.count). Samples: \(sample)")
            }

            lastThemeMappingRefresh = now
        } catch {
            recordPermissionIfNeeded(error, context: "themes")
            SyncLogService.shared.logError(tag: "themes", error: error)
        }
    }

    struct DeduplicationResult {
        let deleted: Int
        let groups: Int
        let error: String?
    }

    // Full-sweep duplicate cleanup for the current user.
    // Scans all tasks (paged), identifies duplicate groups by several keys,
    // marks their duplicateOf and removes them (hardDelete controls removal).
    func fullSweepAndRemoveDuplicates(hardDelete: Bool) async -> DeduplicationResult {
        guard let db = FirebaseManager.shared.firestore, let user = Auth.auth().currentUser else {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Cannot run full sweep: missing auth or Firestore")
            return DeduplicationResult(deleted: 0, groups: 0, error: "Not authenticated or Firebase not configured")
        }
        do {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "Starting full-sweep duplicate cleanup (hardDelete=\(hardDelete)) for uid=\(user.uid)")

            var allDocs: [DocumentSnapshot] = []
            var lastDoc: DocumentSnapshot? = nil
            while true {
                var query = db.collection("tasks")
                    .whereField("ownerUid", isEqualTo: user.uid)
                    .order(by: FieldPath.documentID())
                    .limit(to: 1000)
                if let lastDoc {
                    query = query.start(afterDocument: lastDoc)
                }
                let snap = try await query.getDocuments()
                if snap.documents.isEmpty { break }
                allDocs.append(contentsOf: snap.documents)
                lastDoc = snap.documents.last
            }

            let tasks: [FbTask] = allDocs.compactMap(toTask)
            SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "Full sweep fetched tasks=\(tasks.count)")

            struct DupInfo { let task: FbTask; let key: String; let reason: String; let survivorId: String }
            var dupMap: [String: DupInfo] = [:] // duplicateId -> info

            func pickDuplicates(groups: [String: [FbTask]], reason: String) {
                for (key, group) in groups where group.count > 1 {
                    let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                    guard let survivor = sorted.first else { continue }
                    for duplicate in sorted.dropFirst() {
                        if dupMap[duplicate.id] == nil {
                            dupMap[duplicate.id] = DupInfo(task: duplicate, key: key, reason: reason, survivorId: survivor.id)
                        }
                    }
                }
            }

            // Group by keys we consider authoritative for duplicates
            var byReminder: [String: [FbTask]] = [:]
            var byRef: [String: [FbTask]] = [:]
            var bySourceRef: [String: [FbTask]] = [:]
            var byIos: [String: [FbTask]] = [:]
            var byExternal: [String: [FbTask]] = [:]
            for task in tasks {
                if let rid = task.reminderId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !rid.isEmpty {
                    byReminder[rid, default: []].append(task)
                }
                if let referenceKey = task.reference?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !referenceKey.isEmpty {
                    byRef[referenceKey, default: []].append(task)
                }
                if let sourceKey = task.sourceRef?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !sourceKey.isEmpty {
                    bySourceRef[sourceKey, default: []].append(task)
                }
                if let iosKey = task.iosReminderId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !iosKey.isEmpty {
                    byIos[iosKey, default: []].append(task)
                }
                if let externalKey = task.externalId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !externalKey.isEmpty {
                    byExternal[externalKey, default: []].append(task)
                }
            }

            pickDuplicates(groups: byReminder, reason: "duplicateReminderId")
            pickDuplicates(groups: byRef, reason: "duplicateTaskRef")
            pickDuplicates(groups: bySourceRef, reason: "duplicateSourceRef")
            pickDuplicates(groups: byIos, reason: "duplicateIosReminderId")
            pickDuplicates(groups: byExternal, reason: "duplicateExternalId")

            let duplicates = Array(dupMap.values)
            if duplicates.isEmpty {
                SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "Full sweep: no duplicates identified")
                return DeduplicationResult(deleted: 0, groups: 0, error: nil)
            }

            // Write updates/removals in batches of ~400 to avoid request size limits
            var deleted = 0
            var groups = Set(duplicates.map { $0.survivorId }).count
            let chunkSize = 400
            for chunk in stride(from: 0, to: duplicates.count, by: chunkSize) {
                let end = min(chunk + chunkSize, duplicates.count)
                let slice = duplicates[chunk..<end]
                let batch = db.batch()
                for info in slice {
                    let ref = db.collection("tasks").document(info.task.id)
                    if hardDelete {
                        batch.deleteDocument(ref)
                        deleted += 1
                    } else {
                        var payload: [String: Any] = [
                            "status": 2,
                            "updatedAt": FieldValue.serverTimestamp(),
                            "duplicateOf": info.survivorId,
                            "duplicateKey": info.key
                        ]
                        if let rid = info.task.reminderId, !rid.isEmpty { payload["reminderId"] = rid }
                        let nowMs = Date().timeIntervalSince1970 * 1000.0
                        payload["completedAt"] = nowMs
                        payload["deleteAfter"] = nowMs + completedTaskTTL
                        batch.setData(payload, forDocument: ref, merge: true)
                        deleted += 1
                    }
                }
                try await batch.commit()
            }

            // Activity log for reporting
            do {
                try await db.collection("activity_stream").addDocument(data: [
                    "activityType": "deduplicate_tasks_full_sweep",
                    "ownerUid": user.uid,
                    "userId": user.uid,
                    "actor": "MacApp",
                    "createdAt": FieldValue.serverTimestamp(),
                    "updatedAt": FieldValue.serverTimestamp(),
                    "metadata": [
                        "hardDelete": hardDelete,
                        "deleted": deleted,
                        "groups": groups
                    ]
                ])
            } catch {
                SyncLogService.shared.logEvent(tag: "activity", level: "ERROR", message: "Failed to write full-sweep dedupe activity: \(error.localizedDescription)")
            }

            SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "Full sweep: deleted=\(deleted) groups=\(groups) hardDelete=\(hardDelete)")
            return DeduplicationResult(deleted: deleted, groups: groups, error: nil)
        } catch {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Full sweep failed: \(error.localizedDescription)")
            return DeduplicationResult(deleted: 0, groups: 0, error: error.localizedDescription)
        }
    }

    // Remove all duplicate tasks for the current user. If hardDelete is true, documents are deleted; otherwise they are marked deleted.
    func deleteAllDuplicates(hardDelete: Bool) async -> DeduplicationResult {
        guard let db = FirebaseManager.shared.firestore, let user = Auth.auth().currentUser else {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Cannot start dedupe: missing auth or Firestore")
            return DeduplicationResult(deleted: 0, groups: 0, error: "Not authenticated or Firebase not configured")
        }
        do {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Starting deleteAllDuplicates(hardDelete=\(hardDelete)) for uid=\(user.uid)")
            let snapshot = try await db.collection("tasks").whereField("ownerUid", isEqualTo: user.uid).getDocuments()
            let allDocs = snapshot.documents
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Fetched tasks: \(allDocs.count)")
            let duplicates = allDocs.filter { ($0.data()["duplicateOf"] as? String)?.isEmpty == false }
            if duplicates.isEmpty {
                // Provide diagnostics to explain "not starting"
                SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "No duplicates marked (duplicateOf) – running diagnostics")
                // Group by duplicateKey and reminderId to show potential groups
                var byKey: [String: Int] = [:]
                var byRid: [String: Int] = [:]
                for doc in allDocs {
                    let data = doc.data()
                    if let key = (data["duplicateKey"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines), !key.isEmpty {
                        byKey[key, default: 0] += 1
                    }
                    if let rid = (data["reminderId"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines), !rid.isEmpty {
                        let norm = "reminder:\(rid.lowercased())"
                        byRid[norm, default: 0] += 1
                    }
                }
                let keyGroups = byKey.filter { $0.value > 1 }.count
                let ridGroups = byRid.filter { $0.value > 1 }.count
                let sampleKeys = Array(byKey.filter { $0.value > 1 }.keys.prefix(3))
                let sampleRids = Array(byRid.filter { $0.value > 1 }.keys.prefix(3))
                SyncLogService.shared.logEvent(
                    tag: "dedupe",
                    level: "DEBUG",
                    message: "Diagnostics: duplicateKey groups=\(keyGroups) sample=\(sampleKeys.joined(separator: ", "))"
                )
                SyncLogService.shared.logEvent(
                    tag: "dedupe",
                    level: "DEBUG",
                    message: "Diagnostics: reminderId groups=\(ridGroups) sample=\(sampleRids.joined(separator: ", "))"
                )
                return DeduplicationResult(deleted: 0, groups: 0, error: nil)
            }

            // Group by their canonical id and emit verbose logs per group
            var groupsByKept: [String: [String]] = [:]
            for doc in duplicates {
                if let kept = doc.data()["duplicateOf"] as? String, !kept.isEmpty {
                    groupsByKept[kept, default: []].append(doc.documentID)
                }
            }
            let groupCount = groupsByKept.keys.count
            SyncLogService.shared.logEvent(
                tag: "dedupe",
                level: "DEBUG",
                message: "Groups identified by duplicateOf: \(groupCount)"
            )
            for (keptId, removedIds) in groupsByKept.sorted(by: { $0.key < $1.key }) {
                SyncLogService.shared.logEvent(
                    tag: "dedupe",
                    level: "DEBUG",
                    message: "Group kept=\(keptId) removed=\(removedIds.count)"
                )
                // Log details for each duplicate in this group
                for removedId in removedIds {
                    if let snap = duplicates.first(where: { $0.documentID == removedId }) {
                        let data = snap.data()
                        let title = (data["title"] as? String) ?? "(untitled)"
                        let ref = (data["ref"] as? String) ?? (data["reference"] as? String) ?? ""
                        let rid = (data["reminderId"] as? String) ?? ""
                        let key = (data["duplicateKey"] as? String) ?? ""
                        let line = [
                            "  dupId=\(removedId)",
                            ref.isEmpty ? nil : "ref=\(ref)",
                            rid.isEmpty ? nil : "rid=\(rid)",
                            key.isEmpty ? nil : "key=\(key)",
                            "title=\(title)"
                        ].compactMap { $0 }.joined(separator: " ")
                        SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: line)
                    }
                }
            }

            // Perform deletion or marking
            let bulk = db.batch()
            for doc in duplicates {
                let ref = db.collection("tasks").document(doc.documentID)
                if hardDelete {
                    bulk.deleteDocument(ref)
                } else {
                    var payload: [String: Any] = [
                        "deleted": true,
                        "status": 2,
                        "updatedAt": FieldValue.serverTimestamp(),
                        "serverUpdatedAt": FieldValue.serverTimestamp(),
                        "reminderSyncDirective": "complete"
                    ]
                    let nowMs = Date().timeIntervalSince1970 * 1000.0
                    payload["completedAt"] = nowMs
                    payload["deleteAfter"] = nowMs + completedTaskTTL
                    bulk.setData(payload, forDocument: ref, merge: true)
                }
            }
            try await bulk.commit()

            // Emit activity so data-quality email counts these deletions
            let groupsPayload: [[String: Any]] = groupsByKept.map { kept, removed in
                ["kept": kept, "removed": removed, "keys": ["manual_cleanup"]]
            }
            do {
                try await db.collection("activity_stream").addDocument(data: [
                    "activityType": "deduplicate_tasks",
                    "ownerUid": user.uid,
                    "userId": user.uid,
                    "actor": "MacApp",
                    "createdAt": FieldValue.serverTimestamp(),
                    "updatedAt": FieldValue.serverTimestamp(),
                    "metadata": [
                        "groups": groupsPayload,
                        "hardDelete": hardDelete,
                        "diagnostics": [
                            "requestedBy": "MacApp",
                            "markedDuplicates": duplicates.count,
                            "totalTasks": allDocs.count
                        ]
                    ]
                ])
            } catch {
                SyncLogService.shared.logEvent(tag: "activity", level: "ERROR", message: "Failed to write dedupe cleanup activity: \(error.localizedDescription)")
            }

            SyncLogService.shared.logEvent(
                tag: "dedupe",
                level: "INFO",
                message: "Deleted \(duplicates.count) duplicates across \(groupsByKept.keys.count) groups (hardDelete=\(hardDelete))"
            )
            return DeduplicationResult(deleted: duplicates.count, groups: groupsByKept.keys.count, error: nil)
        } catch {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Delete all duplicates failed: \(error.localizedDescription)")
            return DeduplicationResult(deleted: 0, groups: 0, error: error.localizedDescription)
        }
    }

    // Diagnostic-only duplicate analysis: no writes; logs detailed reasoning
    func diagnoseDuplicates() async -> DedupeDiagnostics {
        guard let db = FirebaseManager.shared.firestore, let user = Auth.auth().currentUser else {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Cannot diagnose duplicates: missing auth or Firestore")
            return DedupeDiagnostics(processed: 0, groups: 0, keyGroups: 0, ridGroups: 0, error: "Not authenticated or Firebase not configured")
        }
        do {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Diagnose duplicates starting for uid=\(user.uid)")
            let snap = try await db.collection("tasks").whereField("ownerUid", isEqualTo: user.uid).getDocuments()
            let docs = snap.documents
            var byDuplicateOf = 0
            var byKey: [String: Int] = [:]
            var byRid: [String: Int] = [:]
            for doc in docs {
                let data = doc.data()
                if let dupOf = data["duplicateOf"] as? String, !dupOf.isEmpty { byDuplicateOf += 1 }
                if let key = (data["duplicateKey"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines), !key.isEmpty {
                    byKey[key, default: 0] += 1
                }
                if let rid = (data["reminderId"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines), !rid.isEmpty {
                    let norm = "reminder:\(rid.lowercased())"
                    byRid[norm, default: 0] += 1
                }
            }
            let keyGroups = byKey.filter { $0.value > 1 }.count
            let ridGroups = byRid.filter { $0.value > 1 }.count
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Docs=\(docs.count) duplicateOf-marked=\(byDuplicateOf)")
            if keyGroups > 0 {
                let groups = byKey.filter { $0.value > 1 }
                for (key, count) in groups.sorted(by: { $0.key < $1.key }) {
                    SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "duplicateKey=\(key) count=\(count)")
                    // Print members for this key
                    for doc in docs where ((doc.data()["duplicateKey"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)) == key {
                        let data = doc.data()
                        let id = doc.documentID
                        let title = (data["title"] as? String) ?? "(untitled)"
                        let ref = (data["ref"] as? String) ?? (data["reference"] as? String) ?? ""
                        let rid = (data["reminderId"] as? String) ?? ""
                        let dupOf = (data["duplicateOf"] as? String) ?? ""
                        let line = [
                            "  id=\(id)",
                            ref.isEmpty ? nil : "ref=\(ref)",
                            rid.isEmpty ? nil : "rid=\(rid)",
                            dupOf.isEmpty ? nil : "duplicateOf=\(dupOf)",
                            "title=\(title)"
                        ].compactMap { $0 }.joined(separator: " ")
                        SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: line)
                    }
                }
            } else {
                SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "No duplicateKey groups found")
            }
            if ridGroups > 0 {
                let groups = byRid.filter { $0.value > 1 }
                for (key, count) in groups.sorted(by: { $0.key < $1.key }) {
                    SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "reminderKey=\(key) count=\(count)")
                    let rid = key.replacingOccurrences(of: "reminder:", with: "")
                    for doc in docs where ((doc.data()["reminderId"] as? String)?.lowercased()) == rid {
                        let data = doc.data()
                        let id = doc.documentID
                        let title = (data["title"] as? String) ?? "(untitled)"
                        let ref = (data["ref"] as? String) ?? (data["reference"] as? String) ?? ""
                        let dupOf = (data["duplicateOf"] as? String) ?? ""
                        let line = [
                            "  id=\(id)",
                            ref.isEmpty ? nil : "ref=\(ref)",
                            dupOf.isEmpty ? nil : "duplicateOf=\(dupOf)",
                            "title=\(title)"
                        ].compactMap { $0 }.joined(separator: " ")
                        SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: line)
                    }
                }
            } else {
                SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "No reminderId groups found")
            }
            // Also mirror a diagnostic activity so server reports can include this run
            #if canImport(FirebaseFirestore) && canImport(FirebaseAuth)
            do {
                let activity: [String: Any] = [
                    "activityType": "deduplicate_diagnostics",
                    "ownerUid": user.uid,
                    "userId": user.uid,
                    "actor": "MacApp",
                    "createdAt": FieldValue.serverTimestamp(),
                    "updatedAt": FieldValue.serverTimestamp(),
                    "metadata": [
                        "total": docs.count,
                        "duplicateOfMarked": byDuplicateOf,
                        "duplicateKeyGroups": keyGroups,
                        "reminderIdGroups": ridGroups
                    ]
                ]
                try await db.collection("activity_stream").addDocument(data: activity)
            } catch {
                SyncLogService.shared.logEvent(tag: "activity", level: "ERROR", message: "Failed to write diagnostic activity: \(error.localizedDescription)")
            }
            #endif
            return DedupeDiagnostics(processed: docs.count, groups: max(keyGroups, ridGroups), keyGroups: keyGroups, ridGroups: ridGroups, error: nil)
        } catch {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Diagnose failed: \(error.localizedDescription)")
            return DedupeDiagnostics(processed: 0, groups: 0, keyGroups: 0, ridGroups: 0, error: error.localizedDescription)
        }
    }

    func themeNames() -> [String] {
        cachedRemoteThemeNames
    }

    private func statusString(for statusValue: Any?) -> String {
        isDone(statusValue) ? "complete" : "open"
    }

    private func parseISO(_ string: String?) -> Date? {
        guard let string else { return nil }
        return isoFormatter.date(from: string)
    }

    // Hardened title normalizer (parity with server):
    // - Lowercase
    // - Strip diacritics and width variants
    // - Remove zero-width/formatting characters
    // - Remove URLs and non-alphanumerics to spaces
    // - Collapse whitespace
    private func normalizeTitleLocal(_ text: String) -> String {
        var normalizedText = text.folding(options: [.diacriticInsensitive, .widthInsensitive, .caseInsensitive], locale: .current).lowercased()
        // Remove zero-width characters and common format marks
        let patterns = [
            "\u{200B}", "\u{200C}", "\u{200D}", // ZWSP/ZWNJ/ZWJ
            "\u{FEFF}", // BOM
            "\u{00AD}", // soft hyphen
            "\u{061C}", // ALM
            "[\u{2060}-\u{206F}]", // general format controls
            "\u{FE0E}", "\u{FE0F}" // variation selectors
        ]
        for pattern in patterns {
            normalizedText = normalizedText.replacingOccurrences(of: pattern, with: "", options: .regularExpression)
        }
        // Strip URLs
        normalizedText = normalizedText.replacingOccurrences(of: "https?://\\S+", with: " ", options: .regularExpression)
        normalizedText = normalizedText.replacingOccurrences(of: "www\\.[^\\s]+", with: " ", options: .regularExpression)
        // Replace non-alphanumerics with space
        normalizedText = normalizedText.replacingOccurrences(of: "[^a-z0-9]", with: " ", options: .regularExpression)
        // Collapse whitespace
        normalizedText = normalizedText.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression).trimmingCharacters(in: .whitespacesAndNewlines)
        return normalizedText
    }

    private func awaitServerRef(for document: DocumentReference, ownerUid: String, timeout: TimeInterval = 5.0) async -> String? {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            do {
                let snap = try await document.getDocument()
                if let refValue = snap.data()?["ref"] as? String,
                   !refValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return refValue
                }
            } catch {
                SyncLogService.shared.logEvent(
                    tag: "sync",
                    level: "WARN",
                    message: "Await ref read failed for \(document.documentID): \(error.localizedDescription)"
                )
            }
            try? await Task.sleep(nanoseconds: 250_000_000) // 250ms
        }
        return nil
    }

    private func fetchTaskByReference(_ refValue: String, ownerUid: String, db: Firestore) async -> FbTask? {
        let normalized = refValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }
        do {
            let byRefSnapshot = try await db.collection("tasks")
                .whereField("ownerUid", isEqualTo: ownerUid)
                .whereField("ref", isEqualTo: normalized)
                .limit(to: 1)
                .getDocuments()
            if let doc = byRefSnapshot.documents.first, let task = toTask(doc) {
                return task
            }
            let directDoc = try await db.collection("tasks").document(normalized).getDocument()
            if directDoc.exists, let task = toTask(directDoc) {
                return task
            }
        } catch {
            let nsError = error as NSError
            if nsError.domain == FirestoreErrorDomain,
               nsError.code == FirestoreErrorCode.permissionDenied.rawValue {
                SyncLogService.shared.logEvent(
                    tag: "sync",
                    level: "WARN",
                    message: "fetchTaskByReference permission denied for \(normalized): \(error.localizedDescription)"
                )
                recordPermissionIfNeeded(error, context: "tasks(refLookup)")
            } else {
                SyncLogService.shared.logEvent(
                    tag: "sync",
                    level: "WARN",
                    message: "fetchTaskByReference failed for \(normalized): \(error.localizedDescription)"
                )
            }
        }
        return nil
    }

    private func importReminderToBob(reminder: EKReminder, ownerUid: String, db: Firestore, dryRun: Bool, batch: WriteBatch? = nil) async throws -> FbTask {
        let title = await MainActor.run { reminder.title ?? "" }
        let reminderIdentifier = await MainActor.run { reminder.calendarItemIdentifier }
        let isCompleted = await MainActor.run { reminder.isCompleted }
        let dueDate = await MainActor.run { reminder.dueDateComponents?.date }
        let dueMillis = dueDate.map { $0.timeIntervalSince1970 * 1_000.0 }
        let calendarIdentifier = await MainActor.run { reminder.calendar.calendarIdentifier }
        let calendarTitle = await MainActor.run { reminder.calendar.title }
        let reminderTags: [String] = await MainActor.run {
            reminder.rmbCurrentTags().compactMap { tag in
                let trimmed = tag.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }
        }
        let inferredType = inferItemType(calendarTitle: calendarTitle, tags: reminderTags)
        let recurrence = recurrencePayload(for: reminder)
        let now = Date()
        // Batch writes are intentionally bypassed for new tasks so we can read the server-assigned ref immediately.
        _ = batch

        // Create a document reference and a local human-readable ref (TK-XXXXX)
        let doc = db.collection("tasks").document()
        let localRef = makeLocalTaskRef()

        var data: [String: Any] = [
            "ownerUid": ownerUid,
            "title": title,
            "status": isCompleted ? 2 : 0,
            "reminderId": reminderIdentifier,
            "createdAt": FieldValue.serverTimestamp(),
            "updatedAt": FieldValue.serverTimestamp(),
            // Align with Bob schema and filters
            "persona": "personal",
            "source": "MacApp",
            "createdBy": "mac_app",
            "sourceClient": "MacApp",
            "serverUpdatedAt": FieldValue.serverTimestamp(),
            "reminderListId": calendarIdentifier,
            "reminderListName": calendarTitle,
            "tags": reminderTags,
            "ref": localRef,
            "reference": localRef,
            "code": localRef
        ]
        
        // Priority Mapping (Apple 0-9 -> BOB 1-5)
        let applePriority = await MainActor.run { reminder.priority }
        let bobPriority: Int
        switch applePriority {
        case 1...4: bobPriority = 1 // High (!!!)
        case 5: bobPriority = 2 // Medium (!!) -> Map to P2 (Medium-High) to align with reverse mapping? 
                                // User said P1->!!!, P2->!!, P3->!. 
                                // So Apple !! (5) should map to P2.
        case 6...9: bobPriority = 3 // Low (!) -> Map to P3
        default: bobPriority = 4 // None -> P4 (Low-ish)
        }
        data["priority"] = bobPriority

        // Persist duplicateKey to align with backend duplicate detection
        let duplicateKey = "reminder:\(reminderIdentifier.lowercased())"
        data["duplicateKey"] = duplicateKey
        if isCompleted {
            // Attach client-calculated lifecycle fields so TTL cleanup can proceed even before triggers run
            let nowMs = now.timeIntervalSince1970 * 1000.0
            data["completedAt"] = nowMs
            data["deleteAfter"] = nowMs + completedTaskTTL // +30 days
        }
        // Sprint by due-date (fallback when no story)
        var resolvedSprint: (id: String, name: String)? = nil
        if let due = dueDate {
            if let sprint = await resolveSprintForDueDate(due: due, ownerUid: ownerUid, db: db) {
                data["sprintId"] = sprint.id
                resolvedSprint = sprint
            }
        }
        if let dueMillis { data["dueDate"] = dueMillis }
        if let inferredType { data["type"] = inferredType }
        if let recurrence { data["recurrence"] = recurrence }
        // Convenience fields for simple querying
        if let freq = recurrence?["frequency"] as? String { data["repeatFrequency"] = freq }
        if let interval = recurrence?["interval"] { data["repeatInterval"] = interval }
        if let days = recurrence?["daysOfWeek"] { data["repeatDaysOfWeek"] = days }

        if !dryRun {
            try await doc.setData(data)
            let titleForLog = title
                .replacingOccurrences(of: "\n", with: " ")
                .replacingOccurrences(of: "\r", with: " ")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let logTitle = titleForLog.isEmpty ? "<empty>" : titleForLog
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Created task docId=\(doc.documentID) ref=\(localRef) title=\"\(logTitle)\""
            )
        }
        let resolvedRef = localRef

        let existingNotes = await MainActor.run { reminder.notes }
        let (_, userLines) = parseBobNote(notes: existingNotes)

        var meta: [String: String] = [
            "status": isCompleted ? "complete" : "open",
            "synced": isoNow()
        ]
        // Enrich BOB note for better traceability/dedup hints
        meta["taskRef"] = resolvedRef
        if let dueMillis { meta["due"] = isoString(forMillis: dueMillis) }
        meta["list"] = calendarTitle
        meta["listId"] = calendarIdentifier
        if let sprintName = resolvedSprint?.name { meta["sprint"] = sprintName }
        if let sprintId = resolvedSprint?.id { meta["sprintId"] = sprintId }
        // Build enriched tag list for note
        var tagSet = Set(reminderTags)
        if let sprintName = resolvedSprint?.name, let sprintTag = makeSprintTag(from: sprintName) { tagSet.insert(sprintTag) }
        let tagList = Array(tagSet).sorted()
        if !tagList.isEmpty { meta["tags"] = tagList.joined(separator: ", ") }

        let includeMetadata = await shouldIncludeBobMetadataInNotes()
        let newNotes = composeBobNote(meta: meta, userLines: userLines, includeMetadataBlock: includeMetadata)
        let deepLinkURL = taskDeepLink(for: meta["taskRef"] ?? resolvedRef)
        if !dryRun {
            await MainActor.run {
                reminder.notes = newNotes
                if let url = deepLinkURL {
                    reminder.url = url
                }
                RemindersService.shared.save(reminder: reminder)
            }
        }

        var importMeta: [String: Any] = [
            "title": title,
            "status": isCompleted ? "complete" : "open",
            "calendar": calendarTitle
        ]
        if let dueMillis { importMeta["due"] = isoString(forMillis: dueMillis) }
        if !tagList.isEmpty { importMeta["tags"] = tagList }
        if let sprintName = resolvedSprint?.name { importMeta["sprint"] = sprintName }
        if let inferredType { importMeta["type"] = inferredType }
        if let recurrence { importMeta["recurrence"] = recurrence }
        importMeta["taskRef"] = resolvedRef
        SyncLogService.shared.logSyncDetail(
            direction: .toBob,
            action: "importReminder",
            taskId: doc.documentID,
            storyId: nil,
            metadata: importMeta,
            dryRun: dryRun
        )

        return FbTask(
            id: doc.documentID,
            title: title,
            dueDate: dueMillis,
            createdAt: Date(),
            completedAt: isCompleted ? now : nil,
            deleteAfter: isCompleted ? now.addingTimeInterval(completedTaskTTL / 1000.0) : nil,
            reminderId: reminderIdentifier,
            iosReminderId: nil,
            status: isCompleted ? 2 : 0,
            storyId: nil,
            goalId: nil,
            reference: resolvedRef,
            sourceRef: nil,
            externalId: nil,
            updatedAt: Date(),
            serverUpdatedAt: nil,
            reminderListId: calendarIdentifier,
            reminderListName: calendarTitle,
            tags: reminderTags,
            convertedToStoryId: nil,
            deletedFlag: nil,
            reminderSyncDirective: nil,
            priority: bobPriority
        )
    }

    private func toTask(_ doc: DocumentSnapshot) -> FbTask? {
        let data = doc.data() ?? [:]
        let updatedAt: Date?
        if let updatedTS = data["updatedAt"] as? Timestamp {
            updatedAt = updatedTS.dateValue()
        } else if let date = data["updatedAt"] as? Date {
            updatedAt = date
        } else {
            updatedAt = nil
        }

        let createdAt: Date? = {
            if let ts = data["createdAt"] as? Timestamp { return ts.dateValue() }
            if let dateValue = data["createdAt"] as? Date { return dateValue }
            if let numberValue = data["createdAt"] as? NSNumber { return Date(timeIntervalSince1970: numberValue.doubleValue / 1_000.0) }
            if let stringValue = data["createdAt"] as? String, let parsedDate = isoFormatter.date(from: stringValue) { return parsedDate }
            return nil
        }()
        let serverUpdatedAt: Date? = {
            if let ts = data["serverUpdatedAt"] as? Timestamp { return ts.dateValue() }
            if let dateValue = data["serverUpdatedAt"] as? Date { return dateValue }
            if let numberValue = data["serverUpdatedAt"] as? NSNumber { return Date(timeIntervalSince1970: numberValue.doubleValue / 1_000.0) }
            if let stringValue = data["serverUpdatedAt"] as? String, let parsedDate = isoFormatter.date(from: stringValue) { return parsedDate }
            return nil
        }()
        let completedAt: Date? = {
            if let ts = data["completedAt"] as? Timestamp { return ts.dateValue() }
            if let dateValue = data["completedAt"] as? Date { return dateValue }
            if let numberValue = data["completedAt"] as? NSNumber { return Date(timeIntervalSince1970: numberValue.doubleValue / 1_000.0) }
            if let stringValue = data["completedAt"] as? String, let parsedDate = isoFormatter.date(from: stringValue) { return parsedDate }
            return nil
        }()
        let deleteAfter: Date? = {
            if let ts = data["deleteAfter"] as? Timestamp { return ts.dateValue() }
            if let dateValue = data["deleteAfter"] as? Date { return dateValue }
            if let numberValue = data["deleteAfter"] as? NSNumber { return Date(timeIntervalSince1970: numberValue.doubleValue / 1_000.0) }
            if let stringValue = data["deleteAfter"] as? String, let parsedDate = isoFormatter.date(from: stringValue) { return parsedDate }
            return nil
        }()

        let rawDueDate = data["dueDate"]
        let dueDate: Double?
        if let number = rawDueDate as? NSNumber {
            dueDate = number.doubleValue
        } else if let doubleValue = rawDueDate as? Double {
            dueDate = doubleValue
        } else if let timestamp = rawDueDate as? Timestamp {
            dueDate = timestamp.dateValue().timeIntervalSince1970 * 1_000.0
        } else if let dateValue = rawDueDate as? Date {
            dueDate = dateValue.timeIntervalSince1970 * 1_000.0
        } else if let stringValue = rawDueDate as? String, let parsed = isoFormatter.date(from: stringValue) {
            dueDate = parsed.timeIntervalSince1970 * 1_000.0
        } else {
            dueDate = nil
        }

        let reminderListId = (data["reminderListId"] as? String)
            ?? (data["remindersListId"] as? String)
            ?? (data["listId"] as? String)
            ?? (data["calendarId"] as? String)

        let reminderListName = (data["reminderListName"] as? String)
            ?? (data["remindersListName"] as? String)
            ?? (data["listName"] as? String)
            ?? (data["calendar"] as? String)

        let rawTags = data["tags"]
        let tags: [String]
        if let array = rawTags as? [String] {
            tags = array.compactMap { tag in
                let trimmed = tag.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }
        } else if let single = rawTags as? String {
            let trimmed = single.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
            tags = trimmed.isEmpty ? [] : [trimmed]
        } else {
            tags = []
        }

        return FbTask(
            id: doc.documentID,
            title: data["title"] as? String ?? "Task",
            dueDate: dueDate,
            createdAt: createdAt,
            completedAt: completedAt,
            deleteAfter: deleteAfter,
            reminderId: data["reminderId"] as? String,
            iosReminderId: data["iosReminderId"] as? String,
            status: data["status"],
            storyId: nonEmpty(data["storyId"] as? String),
            goalId: nonEmpty(data["goalId"] as? String),
            reference: (data["reference"] as? String) ?? (data["ref"] as? String) ?? (data["shortId"] as? String) ?? (data["code"] as? String),
            sourceRef: (data["sourceRef"] as? String) ?? (data["source_reference"] as? String),
            externalId: (data["taskId"] as? String) ?? (data["externalId"] as? String),
            updatedAt: updatedAt,
            serverUpdatedAt: serverUpdatedAt,
            reminderListId: reminderListId,
            reminderListName: reminderListName,
            tags: tags,
            convertedToStoryId: data["convertedToStoryId"] as? String,
            deletedFlag: data["deleted"],
            reminderSyncDirective: data["reminderSyncDirective"] as? String,
            priority: data["priority"] as? Int
        )
    }

    enum SyncMode { case full, delta }

    // Bidirectional sync between Firestore tasks and Apple Reminders.
    // - Creates Reminders for tasks without reminderId
    // - Updates fields both directions (title, due, completion)
    // - Clears mappings for tasks whose reminders are gone
    // - Best-effort delete: if task has deleted flag, remove reminder
    func syncNow(mode: SyncMode = .full, targetCalendar preferredCalendar: EKCalendar?) async -> (created: Int, updated: Int, errors: [String]) {
        guard AppConstants.useNativeReminders else {
            return (0, 0, ["Native Reminders integration disabled"])
        }
        // Preflight: ensure Reminders access is granted
        let hasAccess = await MainActor.run { RemindersService.shared.hasFullRemindersAccess() }
        guard hasAccess else { return (0, 0, ["Reminders access not granted"]) }

        guard let user = Auth.auth().currentUser, let db = FirebaseManager.shared.firestore else {
            return (0, 0, ["Not authenticated or Firebase not configured"])
        }
        let ownerUid = user.uid
        let syncStart = Date()
        var milestones: [(String, Int)] = []
        func mark(_ label: String) {
            let elapsedMs = Int(Date().timeIntervalSince(syncStart) * 1000)
            milestones.append((label, elapsedMs))
            SyncLogService.shared.logEvent(tag: "sync", level: "DEBUG", message: "Milestone \(label) at \(elapsedMs)ms")
        }
        SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: "Preflight OK; starting phases (no theme mapping)")
        // Connectivity probe + verbose logging
        let probeId = syncInstanceId.isEmpty ? UUID().uuidString : "sync-probe-\(syncInstanceId)"
        do {
            try await db.collection("diagnostics").document(probeId).setData([
                "ownerUid": user.uid,
                "actor": "MacApp",
                "createdAt": FieldValue.serverTimestamp(),
                "mode": "\(mode)",
                "targetCalendar": preferredCalendar?.calendarIdentifier ?? "none"
            ])
            let probeSnap = try await db.collection("tasks")
                .whereField("ownerUid", isEqualTo: user.uid)
                .order(by: FieldPath.documentID())
                .limit(to: 5)
                .getDocuments()
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "DEBUG",
                message: "Probe write/read succeeded; sample tasks fetched=\(probeSnap.documents.count)"
            )
            mark("probe")
        } catch {
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "ERROR",
                message: "Probe write/read failed: \(error.localizedDescription)"
            )
            // Continue; sync may still proceed
        }
        let dryRun = await MainActor.run { UserPreferences.shared.syncDryRun }
        var created = 0
        var updated = 0
        var errors: [String] = []
        var repairs = 0
        var mergesToBob = 0
        var updatesFromBob = 0
        var createdLinkedStories = 0
        var createdWithTheme = 0
        // Diagnostics accumulators
        struct SkippedItem { let title: String; let reason: String; let calendar: String; let tags: [String]; let due: Date? }
        var skippedImports: [SkippedItem] = []   // Reminders not imported to Bob
        var skippedMerges: [SkippedItem] = []    // Reminders skipped when pushing/pulling changes due to rules
        // For cross-phase diagnostics outside the task query 'do' block
        var fetchedTasksForDiff: [FbTask] = []
        var firestoreOnlyCount = 0
        var macOnlyCount = 0

            var storyContextCache: [String: StoryContext] = [:]
            var sprintCache: [String: String?] = [:]
            var goalContextCache: [String: GoalContext] = [:]

            // Prefetch story/goal contexts in chunked queries to reduce per-doc reads
            func prefetchContexts(for tasks: [FbTask]) async {
                let storyIds = Array(Set(tasks.compactMap { $0.storyId })).filter { !$0.isEmpty }
                let goalIds = Array(Set(tasks.compactMap { $0.goalId })).filter { !$0.isEmpty }
                let chunkSize = 10
                var sprintIds: Set<String> = []
                // Goals
                if !goalIds.isEmpty {
                    for start in stride(from: 0, to: goalIds.count, by: chunkSize) {
                        let end = min(start + chunkSize, goalIds.count)
                        let chunk = Array(goalIds[start..<end])
                        do {
                            let snap = try await db.collection("goals").whereField(FieldPath.documentID(), in: chunk).getDocuments()
                            for doc in snap.documents {
                                let data = doc.data()
                                var ctx = GoalContext()
                                ctx.ref = (data["reference"] as? String) ?? (data["ref"] as? String) ?? (data["code"] as? String) ?? doc.documentID
                                ctx.themeName = (data["themeId"] as? String) ?? (data["theme"] as? String)
                                goalContextCache[doc.documentID] = ctx
                            }
                        } catch { /* ignore prefetch errors; fall back to on-demand */ }
                    }
                }
                // Stories
                if !storyIds.isEmpty {
                    for start in stride(from: 0, to: storyIds.count, by: chunkSize) {
                        let end = min(start + chunkSize, storyIds.count)
                        let chunk = Array(storyIds[start..<end])
                        do {
                            let snap = try await db.collection("stories").whereField(FieldPath.documentID(), in: chunk).getDocuments()
                            for doc in snap.documents {
                                let data = doc.data()
                                var ctx = StoryContext()
                                ctx.storyRef = (data["reference"] as? String) ?? (data["ref"] as? String) ?? (data["shortId"] as? String) ?? (data["code"] as? String) ?? doc.documentID
                                ctx.themeName = (data["themeId"] as? String) ?? (data["theme"] as? String)
                                if let sprintId = data["sprintId"] as? String { ctx.sprintId = sprintId; sprintIds.insert(sprintId) }
                                storyContextCache[doc.documentID] = ctx
                            }
                        } catch { /* ignore prefetch errors; fall back to on-demand */ }
                    }
                }
                // Sprints (prefetch names)
                if !sprintIds.isEmpty {
                    let allSprintIds = Array(sprintIds)
                    for start in stride(from: 0, to: allSprintIds.count, by: chunkSize) {
                        let end = min(start + chunkSize, allSprintIds.count)
                        let chunk = Array(allSprintIds[start..<end])
                        do {
                            let snap = try await db.collection("sprints").whereField(FieldPath.documentID(), in: chunk).getDocuments()
                            for doc in snap.documents {
                                let data = doc.data()
                                let name = (data["name"] as? String) ?? (data["title"] as? String)
                                sprintCache[doc.documentID] = name
                            }
                        } catch { /* ignore prefetch errors; on-demand fetch will fill */ }
                    }
                }
            }

        func fetchSprintName(_ sprintId: String) async -> String? {
            guard !sprintId.isEmpty else { return nil }
            if let cached = sprintCache[sprintId] {
                return cached ?? nil
            }
            do {
                let sprintSnapshot = try await db.collection("sprints").document(sprintId).getDocument()
                if let sprintData = sprintSnapshot.data() {
                    let name = (sprintData["name"] as? String) ?? (sprintData["title"] as? String)
                    sprintCache[sprintId] = name
                    return name
                }
            } catch {
                recordPermissionIfNeeded(error, context: "sprints/\(sprintId)")
            }
            sprintCache[sprintId] = nil
            return nil
        }

        func fetchGoalContext(_ goalId: String) async -> GoalContext {
            guard !goalId.isEmpty else { return GoalContext() }
            if let cached = goalContextCache[goalId] {
                return cached
            }
            var ctx = GoalContext()
            do {
                let goalSnapshot = try await db.collection("goals").document(goalId).getDocument()
                if let goalData = goalSnapshot.data() {
                    ctx.ref = (goalData["reference"] as? String) ?? (goalData["ref"] as? String) ?? (goalData["code"] as? String)
                    ctx.themeName = (goalData["themeId"] as? String) ?? (goalData["theme"] as? String)
                }
            } catch {
                recordPermissionIfNeeded(error, context: "goals/\(goalId)")
            }
            if ctx.ref == nil { ctx.ref = goalId }
            goalContextCache[goalId] = ctx
            return ctx
        }

        func fetchStoryContext(storyId: String?, goalId: String?) async -> StoryContext {
            let normalizedStoryId = nonEmpty(storyId)
            let normalizedGoalId = nonEmpty(goalId)

            if let sid = normalizedStoryId, let cached = storyContextCache[sid] {
                return cached
            }

            var ctx = StoryContext()
            var resolvedGoalId = normalizedGoalId

            if let sid = normalizedStoryId {
                do {
                    let storySnapshot = try await db.collection("stories").document(sid).getDocument()
                    if let storyData = storySnapshot.data() {
                        ctx.storyRef = (storyData["reference"] as? String) ?? (storyData["ref"] as? String) ?? (storyData["shortId"] as? String) ?? (storyData["code"] as? String) ?? sid
                        ctx.themeName = (storyData["themeId"] as? String) ?? (storyData["theme"] as? String)
                        if let sprintId = nonEmpty(storyData["sprintId"] as? String) {
                            ctx.sprintId = sprintId
                            ctx.sprintName = await fetchSprintName(sprintId)
                        }
                        if let goalFromStory = nonEmpty(storyData["goalId"] as? String) {
                            resolvedGoalId = goalFromStory
                        }
                    } else {
                        ctx.storyRef = sid
                    }
                } catch {
                    recordPermissionIfNeeded(error, context: "stories/\(sid)")
                    ctx.storyRef = sid
                }
            }

            if let gid = resolvedGoalId {
                let goalCtx = await fetchGoalContext(gid)
                if ctx.themeName == nil { ctx.themeName = goalCtx.themeName }
                ctx.goalRef = goalCtx.ref ?? gid
            }

            if ctx.sprintName == nil, let sprintId = ctx.sprintId {
                ctx.sprintName = await fetchSprintName(sprintId)
            }

            if ctx.storyRef == nil, let sid = normalizedStoryId {
                ctx.storyRef = sid
            }
            if ctx.goalRef == nil, let gid = normalizedGoalId {
                ctx.goalRef = gid
            }

            if let sid = normalizedStoryId {
                storyContextCache[sid] = ctx
            }

            return ctx
        }

        do {
            // Load candidate tasks
            let queryStart = Date()
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Querying tasks (ownerUid=\(user.uid), sort=serverUpdatedAt, limit=3000)"
            )
            // Prefer ordering/filtering by serverUpdatedAt for reliable deltas (server authoritative time)
            var query: Query = db.collection("tasks").whereField("ownerUid", isEqualTo: user.uid)
            if mode == .delta {
                let since = await MainActor.run { UserPreferences.shared.lastDeltaSyncDate ?? UserPreferences.shared.lastSyncDate }
                if let since { query = query.whereField("serverUpdatedAt", isGreaterThan: since) }
            }
            query = query.order(by: "serverUpdatedAt", descending: true).limit(to: 3000)
            let taskQuerySnapshot: QuerySnapshot
            do {
                taskQuerySnapshot = try await query.getDocuments()
            } catch {
                // Fallback when the composite index (ownerUid + serverUpdatedAt) is missing
                let ns = error as NSError
                let needsIndex = ns.domain == FirestoreErrorDomain && ns.code == FirestoreErrorCode.failedPrecondition.rawValue
                let msg = ns.userInfo[NSLocalizedFailureReasonErrorKey] as? String ?? ns.localizedDescription
                if needsIndex && msg.localizedCaseInsensitiveContains("requires an index") {
                    SyncLogService.shared.logEvent(tag: "sync", level: "WARN", message: "Missing serverUpdatedAt index; falling back to updatedAt")
                    var fallback = db.collection("tasks").whereField("ownerUid", isEqualTo: user.uid)
                    if mode == .delta {
                        let since = await MainActor.run { UserPreferences.shared.lastDeltaSyncDate ?? UserPreferences.shared.lastSyncDate }
                        if let since { fallback = fallback.whereField("updatedAt", isGreaterThan: since) }
                    }
                    fallback = fallback.order(by: "updatedAt", descending: true).limit(to: 3000)
                    taskQuerySnapshot = try await fallback.getDocuments()
                } else {
                    throw error
                }
            }
            let queryElapsed = Int((Date().timeIntervalSince(queryStart)) * 1000)
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Tasks snapshot fetched in \(queryElapsed)ms"
            )
            var tasks = taskQuerySnapshot.documents.compactMap(toTask)
            // Prepare a single write batch early so imports and merges share one commit
            let batch = db.batch()
            // Prefetch related contexts in bulk to minimize per-doc reads
            await prefetchContexts(for: tasks)
            let tasksWithoutReminders = tasks.filter { $0.reminderId == nil && !isDone($0.status) }
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Fetched tasks: \(tasks.count); missing reminders: \(tasksWithoutReminders.count)"
            )
            var taskById: [String: FbTask] = Dictionary(uniqueKeysWithValues: tasks.map { ($0.id, $0) })
            var taskByReminderIdLatest: [String: FbTask] = [:]
            var taskByReferenceLatest: [String: FbTask] = [:]
            var taskByNormalizedTitleOldest: [String: FbTask] = [:]
            for task in tasks {
                guard let rid = task.reminderId, !rid.isEmpty else { continue }
                if let existing = taskByReminderIdLatest[rid] {
                    let existingUpdated = existing.updatedAt ?? Date.distantPast
                    let candidateUpdated = task.updatedAt ?? Date.distantPast
                    if candidateUpdated > existingUpdated {
                        taskByReminderIdLatest[rid] = task
                    }
                } else {
                    taskByReminderIdLatest[rid] = task
                }
            }
            for task in tasks {
                if let ref = task.reference?.lowercased(), !ref.isEmpty {
                    if let existing = taskByReferenceLatest[ref] {
                        let existingUpdated = existing.updatedAt ?? Date.distantPast
                        let candidateUpdated = task.updatedAt ?? Date.distantPast
                        if candidateUpdated > existingUpdated {
                            taskByReferenceLatest[ref] = task
                        }
                    } else {
                        taskByReferenceLatest[ref] = task
                    }
                }
                // Build a hardened normalized-title index (parity with server)
                let norm = normalizeTitleLocal(task.title)
                if !norm.isEmpty {
                    if let existing = taskByNormalizedTitleOldest[norm] {
                        // Prefer the oldest using createdAt when available, else updatedAt
                        let existingKey = existing.createdAt ?? existing.updatedAt ?? Date.distantFuture
                        let candidateKey = task.createdAt ?? task.updatedAt ?? Date.distantFuture
                        if candidateKey < existingKey { taskByNormalizedTitleOldest[norm] = task }
                    } else {
                        taskByNormalizedTitleOldest[norm] = task
                    }
                }
            }

            func restoreReminderMetadata(for reminder: EKReminder, task: FbTask, existingNotes: String?, previousTag: String?, userLines: [String]) async {
                let includeMetadata = await shouldIncludeBobMetadataInNotes()
                let context = await fetchStoryContext(storyId: task.storyId, goalId: task.goalId)
                let calendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }
                var meta: [String: String] = [
                    "status": statusString(for: task.status),
                    "synced": isoNow()
                ]
                // Only include human-friendly refs in note
                if let sid = task.storyId { meta["storyId"] = sid }
                if let gid = task.goalId { meta["goalId"] = gid }
                if let due = task.dueDate { meta["due"] = isoString(forMillis: due) }
                if let storyRef = context.storyRef { meta["storyRef"] = storyRef }
                if let goalRef = context.goalRef { meta["goalRef"] = goalRef }
                if let theme = context.themeName { meta["theme"] = theme }
                if let sprint = context.sprintName { meta["sprint"] = sprint }
                if let sprintId = context.sprintId { meta["sprintId"] = sprintId }
                let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                meta["taskRef"] = taskRefValue
                meta["list"] = task.reminderListName ?? calendarInfo.name
                meta["listId"] = task.reminderListId ?? calendarInfo.id
                // Compose enriched tags for note (#tags: ...)
                var tagSet = Set(task.tags)
                if let sref = context.storyRef { tagSet.insert(sref) }
                if let gref = context.goalRef { tagSet.insert(gref) }
                if let tname = context.themeName { tagSet.insert(tname) }
                if let sprintTag = makeSprintTag(from: context.sprintName) { tagSet.insert(sprintTag) }
                let tagList = Array(tagSet).sorted()
                if !tagList.isEmpty { meta["tags"] = tagList.joined(separator: ", ") }

                let rebuiltNotes = composeBobNote(meta: meta, userLines: userLines, includeMetadataBlock: includeMetadata)
                let linkURL = taskDeepLink(for: taskRefValue)

                if !dryRun {
                    await MainActor.run {
                        var needsSave = false
                        if existingNotes != rebuiltNotes {
                            reminder.notes = rebuiltNotes
                            needsSave = true
                        }
                        // Ensure URL deep link points to Bob task page using taskRef when available
                        if let url = linkURL {
                            if reminder.url != url {
                                reminder.url = url
                                needsSave = true
                            }
                        }
                        if includeMetadata, reminder.rmbSetTagsList(newTags: tagList) {
                            needsSave = true
                        }
                        if needsSave {
                            RemindersService.shared.save(reminder: reminder)
                        }
                    }
                }

                var detailMeta: [String: Any] = [
                    "taskRef": taskRefValue,
                    "status": meta["status"] ?? "unknown",
                    "calendar": meta["list"] ?? calendarInfo.name
                ]
                if let due = task.dueDate { detailMeta["due"] = isoString(forMillis: due) }
                if let storyRef = context.storyRef { detailMeta["storyRef"] = storyRef }
                if let goalRef = context.goalRef { detailMeta["goalRef"] = goalRef }
                if let theme = context.themeName { detailMeta["theme"] = theme }
                if let sprint = context.sprintName { detailMeta["sprint"] = sprint }
                if !tagList.isEmpty { detailMeta["tags"] = tagList }

                SyncLogService.shared.logSyncDetail(
                    direction: .toReminders,
                    action: "restoreReminderMetadata",
                    taskId: task.id,
                    storyId: task.storyId,
                    metadata: detailMeta,
                    dryRun: dryRun
                )
            }

            // Index existing reminders by taskId to avoid duplicates
            let calStart = Date()
            let lookupCalendars: [EKCalendar] = await MainActor.run { RemindersService.shared.getCalendars() }
            let calElapsed = Int((Date().timeIntervalSince(calStart)) * 1000)
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Loaded calendars: \(lookupCalendars.count) in \(calElapsed)ms"
            )
            let existingReminders = await RemindersService.shared.fetchReminders(in: lookupCalendars)
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Fetched reminders: \(existingReminders.count)"
            )

            var remindersById: [String: EKReminder] = [:]
            var existingTaskIds: Set<String> = []
            var remindersNeedingImport: [EKReminder] = []

            for reminder in existingReminders {
                // Only ignore recurring reminders that are not chores/routines
                if let rules = reminder.recurrenceRules, !rules.isEmpty {
                    let calendarName = await MainActor.run { reminder.calendar.title }
                    let reminderTags = await MainActor.run { reminder.rmbCurrentTags() }
                    if inferItemType(calendarTitle: calendarName, tags: reminderTags) == nil {
                        // Count as skipped import due to recurrence (non-chore/routine)
                        let titleText = await MainActor.run { reminder.title ?? "" }
                        let cal = await MainActor.run { reminder.calendar.title }
                        let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                        let due = await MainActor.run { reminder.dueDateComponents?.date }
                        skippedImports.append(SkippedItem(title: titleText, reason: "recurring", calendar: cal, tags: tagList, due: due))
                        continue
                    }
                }
                let rid = await MainActor.run { reminder.calendarItemIdentifier }
                remindersById[rid] = reminder

                let notes = await MainActor.run { reminder.notes }
                let parsed = parseBobNote(notes: notes)
                if let taskId = parsed.meta["taskId"], !taskId.isEmpty {
                    existingTaskIds.insert(taskId)

                    let trimmedList = parsed.meta["list"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                    let trimmedListId = parsed.meta["listId"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                    let trimmedTag = parsed.meta["tags"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                    let reminderCalendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }

                    var needsMetadataRepair = false
                    if trimmedList.isEmpty || trimmedList.caseInsensitiveCompare(reminderCalendarInfo.name) != .orderedSame {
                        needsMetadataRepair = true
                    }
                    if trimmedListId.isEmpty || trimmedListId != reminderCalendarInfo.id {
                        needsMetadataRepair = true
                    }

                    if let matchingTask = taskById[taskId] ?? taskByReminderIdLatest[rid] {
                        if let expectedListName = matchingTask.reminderListName, !expectedListName.isEmpty,
                           trimmedList.caseInsensitiveCompare(expectedListName) != .orderedSame {
                            needsMetadataRepair = true
                        }
                        if let expectedListId = matchingTask.reminderListId, !expectedListId.isEmpty,
                           trimmedListId != expectedListId {
                            needsMetadataRepair = true
                        }
                        if let canonicalTag = matchingTask.tags.first?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines), !canonicalTag.isEmpty {
                            if trimmedTag.caseInsensitiveCompare(canonicalTag) != .orderedSame {
                                needsMetadataRepair = true
                            }
                        } else if trimmedTag.isEmpty {
                            needsMetadataRepair = true
                        }
                        // Always rebuild to enforce expected pattern (also counts as a repair when needed)
                        if needsMetadataRepair { repairs += 1 }
                        await restoreReminderMetadata(
                            for: reminder,
                            task: matchingTask,
                            existingNotes: notes,
                            previousTag: parsed.meta["tags"],
                            userLines: parsed.userLines
                        )
                    } else if needsMetadataRepair {
                        // We were unable to resolve a matching task but metadata is incomplete; attempt best-effort repair using placeholder task details.
                        let placeholderTitle = await MainActor.run { reminder.title ?? "" }
                        let placeholder = FbTask(
                            id: taskId,
                            title: placeholderTitle,
                            dueDate: nil,
                            createdAt: nil,
                            completedAt: nil,
                            deleteAfter: nil,
                            reminderId: rid,
                            iosReminderId: nil,
                            status: nil,
                            storyId: nil,
                            goalId: nil,
                            reference: nil,
                            sourceRef: nil,
                            externalId: nil,
                            updatedAt: nil,
                            serverUpdatedAt: nil,
                            reminderListId: reminderCalendarInfo.id,
                            reminderListName: reminderCalendarInfo.name,
                            tags: trimmedTag.isEmpty ? [] : [trimmedTag],
                            convertedToStoryId: nil,
                            deletedFlag: nil,
                            reminderSyncDirective: nil,
                            priority: nil
                        )
                        repairs += 1
                        await restoreReminderMetadata(
                            for: reminder,
                            task: placeholder,
                            existingNotes: notes,
                            previousTag: parsed.meta["tags"],
                            userLines: parsed.userLines
                        )
                    }

                    continue
                }

                // Fallback: try taskRef from note header if present.
                // Prefer matching by human-readable reference; if not found, also
                // treat taskRef as a direct Firestore document ID to ensure
                // idempotency when tasks have no reference set.
                if let taskRefToken = parsed.meta["taskRef"], !taskRefToken.isEmpty {
                    let key = taskRefToken.lowercased()
                    let matchingTask = taskByReferenceLatest[key] ?? taskById[taskRefToken]
                    if let matchingTask {
                        existingTaskIds.insert(matchingTask.id)

                        let trimmedList = parsed.meta["list"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                        let trimmedListId = parsed.meta["listId"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                        let trimmedTag = parsed.meta["tags"]?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
                        let reminderCalendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }

                        var needsMetadataRepair = false
                        if let expectedListName = matchingTask.reminderListName, !expectedListName.isEmpty,
                           trimmedList.caseInsensitiveCompare(expectedListName) != .orderedSame {
                            needsMetadataRepair = true
                        }
                        if let expectedListId = matchingTask.reminderListId, !expectedListId.isEmpty,
                           trimmedListId != expectedListId {
                            needsMetadataRepair = true
                        }
                        if let canonicalTag = matchingTask.tags.first?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines), !canonicalTag.isEmpty {
                            if trimmedTag.caseInsensitiveCompare(canonicalTag) != .orderedSame {
                                needsMetadataRepair = true
                            }
                        }

                        // Always rebuild to enforce the expected BOB: block format
                        await restoreReminderMetadata(
                            for: reminder,
                            task: matchingTask,
                            existingNotes: notes,
                            previousTag: parsed.meta["tags"],
                            userLines: parsed.userLines
                        )
                        continue
                    }
                }

                if let existingTask = taskByReminderIdLatest[rid] {
                    await restoreReminderMetadata(for: reminder, task: existingTask, existingNotes: notes, previousTag: parsed.meta["tags"], userLines: parsed.userLines)
                    existingTaskIds.insert(existingTask.id)
                    continue
                }

                // Skip importing repeating reminders unless treated as chore/routine
                if let rules = reminder.recurrenceRules, !rules.isEmpty {
                    let calTitle = await MainActor.run { reminder.calendar.title }
                    let tags = await MainActor.run { reminder.rmbCurrentTags() }
                    if inferItemType(calendarTitle: calTitle, tags: tags) == nil { continue }
                }

                // Optional triage classification + routing before import
                // Goal: If in triage list and judged as work, move to Work list and skip Firestore import (personal-only)
                let prefs = await MainActor.run { UserPreferences.shared }
                let triageName = await MainActor.run { prefs.triageCalendarName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "" }
                let workListName = await MainActor.run { prefs.workCalendarName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "" }
                let enableTriage = await MainActor.run { prefs.enableTriageClassification }
                let reminderCalendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }
                let includeMetadataInNotes = await shouldIncludeBobMetadataInNotes()
                let stripMetadataIfHidden: () async -> Void = { [weak self] in
                    guard let self else { return }
                    guard !includeMetadataInNotes else { return }
                    let notesValue = await MainActor.run { reminder.notes }
                    let (_, userLines) = await self.parseBobNote(notes: notesValue)
                    let cleaned = await self.composeBobNote(meta: [:], userLines: userLines, includeMetadataBlock: false)
                    if cleaned != notesValue {
                        await MainActor.run {
                            reminder.notes = cleaned
                            RemindersService.shared.save(reminder: reminder)
                        }
                    }
                }

                // Never import items that already live in the configured Work list
                if !workListName.isEmpty, reminderCalendarInfo.name.caseInsensitiveCompare(workListName) == .orderedSame {
                    let titleForLog = await MainActor.run { reminder.title ?? "(untitled)" }
                    let msg = "Skipping Firestore import for work item in ‘\(workListName)’: \(titleForLog)"
                    SyncLogService.shared.logEvent(tag: "triage", level: "INFO", message: msg)
                    await stripMetadataIfHidden()
                    continue
                }

                // Also never import items explicitly tagged as "work" in Bob note metadata
                do {
                    let tags = await MainActor.run { reminder.rmbCurrentTags().map { $0.lowercased() } }
                    if tags.contains("work") {
                        let titleForLog = await MainActor.run { reminder.title ?? "(untitled)" }
                        SyncLogService.shared.logEvent(tag: "triage", level: "INFO", message: "Skipping Firestore import for work-tagged item: \(titleForLog)")
                        await stripMetadataIfHidden()
                        continue
                    }
                }

                // If configured, classify triage items prior to import
                if enableTriage, !triageName.isEmpty,
                   reminderCalendarInfo.name.caseInsensitiveCompare(triageName) == .orderedSame {
                    let title = await MainActor.run { reminder.title ?? "" }
                    let notes = await MainActor.run { reminder.notes }
                    let tags = await MainActor.run { reminder.rmbCurrentTags() }
                    let result = classifyTriage(title: title, notes: notes, tags: tags)

                    switch result.persona {
                    case .work:
                        // Move to work list (if configured) and tag as work, then skip import
                        var moved = false
                        if !workListName.isEmpty {
                            let target: EKCalendar? = await MainActor.run { RemindersService.shared.ensureCalendar(named: workListName) }
                            if let target {
                                if !dryRun {
                                    await MainActor.run { RemindersService.shared.move(reminder: reminder, to: target) }
                                }
                                moved = true
                            }
                        }
                        if !dryRun {
                            if includeMetadataInNotes {
                                let didTag: Bool = await MainActor.run { reminder.rmbUpdateTag(newTag: "work", removing: nil) }
                                if didTag {
                                    await MainActor.run { RemindersService.shared.save(reminder: reminder) }
                                }
                            }
                        }
                        let moveMsg = moved ? "moved to ‘\(workListName)’" : "left in triage (no work list configured)"
                        // Record skip for diagnostics list
                        let diagTitle = await MainActor.run { reminder.title ?? "" }
                        let diagTags = await MainActor.run { reminder.rmbCurrentTags() }
                        let diagDue = await MainActor.run { reminder.dueDateComponents?.date }
                        skippedImports.append(SkippedItem(title: diagTitle, reason: "triage_work", calendar: reminderCalendarInfo.name, tags: diagTags, due: diagDue))
                        let msg = String(
                            format: "Classified as WORK (%.2f) – %@: %@",
                            result.confidence,
                            moveMsg,
                            title
                        )
                            SyncLogService.shared.logEvent(
                                tag: "triage",
                                level: "INFO",
                                message: msg
                            )
                            await stripMetadataIfHidden()
                            continue
                    case .personal:
                        // Allow normal import path; optionally tag for visibility and theme
                        var tagsChanged = false
                        if !dryRun, includeMetadataInNotes {
                            let didTagPersonal: Bool = await MainActor.run { reminder.rmbUpdateTag(newTag: "personal", removing: nil) }
                            if didTagPersonal { tagsChanged = true }
                            // Add a theme tag when suggested and it matches a known Bob theme
                            if let hint = result.suggestedTheme, !hint.isEmpty {
                                let knownThemes = self.themeNames()
                                if let canonical =
                                    knownThemes.first(where: { $0.caseInsensitiveCompare(hint) == .orderedSame }) ??
                                    knownThemes.first(where: {
                                        let lcHint = hint.lowercased()
                                        let candidateLC = $0.lowercased()
                                        return lcHint.contains(candidateLC) || candidateLC.contains(lcHint)
                                    }) {
                                    let didThemeTag: Bool = await MainActor.run { reminder.rmbUpdateTag(newTag: canonical, removing: "theme-\(canonical)") }
                                    if didThemeTag { tagsChanged = true }
                                }
                            }
                            if tagsChanged {
                                await MainActor.run { RemindersService.shared.save(reminder: reminder) }
                            }
                        }
                        let msg = String(
                            format: "Classified as PERSONAL (%.2f) – importing: %@",
                            result.confidence,
                            title
                        )
                        SyncLogService.shared.logEvent(
                            tag: "triage",
                            level: "INFO",
                            message: msg
                        )
                    case .unknown:
                        // Fall through to import; no decision made
                        SyncLogService.shared.logEvent(tag: "triage", level: "INFO", message: "Classification unknown – importing: \(title)")
                    }
                }

                remindersNeedingImport.append(reminder)
            }

            // Remove reminders for tasks whose deleteAfter has elapsed (client-side cleanup alongside server TTL)
            let nowDate = Date()
            let expiredTasks = tasks.filter { task in
                guard let expiry = task.deleteAfter else { return false }
                guard task.reminderId != nil else { return false }
                return expiry <= nowDate
            }
            if !expiredTasks.isEmpty {
                SyncLogService.shared.logEvent(
                    tag: "sync",
                    level: "INFO",
                    message: "Removing expired reminders for \(expiredTasks.count) tasks (deleteAfter elapsed)"
                )
            }
            for task in expiredTasks {
                let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                var meta: [String: Any] = [
                    "taskRef": taskRefValue,
                    "deleteAfter": isoFormatter.string(from: task.deleteAfter ?? nowDate)
                ]
                if let rid = task.reminderId { meta["reminderId"] = rid }
                if let rid = task.reminderId, let reminder = remindersById[rid] {
                    if !dryRun {
                        await MainActor.run { RemindersService.shared.remove(reminder: reminder) }
                    }
                    meta["removedReminder"] = true
                } else {
                    meta["removedReminder"] = false
                    meta["reason"] = "reminder_not_found"
                }
                if !dryRun {
                    let ref = db.collection("tasks").document(task.id)
                    batch.setData([
                        "updatedAt": FieldValue.serverTimestamp(),
                        "serverUpdatedAt": FieldValue.serverTimestamp(),
                        "reminderId": FieldValue.delete(),
                        "reminderListId": FieldValue.delete(),
                        "reminderListName": FieldValue.delete(),
                        "reminderMissingAt": FieldValue.serverTimestamp()
                    ], forDocument: ref, merge: true)
                }
                SyncLogService.shared.logSyncDetail(
                    direction: .toReminders,
                    action: "removeExpiredReminder",
                    taskId: task.id,
                    storyId: task.storyId,
                    metadata: meta,
                    dryRun: dryRun
                )
            }

            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Reminders needing import: \(remindersNeedingImport.count); existingTaskIds: \(existingTaskIds.count)"
            )
            // Decide which tasks need reminders created
            var toCreate = tasksWithoutReminders.filter { !existingTaskIds.contains($0.id) }
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Tasks to create (no reminderId + not existing): \(toCreate.count)"
            )

            // Build dedupe index from current tasks for pre-import matching
            struct DedupeIndex {
                let byReminder: [String: FbTask]
                let byRef: [String: FbTask]
                let bySourceRef: [String: FbTask]
                let byIos: [String: FbTask]
                let byExternal: [String: FbTask]

                static func build(from tasks: [FbTask]) -> DedupeIndex {
                    var byReminderMap: [String: FbTask] = [:]
                    var refMap: [String: FbTask] = [:]
                    var sourceRefMap: [String: FbTask] = [:]
                    var iosMap: [String: FbTask] = [:]
                    var externalMap: [String: FbTask] = [:]
                    for task in tasks {
                        if let key = task.reminderId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty {
                            if byReminderMap[key] == nil { byReminderMap[key] = task }
                        }
                        if let key = task.reference?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty {
                            if refMap[key] == nil { refMap[key] = task }
                        }
                        if let key = task.sourceRef?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty {
                            if sourceRefMap[key] == nil { sourceRefMap[key] = task }
                        }
                        if let key = task.iosReminderId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty {
                            if iosMap[key] == nil { iosMap[key] = task }
                        }
                        if let key = task.externalId?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty {
                            if externalMap[key] == nil { externalMap[key] = task }
                        }
                    }
                    return DedupeIndex(byReminder: byReminderMap, byRef: refMap, bySourceRef: sourceRefMap, byIos: iosMap, byExternal: externalMap)
                }

                func resolve(reminderId: String?, ref: String?, sourceRef: String?, ios: String?, external: String?) -> FbTask? {
                    let keys: [(String?, [String: FbTask])] = [
                        (reminderId, byReminder),
                        (ref, byRef),
                        (sourceRef, bySourceRef),
                        (ios, byIos),
                        (external, byExternal)
                    ]
                    for (raw, map) in keys {
                        if let key = raw?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(), !key.isEmpty, let hit = map[key] {
                            return hit
                        }
                    }
                    // Combo key: if we have at least two values, check pairwise matches
                    let values = [ref, sourceRef, external].compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }.filter { !$0.isEmpty }
                    if values.count >= 2 {
                        for value in values {
                            if let hit = byRef[value] ?? bySourceRef[value] ?? byExternal[value] { return hit }
                        }
                    }
                    return nil
                }
            }
            var dedupeIndex = DedupeIndex.build(from: tasks)
            var existingRefsSet = Set(tasks.compactMap { $0.reference?.lowercased() })

            for reminder in remindersNeedingImport {
                do {
                    // Pre-import dedupe: try to link to existing task instead of creating a new one
                    let rid = await MainActor.run { reminder.calendarItemIdentifier }
                    let notes = await MainActor.run { reminder.notes }
                    let parsed = parseBobNote(notes: notes)
                    let noteRef = parsed.meta["taskRef"]?.trimmingCharacters(in: .whitespacesAndNewlines)
                    let canonical = dedupeIndex.resolve(reminderId: rid, ref: noteRef, sourceRef: nil, ios: nil, external: nil)
                    if let canonical {
                        // Merge reminder into canonical task
                        var data: [String: Any] = [
                            "reminderId": rid,
                            "updatedAt": FieldValue.serverTimestamp(),
                            "status": await MainActor.run { reminder.isCompleted } ? 2 : 0,
                            "title": await MainActor.run { reminder.title ?? "" },
                            "reminderListId": await MainActor.run { reminder.calendar.calendarIdentifier },
                            "reminderListName": await MainActor.run { reminder.calendar.title },
                            "source": "MacApp",
                            "serverUpdatedAt": FieldValue.serverTimestamp()
                        ]
                        if let due = await MainActor.run { reminder.dueDateComponents?.date } {
                            data["dueDate"] = due.timeIntervalSince1970 * 1_000.0
                        }
                        if await MainActor.run { reminder.isCompleted } {
                            let nowMs = Date().timeIntervalSince1970 * 1000.0
                            data["completedAt"] = nowMs
                            data["deleteAfter"] = nowMs + completedTaskTTL
                        }
                        if !dryRun {
                            try await db.collection("tasks").document(canonical.id).setData(data, merge: true)
                        }
                        let ctx = await fetchStoryContext(storyId: canonical.storyId, goalId: canonical.goalId)
                        var meta: [String: String] = [
                            "status": await MainActor.run { reminder.isCompleted } ? "complete" : "open",
                            "synced": isoNow(),
                        ]
                        if let sref = ctx.storyRef { meta["storyRef"] = sref }
                        if let gref = ctx.goalRef { meta["goalRef"] = gref }
                        let taskRefValue = (canonical.reference?.isEmpty == false) ? canonical.reference! : canonical.id
                        meta["taskRef"] = taskRefValue
                        let existingLines = await MainActor.run { reminder.notes }
                        let (_, userLines) = parseBobNote(notes: existingLines)
                        let includeMetadata = await shouldIncludeBobMetadataInNotes()
                        let newNotes = composeBobNote(meta: meta, userLines: userLines, includeMetadataBlock: includeMetadata)
                        let linkURL = taskDeepLink(for: taskRefValue)
                        if !dryRun {
                            await MainActor.run {
                                reminder.notes = newNotes
                                if let url = linkURL { reminder.url = url }
                                RemindersService.shared.save(reminder: reminder)
                            }
                        }
                        SyncLogService.shared.logSyncDetail(direction: .toBob, action: "mergeReminder", taskId: canonical.id, storyId: canonical.storyId, metadata: [
                            "reason": "preImportDedupe",
                            "taskRef": taskRefValue,
                        ], dryRun: dryRun)
                        // Update local caches
                        if !dryRun {
                            existingTaskIds.insert(canonical.id)
                            tasks.append(canonical)
                            taskById[canonical.id] = canonical
                        }
                        continue
                    }

                    // If the reminder note already contains a Bob task reference but we
                    // couldn't find it in the current snapshot (query window), attempt
                    // a direct lookup in Firestore. If still missing, fall back to
                    // import instead of permanently skipping.
                    if let noteRef, !noteRef.isEmpty {
                        let title = await MainActor.run { reminder.title ?? "" }
                        if let remote = await fetchTaskByReference(noteRef, ownerUid: ownerUid, db: db) {
                            existingTaskIds.insert(remote.id)
                            tasks.append(remote)
                            taskById[remote.id] = remote
                            if let refValue = remote.reference?.lowercased(), !refValue.isEmpty {
                                taskByReferenceLatest[refValue] = remote
                            }
                            dedupeIndex = DedupeIndex.build(from: tasks)
                            await restoreReminderMetadata(
                                for: reminder,
                                task: remote,
                                existingNotes: notes,
                                previousTag: parsed.meta["tags"],
                                userLines: parsed.userLines
                            )
                            SyncLogService.shared.logSyncDetail(
                                direction: .toBob,
                                action: "linkReminderByNoteRef",
                                taskId: remote.id,
                                storyId: remote.storyId,
                                metadata: [
                                    "taskRef": noteRef,
                                    "title": title,
                                    "reminderId": rid,
                                    "reason": "found_remote_by_ref"
                                ],
                                dryRun: dryRun
                            )
                            continue
                        } else {
                            SyncLogService.shared.logSyncDetail(
                                direction: .toBob,
                                action: "skipImportDueToNoteRef",
                                taskId: nil,
                                storyId: nil,
                                metadata: [
                                    "taskRef": noteRef,
                                    "title": title,
                                    "reminderId": rid,
                                    "reason": "missing_in_snapshot_and_remote_importing_new"
                                ],
                                dryRun: dryRun
                            )
                            // Do not skip import; allow flow to create a new task since the
                            // referenced task is missing in Firestore.
                        }
                    }

                    // Global title-based fallback: if a non-done task with the
                    // same normalized title exists anywhere, link to it rather than
                    // importing. This enforces global uniqueness by title at import time.
                    let normTitle = normalizeTitleLocal(await MainActor.run { reminder.title ?? "" })
                    if let candidate = taskByNormalizedTitleOldest[normTitle], !isDone(candidate.status) {
                        let calId = await MainActor.run { reminder.calendar.calendarIdentifier }
                        let calName = await MainActor.run { reminder.calendar.title }
                        var data: [String: Any] = [
                            "reminderId": rid,
                            "updatedAt": FieldValue.serverTimestamp(),
                            "status": await MainActor.run { reminder.isCompleted } ? 2 : 0,
                            "title": await MainActor.run { reminder.title ?? "" },
                            "reminderListId": calId,
                            "reminderListName": calName,
                            "source": "MacApp",
                            "serverUpdatedAt": FieldValue.serverTimestamp(),
                            // Help server-side diagnostics by storing a stable duplicateKey
                            "duplicateKey": "title:\(normTitle)"
                        ]
                        if let due = await MainActor.run { reminder.dueDateComponents?.date } {
                            data["dueDate"] = due.timeIntervalSince1970 * 1_000.0
                        }
                        if !dryRun {
                            let ref = db.collection("tasks").document(candidate.id)
                            try await ref.setData(data, merge: true)
                        }
                        SyncLogService.shared.logSyncDetail(
                            direction: .toBob,
                            action: "mergeReminderByTitleGlobal",
                            taskId: candidate.id,
                            storyId: candidate.storyId,
                            metadata: ["title": await MainActor.run { reminder.title ?? "" }],
                            dryRun: dryRun
                        )
                        existingTaskIds.insert(candidate.id)
                        continue
                    }

                    let imported = try await importReminderToBob(reminder: reminder, ownerUid: user.uid, db: db, dryRun: dryRun, batch: batch)
                    if !dryRun {
                        existingTaskIds.insert(imported.id)
                        if let reminderKey = imported.reminderId {
                            remindersById[reminderKey] = reminder
                            taskByReminderIdLatest[reminderKey] = imported
                        }
                        tasks.append(imported)
                        taskById[imported.id] = imported
                        if let refLowercased = imported.reference?.lowercased() { existingRefsSet.insert(refLowercased) }
                        // Keep dedupe index fresh
                        dedupeIndex = DedupeIndex.build(from: tasks)
                    }
                } catch {
                    let title = await MainActor.run { reminder.title ?? "" }
                    let msg = "Import reminder failed for \(title): \(error.localizedDescription)"
                    errors.append(msg)
                    SyncLogService.shared.logEvent(tag: "sync", level: "ERROR", message: msg)
                }
            }

            struct DuplicateInfo {
                let task: FbTask
                let reason: String
                let key: String
                let survivorId: String
            }

            var duplicatesToComplete: [String: DuplicateInfo] = [:]
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Dedupe analysis starting: tasks=\(tasks.count)")

            var tasksByReminderId: [String: [FbTask]] = [:]
            for task in tasks {
                if let rid = task.reminderId, !rid.isEmpty {
                    tasksByReminderId[rid, default: []].append(task)
                }
            }

            for (reminderId, group) in tasksByReminderId where group.count > 1 {
                let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                guard let survivor = sorted.first else { continue }
                for duplicate in sorted.dropFirst() {
                    if duplicatesToComplete[duplicate.id] == nil {
                        duplicatesToComplete[duplicate.id] = DuplicateInfo(task: duplicate, reason: "duplicateReminderId", key: reminderId, survivorId: survivor.id)
                    }
                }
            }

            var tasksByReference: [String: [FbTask]] = [:]
            var tasksBySourceRef: [String: [FbTask]] = [:]
            var tasksByIos: [String: [FbTask]] = [:]
            var tasksByExternal: [String: [FbTask]] = [:]
            for task in tasks {
                if let reference = task.reference?.lowercased(), !reference.isEmpty {
                    tasksByReference[reference, default: []].append(task)
                }
                if let src = task.sourceRef?.lowercased(), !src.isEmpty {
                    tasksBySourceRef[src, default: []].append(task)
                }
                if let ios = task.iosReminderId?.lowercased(), !ios.isEmpty {
                    tasksByIos[ios, default: []].append(task)
                }
                if let ext = task.externalId?.lowercased(), !ext.isEmpty {
                    tasksByExternal[ext, default: []].append(task)
                }
            }

            for (reference, group) in tasksByReference where group.count > 1 {
                let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                guard let survivor = sorted.first else { continue }
                for duplicate in sorted.dropFirst() {
                    if duplicatesToComplete[duplicate.id] == nil {
                        duplicatesToComplete[duplicate.id] = DuplicateInfo(task: duplicate, reason: "duplicateTaskRef", key: reference, survivorId: survivor.id)
                    }
                }
            }

            for (src, group) in tasksBySourceRef where group.count > 1 {
                let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                guard let survivor = sorted.first else { continue }
                for duplicate in sorted.dropFirst() {
                    if duplicatesToComplete[duplicate.id] == nil {
                        duplicatesToComplete[duplicate.id] = DuplicateInfo(task: duplicate, reason: "duplicateSourceRef", key: src, survivorId: survivor.id)
                    }
                }
            }

            for (ios, group) in tasksByIos where group.count > 1 {
                let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                guard let survivor = sorted.first else { continue }
                for duplicate in sorted.dropFirst() {
                    if duplicatesToComplete[duplicate.id] == nil {
                        duplicatesToComplete[duplicate.id] = DuplicateInfo(task: duplicate, reason: "duplicateIosReminderId", key: ios, survivorId: survivor.id)
                    }
                }
            }

            for (ext, group) in tasksByExternal where group.count > 1 {
                let sorted = group.sorted { ($0.updatedAt ?? Date.distantPast) > ($1.updatedAt ?? Date.distantPast) }
                guard let survivor = sorted.first else { continue }
                for duplicate in sorted.dropFirst() {
                    if duplicatesToComplete[duplicate.id] == nil {
                        duplicatesToComplete[duplicate.id] = DuplicateInfo(task: duplicate, reason: "duplicateExternalId", key: ext, survivorId: survivor.id)
                    }
                }
            }

            // Emit debug diagnostics on groups found
            let reasons = duplicatesToComplete.values.map { $0.reason }
            let counts = Dictionary(grouping: reasons, by: { $0 }).mapValues { $0.count }
            let parts = counts.map { "\($0.key)=\($0.value)" }.sorted().joined(separator: ", ")
            let dedupeSummary = parts.isEmpty ? "none" : parts
            SyncLogService.shared.logEvent(tag: "dedupe", level: "DEBUG", message: "Dedupe groups by reason: \(dedupeSummary)")

            let duplicateIds = Set(duplicatesToComplete.keys)
            if !duplicateIds.isEmpty {
                tasks.removeAll { duplicateIds.contains($0.id) }
                duplicateIds.forEach { taskById.removeValue(forKey: $0) }
                toCreate.removeAll { duplicateIds.contains($0.id) }
            }

            let duplicateInfos = Array(duplicatesToComplete.values)
            if !duplicateInfos.isEmpty {
                let byReason = Dictionary(grouping: duplicateInfos, by: { $0.reason }).mapValues { $0.count }
                let reasonSummary = byReason.map { "\($0.key)=\($0.value)" }.sorted().joined(separator: ", ")
                SyncLogService.shared.logEvent(
                    tag: "sync",
                    level: "INFO",
                    message: "Duplicates to complete: \(duplicateInfos.count) [\(reasonSummary)]"
                )
            }

            // Prepare a batch for Firestore updates
            // reuse the single batch declared earlier

            for info in duplicateInfos {
                var payload: [String: Any] = [
                    "status": 2,
                    "updatedAt": FieldValue.serverTimestamp(),
                    "duplicateOf": info.survivorId,
                    "duplicateKey": info.key
                ]
                if let reminderId = info.task.reminderId, !reminderId.isEmpty {
                    payload["reminderId"] = reminderId
                }
                // Schedule TTL deletion for duplicates client-side as well (defensive)
                let nowMs = Date().timeIntervalSince1970 * 1000.0
                payload["completedAt"] = nowMs
                payload["deleteAfter"] = nowMs + completedTaskTTL
                if !dryRun {
                    let ref = db.collection("tasks").document(info.task.id)
                    batch.setData(payload, forDocument: ref, merge: true)
                }
                var logMeta: [String: Any] = [
                    "reason": info.reason,
                    "duplicateKey": info.key,
                    "keptTaskId": info.survivorId
                ]
                if let reminderId = info.task.reminderId { logMeta["reminderId"] = reminderId }
                if let taskRef = info.task.reference { logMeta["taskRef"] = taskRef }
                SyncLogService.shared.logSyncDetail(direction: .toBob, action: "completeDuplicateTask", taskId: info.task.id, storyId: info.task.storyId, metadata: logMeta, dryRun: dryRun)
            }

            // Emit a summarized dedupe activity for data quality reporting
            if !duplicateInfos.isEmpty {
                do {
                    let groupsPayload: [[String: Any]] = duplicateInfos.reduce(into: [:] as [String: [String]]) { acc, info in
                        acc[info.survivorId, default: []].append(info.task.id)
                    }.map { (kept, removed) in
                        let reason = duplicateInfos.first(where: { $0.survivorId == kept })?.reason ?? "duplicate"
                        let key = duplicateInfos.first(where: { $0.survivorId == kept })?.key ?? ""
                        var keys: [String] = []
                        switch reason {
                        case "duplicateReminderId": keys = ["reminder:\(key)"]
                        case "duplicateTaskRef": keys = ["ref:\(key)"]
                        case "duplicateSourceRef": keys = ["sourceref:\(key)"]
                        case "duplicateIosReminderId": keys = ["ios:\(key)"]
                        case "duplicateExternalId": keys = ["external:\(key)"]
                        default: keys = [key]
                        }
                        return [
                            "kept": kept,
                            "removed": removed,
                            "keys": keys
                        ]
                    }
                    let activity: [String: Any] = [
                        "activityType": "deduplicate_tasks",
                        "ownerUid": user.uid,
                        "userId": user.uid,
                        "actor": "MacApp",
                        "createdAt": FieldValue.serverTimestamp(),
                        "updatedAt": FieldValue.serverTimestamp(),
                        "metadata": ["groups": groupsPayload]
                    ]
                    if !dryRun {
                        try await db.collection("activity_stream").addDocument(data: activity)
                    }
                } catch {
                    SyncLogService.shared.logEvent(tag: "activity", level: "ERROR", message: "Failed to write dedupe activity: \(error.localizedDescription)")
                }
            }

            for task in toCreate {
                let context = await fetchStoryContext(storyId: task.storyId, goalId: task.goalId)
                let themeName = context.themeName
                let sprintName = context.sprintName
                let sprintTag = makeSprintTag(from: sprintName)
                let storyRef = context.storyRef
                let goalRef = context.goalRef
                let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                func uniqueTags(_ list: [String]) -> [String] {
                    var seen = Set<String>()
                    return list.compactMap { raw in
                        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard !trimmed.isEmpty else { return nil }
                        let key = trimmed.lowercased()
                        guard !seen.contains(key) else { return nil }
                        seen.insert(key)
                        return trimmed
                    }
                }
                // Resolve calendar using theme→calendar mapping if available
                let cal: EKCalendar? = await MainActor.run {
                    if let name = themeName {
                        if let mappedId = UserPreferences.shared.themeCalendarMap[name],
                           let mapped = RemindersService.shared.getCalendar(withIdentifier: mappedId) {
                            return mapped
                        }
                        return RemindersService.shared.ensureCalendar(named: name)
                    }
                    return preferredCalendar ?? RemindersService.shared.getDefaultCalendar()
                }
                guard let cal = cal else { continue }

                // Preflight: skip if another device already mapped reminderId or holds a fresh create claim
                do {
                    let ref = db.collection("tasks").document(task.id)
                    let snap = try await ref.getDocument()
                    let data = snap.data() ?? [:]
                    if let existingRid = data["reminderId"] as? String,
                       !existingRid.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        let msg = "Skip create for task \(task.id): reminderId already set (\(existingRid))"
                        SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: msg)
                        continue
                    }
                    if let claim = data["reminderCreateClaim"] as? [String: Any],
                       let owner = claim["owner"] as? String,
                       let claimedAtMs = claim["claimedAtMillis"] as? Double {
                        let ageMs = Date().timeIntervalSince1970 * 1000.0 - claimedAtMs
                        if ageMs < 5 * 60 * 1000 && owner != syncInstanceId {
                            let msg = "Skip create for task \(task.id): claimed by \(owner.prefix(8))"
                            SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: msg)
                            continue
                        }
                    }
                    let nowMs = Date().timeIntervalSince1970 * 1000.0
                    let claimPayload: [String: Any] = [
                        "reminderCreateClaim": [
                            "owner": syncInstanceId,
                            "claimedAtMillis": nowMs,
                            "claimedAt": FieldValue.serverTimestamp()
                        ]
                    ]
                    try await ref.setData(claimPayload, merge: true)
                } catch {
                    SyncLogService.shared.logEvent(
                        tag: "sync",
                        level: "WARN",
                        message: "Preflight claim failed for task \(task.id): \(error.localizedDescription)"
                    )
                }

                // Skip if a reminder with this taskId already exists
                if existingTaskIds.contains(task.id) {
                    continue
                }

                var rmb = RmbReminder()
                rmb.title = task.title
                if let due = task.dueDate { rmb.hasDueDate = true; rmb.hasTime = false; rmb.date = Date(timeIntervalSince1970: due / 1_000.0) }
                rmb.calendar = cal

                var noteMeta: [String: String] = [
                    "status": statusString(for: task.status),
                    "synced": isoNow()
                ]
                if let sid = task.storyId { noteMeta["storyId"] = sid }
                if let gid = task.goalId { noteMeta["goalId"] = gid }
                if let due = task.dueDate { noteMeta["due"] = isoString(forMillis: due) }
                if let storyRef { noteMeta["storyRef"] = storyRef }
                if let goalRef { noteMeta["goalRef"] = goalRef }
                noteMeta["taskRef"] = taskRefValue
                noteMeta["list"] = cal.title
                noteMeta["listId"] = cal.calendarIdentifier
                if let sprintName { noteMeta["sprint"] = sprintName }
                if let sid = context.sprintId { noteMeta["sprintId"] = sid }
                if let themeName { noteMeta["theme"] = themeName }
                var tagCandidates: [String] = task.tags
                if let sprintTag { tagCandidates.append(sprintTag) } else if let sprintName { tagCandidates.append(sprintName) }
                if let storyRef { tagCandidates.append("story-\(storyRef)") }
                if let goalRef { tagCandidates.append("goal-\(goalRef)") }
                if let themeName { tagCandidates.append(themeName) }
                let tagsForReminder = uniqueTags(tagCandidates)
                if !tagsForReminder.isEmpty { noteMeta["tags"] = tagsForReminder.joined(separator: ", ") }
                let includeMetadata = await shouldIncludeBobMetadataInNotes()
                rmb.notes = composeBobNote(meta: noteMeta, userLines: [], includeMetadataBlock: includeMetadata)

                let reminderToCreate = rmb
                let rid: String? = await MainActor.run {
                    guard !dryRun, let saved = RemindersService.shared.createNew(with: reminderToCreate, in: cal) else { return nil }
                    // Set a complete #tags list rather than repeatedly overwriting
                    if includeMetadata {
                        _ = saved.rmbSetTagsList(newTags: tagsForReminder)
                    }
                    RemindersService.shared.save(reminder: saved)
                    return saved.calendarItemIdentifier
                }
                var creationMeta: [String: Any] = [
                    "title": task.title,
                    "calendar": cal.title,
                    "status": statusString(for: task.status)
                ]
                if let due = task.dueDate { creationMeta["due"] = isoString(forMillis: due) }
                if let themeName { creationMeta["theme"] = themeName }
                if let sprintName { creationMeta["sprint"] = sprintName }
                if let storyRef { creationMeta["storyRef"] = storyRef }
                if let goalRef { creationMeta["goalRef"] = goalRef }
                creationMeta["taskRef"] = taskRefValue
                if let gid = task.goalId { creationMeta["goalId"] = gid }
                if !tagsForReminder.isEmpty { creationMeta["tags"] = tagsForReminder }
                creationMeta["claimedBy"] = syncInstanceId
                SyncLogService.shared.logSyncDetail(direction: .toReminders, action: "createReminder", taskId: task.id, storyId: task.storyId, metadata: creationMeta, dryRun: dryRun)
                created += 1
                if task.storyId != nil { createdLinkedStories += 1 }
                if themeName != nil { createdWithTheme += 1 }

                // Pre-write mapping idempotently
                if let rid {
                    let ref = db.collection("tasks").document(task.id)
                    let tagsArray: [String] = tagsForReminder
                    let mappingPayload: [String: Any] = [
                        "updatedAt": FieldValue.serverTimestamp(),
                        "serverUpdatedAt": FieldValue.serverTimestamp(),
                        "reminderId": rid,
                        "reminderListId": cal.calendarIdentifier,
                        "reminderListName": cal.title,
                        "tags": tagsArray,
                        "reminderCreateClaim": FieldValue.delete()
                    ]
                    batch.setData(mappingPayload, forDocument: ref, merge: true)
                    taskByReminderIdLatest[rid] = FbTask(
                        id: task.id,
                        title: task.title,
                        dueDate: task.dueDate,
                        createdAt: task.createdAt,
                        completedAt: task.completedAt,
                        deleteAfter: task.deleteAfter,
                        reminderId: rid,
                        iosReminderId: nil,
                        status: task.status,
                        storyId: task.storyId,
                        goalId: task.goalId,
                        reference: task.reference,
                        sourceRef: nil,
                        externalId: nil,
                        updatedAt: task.updatedAt,
                        serverUpdatedAt: task.serverUpdatedAt,
                        reminderListId: cal.calendarIdentifier,
                        reminderListName: cal.title,
                        tags: tagsArray,
                        convertedToStoryId: task.convertedToStoryId,
                        deletedFlag: task.deletedFlag,
                        reminderSyncDirective: task.reminderSyncDirective,
                        priority: task.priority
                    )
                }
            }

            // Refresh and push mapping + completions + field updates back to Firestore
            let calendars: [EKCalendar] = await MainActor.run { RemindersService.shared.getCalendars() }
            let all = await RemindersService.shared.fetchReminders(in: calendars)
            SyncLogService.shared.logEvent(
                tag: "sync",
                level: "INFO",
                message: "Merging reminders to Bob for \(all.count) candidates"
            )
            for reminder in all {
                let includeMetadataInNotes = await shouldIncludeBobMetadataInNotes()
                let stripMetadataIfHidden: () async -> Void = { [weak self] in
                    guard let self else { return }
                    guard !includeMetadataInNotes else { return }
                    let notesValue = await MainActor.run { reminder.notes }
                    let (_, userLines) = await self.parseBobNote(notes: notesValue)
                    let cleaned = await self.composeBobNote(meta: [:], userLines: userLines, includeMetadataBlock: false)
                    if cleaned != notesValue {
                        await MainActor.run {
                            reminder.notes = cleaned
                            RemindersService.shared.save(reminder: reminder)
                        }
                    }
                }
                // Ignore recurring reminders entirely
                if let rules = reminder.recurrenceRules, !rules.isEmpty {
                    let titleText = await MainActor.run { reminder.title ?? "" }
                    let cal = await MainActor.run { reminder.calendar.title }
                    let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                    let due = await MainActor.run { reminder.dueDateComponents?.date }
                    skippedMerges.append(SkippedItem(title: titleText, reason: "recurring", calendar: cal, tags: tagList, due: due))
                    await stripMetadataIfHidden()
                    continue
                }
                let rid = reminder.calendarItemIdentifier
                guard let matchedTask = taskByReminderIdLatest[rid] else { continue }
                // Skip pushing to Bob when reminder is in configured Work list or explicitly tagged as work
                do {
                    let prefs = await MainActor.run { UserPreferences.shared }
                    let workListName = await MainActor.run { prefs.workCalendarName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "" }
                    let calName = await MainActor.run { reminder.calendar.title }
                    if !workListName.isEmpty, calName.caseInsensitiveCompare(workListName) == .orderedSame {
                        let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                        let due = await MainActor.run { reminder.dueDateComponents?.date }
                        let titleText = await MainActor.run { reminder.title ?? "" }
                        skippedMerges.append(SkippedItem(title: titleText, reason: "work_list", calendar: calName, tags: tagList, due: due))
                        await stripMetadataIfHidden()
                        continue
                    }
                    let tags = await MainActor.run { reminder.rmbCurrentTags().map { $0.lowercased() } }
                    if tags.contains("work") {
                        let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                        let due = await MainActor.run { reminder.dueDateComponents?.date }
                        let titleText = await MainActor.run { reminder.title ?? "" }
                        skippedMerges.append(SkippedItem(title: titleText, reason: "work_tag", calendar: calName, tags: tagList, due: due))
                        await stripMetadataIfHidden()
                        continue
                    }
                }
                let notes = await MainActor.run { reminder.notes }
                let parsed = parseBobNote(notes: notes)
                let ref = db.collection("tasks").document(matchedTask.id)
                let title = await MainActor.run { reminder.title ?? "" }
                let calendarTitle = await MainActor.run { reminder.calendar.title }
                let calendarIdentifier = await MainActor.run { reminder.calendar.calendarIdentifier }
                let completed = await MainActor.run { reminder.isCompleted }
                let dueDate = await MainActor.run { reminder.dueDateComponents?.date }
                let reminderTags: [String] = await MainActor.run {
                    reminder.rmbCurrentTags().compactMap { tag in
                        let trimmed = tag.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
                        return trimmed.isEmpty ? nil : trimmed
                    }
                }

                let mergedTags: [String] = {
                    var set = Set(reminderTags)
                    if let sref = parsed.meta["storyRef"], !sref.isEmpty { set.insert(sref) }
                    if let gref = parsed.meta["goalRef"], !gref.isEmpty { set.insert(gref) }
                    if let tname = parsed.meta["theme"], !tname.isEmpty { set.insert(tname) }
                    if let sprintName = parsed.meta["sprint"], let sprintTag = makeSprintTag(from: sprintName) { set.insert(sprintTag) }
                    return Array(set)
                }()

                var data: [String: Any] = [
                    "updatedAt": FieldValue.serverTimestamp(),
                    "reminderId": rid,
                    "title": title,
                    "status": completed ? 2 : 0,
                    "reminderListId": calendarIdentifier,
                    "reminderListName": calendarTitle,
                    "tags": mergedTags
                ]
                if let dueDate {
                    data["dueDate"] = dueDate.timeIntervalSince1970 * 1_000.0
                } else {
                    data["dueDate"] = FieldValue.delete()
                }
                if !dryRun {
                    batch.setData(data, forDocument: ref, merge: true)
                }

                var pushMeta: [String: Any] = [
                    "title": title,
                    "calendar": calendarTitle,
                    "status": completed ? "complete" : "open"
                ]
                if let dueDate { pushMeta["due"] = isoFormatter.string(from: dueDate) }
                if !reminderTags.isEmpty { pushMeta["tags"] = reminderTags }
                let context = await fetchStoryContext(storyId: matchedTask.storyId, goalId: matchedTask.goalId)
                if let storyRef = context.storyRef { pushMeta["storyRef"] = storyRef }
                if let goalRef = context.goalRef { pushMeta["goalRef"] = goalRef }
                let taskRefValue = (matchedTask.reference?.isEmpty == false) ? matchedTask.reference! : matchedTask.id
                pushMeta["taskRef"] = taskRefValue
                SyncLogService.shared.logSyncDetail(direction: .toBob, action: "mergeReminder", taskId: matchedTask.id, storyId: matchedTask.storyId, metadata: pushMeta, dryRun: dryRun)
                mergesToBob += 1
                updated += 1
            }

            // Pull updates from Firestore to Reminders for tasks that already have reminderId
            for task in tasks {
                guard let rid = task.reminderId, let reminder = remindersById[rid] else { continue }
                // Skip pulling from Bob when reminder is in configured Work list or explicitly tagged as work
                do {
                    let prefs = await MainActor.run { UserPreferences.shared }
                    let workListName = await MainActor.run { prefs.workCalendarName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "" }
                    let calName = await MainActor.run { reminder.calendar.title }
                    if !workListName.isEmpty, calName.caseInsensitiveCompare(workListName) == .orderedSame {
                        let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                        let due = await MainActor.run { reminder.dueDateComponents?.date }
                        let titleText = await MainActor.run { reminder.title ?? "" }
                        skippedMerges.append(SkippedItem(title: titleText, reason: "work_list", calendar: calName, tags: tagList, due: due))
                        continue
                    }
                    let tags = await MainActor.run { reminder.rmbCurrentTags().map { $0.lowercased() } }
                    if tags.contains("work") {
                        let tagList = await MainActor.run { reminder.rmbCurrentTags() }
                        let due = await MainActor.run { reminder.dueDateComponents?.date }
                        let titleText = await MainActor.run { reminder.title ?? "" }
                        skippedMerges.append(SkippedItem(title: titleText, reason: "work_tag", calendar: calName, tags: tagList, due: due))
                        continue
                    }
                }

                let context = await fetchStoryContext(storyId: task.storyId, goalId: task.goalId)

                let notes = await MainActor.run { reminder.notes }
                var (meta, userLines) = parseBobNote(notes: notes)
                let reminderTitle = await MainActor.run { reminder.title ?? "" }
                let reminderCompleted = await MainActor.run { reminder.isCompleted }
                let reminderDueDate = await MainActor.run { reminder.dueDateComponents?.date }
                let currentCalendarIdentifier = await MainActor.run { reminder.calendar.calendarIdentifier }
                let currentCalendarTitle = await MainActor.run { reminder.calendar.title }
                let reminderTags: [String] = await MainActor.run {
                    reminder.rmbCurrentTags().compactMap { tag in
                        let trimmed = tag.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
                        return trimmed.isEmpty ? nil : trimmed
                    }
                }
                let reminderLastModified = await MainActor.run { reminder.lastModifiedDate ?? Date.distantPast }
                let metaSynced = parseISO(meta["synced"]) ?? Date.distantPast
                let reminderEffectiveUpdated = max(reminderLastModified, metaSynced)
                let bobUpdated = task.updatedAt ?? Date.distantPast
                let nowIso = isoNow()
                let includeMetadataInNotes = await shouldIncludeBobMetadataInNotes()

                let reminderIsNewer = reminderEffectiveUpdated > bobUpdated
                let bobIsNewer = bobUpdated > reminderEffectiveUpdated

                let ref = db.collection("tasks").document(task.id)

                var reminderChanged = false
                var metaChanged = false

                if reminderIsNewer {
                    var pushData: [String: Any] = [
                        "updatedAt": FieldValue.serverTimestamp(),
                        "title": reminderTitle,
                        "status": reminderCompleted ? 2 : 0,
                        "reminderId": rid
                    ]
                    if reminderCompleted {
                        let nowMs = Date().timeIntervalSince1970 * 1000.0
                        pushData["completedAt"] = nowMs
                        pushData["deleteAfter"] = nowMs + completedTaskTTL
                    } else {
                        pushData["completedAt"] = FieldValue.delete()
                        pushData["deleteAfter"] = FieldValue.delete()
                    }
                    if let due = reminderDueDate {
                        pushData["dueDate"] = due.timeIntervalSince1970 * 1_000.0
                    } else {
                        pushData["dueDate"] = FieldValue.delete()
                    }
                    pushData["reminderListId"] = currentCalendarIdentifier
                    pushData["reminderListName"] = currentCalendarTitle
                    // Derive type + recurrence on reminder updates too
                    if let itemType = inferItemType(calendarTitle: currentCalendarTitle, tags: reminderTags) { pushData["type"] = itemType }
                    if let recurrence = recurrencePayload(for: reminder) {
                        pushData["recurrence"] = recurrence
                        if let freq = recurrence["frequency"] { pushData["repeatFrequency"] = freq }
                        if let interval = recurrence["interval"] { pushData["repeatInterval"] = interval }
                        if let days = recurrence["daysOfWeek"] { pushData["repeatDaysOfWeek"] = days }
                    } else {
                        pushData["recurrence"] = FieldValue.delete()
                        pushData["repeatFrequency"] = FieldValue.delete()
                        pushData["repeatInterval"] = FieldValue.delete()
                        pushData["repeatDaysOfWeek"] = FieldValue.delete()
                    }
                    
                    // Priority Sync (Apple -> BOB)
                    let applePrio = await MainActor.run { reminder.priority }
                    let bobPrio: Int
                    switch applePrio {
                    case 1...4: bobPrio = 1
                    case 5: bobPrio = 2
                    case 6...9: bobPrio = 3
                    default: bobPrio = 4
                    }
                    if bobPrio != (task.priority ?? 3) {
                        pushData["priority"] = bobPrio
                    }

                    if let storyRef = context.storyRef { pushData["storyRef"] = storyRef }
                    if let theme = context.themeName { pushData["theme"] = theme }
                    var sprintIdToSet: String? = context.sprintId
                    if sprintIdToSet == nil, let due = reminderDueDate {
                        if let sprint = await resolveSprintForDueDate(due: due, ownerUid: user.uid, db: db) {
                            sprintIdToSet = sprint.id
                            meta["sprint"] = sprint.name
                        }
                    }
                    if let sid = sprintIdToSet { pushData["sprintId"] = sid; meta["sprintId"] = sid }
                    if let goalRef = context.goalRef { pushData["goalRef"] = goalRef }
                    if let taskRef = task.reference, !taskRef.isEmpty { pushData["reference"] = taskRef }

                    // Compose enriched tags for note metadata early so we can include in push + log
                    var tagSet = Set(reminderTags)
                    if let sref = context.storyRef { tagSet.insert(sref) }
                    if let gref = context.goalRef { tagSet.insert(gref) }
                    if let tname = context.themeName { tagSet.insert(tname) }
                    if let sprintTag = makeSprintTag(from: context.sprintName) { tagSet.insert(sprintTag) }
                    let tagList = Array(tagSet).sorted()
                    if !tagList.isEmpty { pushData["tags"] = tagList }

                    // Bump serverUpdatedAt so delta filter (server clock) sees this change
                    pushData["serverUpdatedAt"] = FieldValue.serverTimestamp()

                    if !dryRun {
                        batch.setData(pushData, forDocument: ref, merge: true)
                    }

                    var logMeta: [String: Any] = [
                        "title": reminderTitle,
                        "status": reminderCompleted ? "complete" : "open",
                        "calendar": currentCalendarTitle
                    ]
                    if let due = reminderDueDate { logMeta["due"] = isoFormatter.string(from: due) }
                    if let storyRef = context.storyRef { logMeta["storyRef"] = storyRef }
                    let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                    logMeta["taskRef"] = taskRefValue
                    if let goalRef = context.goalRef { logMeta["goalRef"] = goalRef }
                    if !tagList.isEmpty { logMeta["tags"] = tagList }
                    SyncLogService.shared.logSyncDetail(direction: .toBob, action: "updateFromReminder", taskId: task.id, storyId: task.storyId, metadata: logMeta, dryRun: dryRun)
                    mergesToBob += 1

                    meta["status"] = reminderCompleted ? "complete" : "open"
                    if let due = reminderDueDate { meta["due"] = isoFormatter.string(from: due) } else { meta.removeValue(forKey: "due") }
                    meta["list"] = currentCalendarTitle
                    meta["listId"] = currentCalendarIdentifier
                    // meta tags already computed; mirror to note metadata
                    if !tagList.isEmpty { meta["tags"] = tagList.joined(separator: ", ") }
                    meta["synced"] = nowIso
                    // Ensure core identifiers present in note for dedup
                    if let uid = Auth.auth().currentUser?.uid, meta["ownerUid"] != uid { meta["ownerUid"] = uid }
                    if meta["source"] == nil { meta["source"] = "MacApp" }
                    if meta["reminderId"] != rid { meta["reminderId"] = rid }
                    metaChanged = true
                    if !dryRun { updated += 1 }
                    // Ensure URL deep link exists with current taskRef and update #tags list
                    if !dryRun {
                        let linkURL = taskDeepLink(for: taskRefValue)
                        let tagsStringConst = meta["tags"] ?? ""
                        await MainActor.run {
                            var needsSave = false
                            if let url = linkURL, reminder.url != url {
                                reminder.url = url
                                needsSave = true
                            }
                            if includeMetadataInNotes, !tagsStringConst.isEmpty {
                                let list = tagsStringConst.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
                                if reminder.rmbSetTagsList(newTags: list) { needsSave = true }
                            }
                            if needsSave {
                                RemindersService.shared.save(reminder: reminder)
                            }
                        }
                    }
                } else if bobIsNewer {
                    let currentCalendarId = await MainActor.run { reminder.calendar.calendarIdentifier }
                    var movedCalendarName: String?
                    let targetCalendar = await MainActor.run { () -> EKCalendar? in
                        if let themeName = context.themeName {
                            if let mappedIdentifier = UserPreferences.shared.themeCalendarMap[themeName],
                               let cal = RemindersService.shared.getCalendar(withIdentifier: mappedIdentifier) {
                                return cal
                            }
                            if let existing = RemindersService.shared.getCalendars().first(where: { $0.title.caseInsensitiveCompare(themeName) == .orderedSame }) {
                                return existing
                            }
                            if !dryRun {
                                return RemindersService.shared.ensureCalendar(named: themeName)
                            }
                        }
                        if let listId = task.reminderListId,
                           let cal = RemindersService.shared.getCalendar(withIdentifier: listId) {
                            return cal
                        }
                        if let listName = task.reminderListName, !listName.isEmpty {
                            if let existing = RemindersService.shared.getCalendars().first(where: { $0.title.caseInsensitiveCompare(listName) == .orderedSame }) {
                                return existing
                            }
                            if !dryRun {
                                return RemindersService.shared.ensureCalendar(named: listName)
                            }
                        }
                        return nil
                    }
                    if let targetCalendar,
                       targetCalendar.calendarIdentifier != currentCalendarId {
                        movedCalendarName = targetCalendar.title
                        if !dryRun {
                            await MainActor.run { RemindersService.shared.move(reminder: reminder, to: targetCalendar) }
                        }
                        reminderChanged = true
                        metaChanged = true
                    }

                    if reminderTitle != task.title {
                        if !dryRun {
                            await MainActor.run { reminder.title = task.title }
                        }
                        reminderChanged = true
                    }

                    if let due = task.dueDate {
                        let date = Date(timeIntervalSince1970: due / 1_000.0)
                        if reminderDueDate != date {
                            if !dryRun {
                                await MainActor.run { reminder.dueDateComponents = date.dateComponents(withTime: false) }
                            }
                            reminderChanged = true
                        }
                        meta["due"] = isoString(forMillis: due)
                    } else if reminderDueDate != nil {
                        if !dryRun {
                            await MainActor.run { reminder.dueDateComponents = nil }
                        }
                        reminderChanged = true
                        meta.removeValue(forKey: "due")
                    }

                    let shouldBeCompleted = isDone(task.status)
                    if reminderCompleted != shouldBeCompleted {
                        if !dryRun {
                            await MainActor.run { reminder.isCompleted = shouldBeCompleted }
                        }
                        reminderChanged = true
                    }
                    
                    // Priority Sync (BOB -> Apple)
                    let targetApplePrio: Int
                    let priorityTag: String
                    switch task.priority ?? 3 {
                    case 1: 
                        targetApplePrio = 1 // High (!!!)
                        priorityTag = "#P1"
                    case 2: 
                        targetApplePrio = 5 // Medium (!!)
                        priorityTag = "#P2"
                    case 3: 
                        targetApplePrio = 9 // Low (!)
                        priorityTag = "#P3"
                    case 4: 
                        targetApplePrio = 0 // None
                        priorityTag = "#P4"
                    case 5: 
                        targetApplePrio = 0 // None
                        priorityTag = "#P5"
                    default: 
                        targetApplePrio = 0
                        priorityTag = ""
                    }
                    
                    let currentApplePrio = await MainActor.run { reminder.priority }
                    if currentApplePrio != targetApplePrio {
                        if !dryRun {
                            await MainActor.run { reminder.priority = targetApplePrio }
                        }
                        reminderChanged = true
                    }

                    // Append Priority Tag to Notes if missing
                    var currentNotes = await MainActor.run { reminder.notes ?? "" }
                    if !priorityTag.isEmpty && !currentNotes.contains(priorityTag) {
                        // Remove old priority tags
                        currentNotes = currentNotes.replacingOccurrences(of: "#P1", with: "")
                            .replacingOccurrences(of: "#P2", with: "")
                            .replacingOccurrences(of: "#P3", with: "")
                            .replacingOccurrences(of: "#P4", with: "")
                            .replacingOccurrences(of: "#P5", with: "")
                            .trimmingCharacters(in: .whitespacesAndNewlines)
                        
                        let newNotes = currentNotes.isEmpty ? priorityTag : "\(currentNotes)\n\n\(priorityTag)"
                        if !dryRun {
                            await MainActor.run { reminder.notes = newNotes }
                        }
                        reminderChanged = true
                    }

                    meta["status"] = shouldBeCompleted ? "complete" : "open"
                    meta["synced"] = nowIso
                    // Ensure core identifiers present in note for dedup
                    if let uid = Auth.auth().currentUser?.uid, meta["ownerUid"] != uid { meta["ownerUid"] = uid }
                    if meta["source"] == nil { meta["source"] = "MacApp" }
                    if meta["reminderId"] != rid { meta["reminderId"] = rid }
                    metaChanged = true

                    var detailMeta: [String: Any] = [
                        "title": task.title,
                        "status": meta["status"] ?? "unknown"
                    ]
                    if let due = task.dueDate { detailMeta["due"] = isoString(forMillis: due) }
                    if let storyRef = context.storyRef { detailMeta["storyRef"] = storyRef }
                    let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                    detailMeta["taskRef"] = taskRefValue
                    if let goalRef = context.goalRef { detailMeta["goalRef"] = goalRef }
                    let resolvedCalendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }
                    detailMeta["calendar"] = movedCalendarName ?? resolvedCalendarInfo.name
                    if !task.tags.isEmpty { detailMeta["tags"] = task.tags }
                    SyncLogService.shared.logSyncDetail(direction: .toReminders, action: "updateReminderFromBob", taskId: task.id, storyId: task.storyId, metadata: detailMeta, dryRun: dryRun)
                    updatesFromBob += 1
                    // Keep reminder URL pointing at Bob task
                    if !dryRun {
                        let linkURL = taskDeepLink(for: taskRefValue)
                        await MainActor.run {
                            if let url = linkURL, reminder.url != url {
                                reminder.url = url
                                RemindersService.shared.save(reminder: reminder)
                            }
                        }
                    }

                    // Ensure Firestore tags inherit story/goal/theme and mark conversions
                    var tagSet = Set(task.tags)
                    if let sref = context.storyRef { tagSet.insert(sref) }
                    if let gref = context.goalRef { tagSet.insert(gref) }
                    if let tname = context.themeName { tagSet.insert(tname) }
                    if task.convertedToStoryId != nil { tagSet.insert("convertedtostory") }
                    if !dryRun {
                        let tagUpdate: [String: Any] = [
                            "updatedAt": FieldValue.serverTimestamp(),
                            "tags": Array(tagSet)
                        ]
                        batch.setData(tagUpdate, forDocument: ref, merge: true)
                    }
                    // Add tag on Reminder when conversion detected
                    if task.convertedToStoryId != nil, shouldBeCompleted {
                        if !dryRun {
                            if includeMetadataInNotes {
                                let existing = await MainActor.run { reminder.rmbCurrentTags() }
                                let updated = Array(Set(existing + ["convertedtostory"]))
                                _ = await MainActor.run { reminder.rmbSetTagsList(newTags: updated) }
                            }
                        }
                        reminderChanged = true
                    }
                }

                // Do not include internal taskId in the note; rely on reminderId mapping
                let storyIdValue = task.storyId
                if let storyIdValue {
                    if meta["storyId"] != storyIdValue { meta["storyId"] = storyIdValue; metaChanged = true }
                } else if meta.removeValue(forKey: "storyId") != nil {
                    metaChanged = true
                }
                let previousTagValue = meta["tags"]

                if let goal = task.goalId {
                    if meta["goalId"] != goal { meta["goalId"] = goal; metaChanged = true }
                } else if meta.removeValue(forKey: "goalId") != nil {
                    metaChanged = true
                }
                if let storyRef = context.storyRef {
                    if meta["storyRef"] != storyRef { meta["storyRef"] = storyRef; metaChanged = true }
                } else if meta.removeValue(forKey: "storyRef") != nil {
                    metaChanged = true
                }
                let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                if meta["taskRef"] != taskRefValue {
                    meta["taskRef"] = taskRefValue
                    metaChanged = true
                }
                if let goalRef = context.goalRef {
                    if meta["goalRef"] != goalRef { meta["goalRef"] = goalRef; metaChanged = true }
                } else if meta.removeValue(forKey: "goalRef") != nil {
                    metaChanged = true
                }
                if let theme = context.themeName {
                    if meta["theme"] != theme { meta["theme"] = theme; metaChanged = true }
                } else if meta.removeValue(forKey: "theme") != nil {
                    metaChanged = true
                }
                if let sprint = context.sprintName {
                    if meta["sprint"] != sprint { meta["sprint"] = sprint; metaChanged = true }
                } else if meta.removeValue(forKey: "sprint") != nil {
                    metaChanged = true
                }
                if let sid = context.sprintId {
                    if meta["sprintId"] != sid { meta["sprintId"] = sid; metaChanged = true }
                } else if meta.removeValue(forKey: "sprintId") != nil {
                    metaChanged = true
                }

                // Compose enriched #tags line for the reminder note
                var tagSetForMeta = Set(task.tags)
                if let sref = context.storyRef { tagSetForMeta.insert(sref) }
                if let gref = context.goalRef { tagSetForMeta.insert(gref) }
                if let tname = context.themeName { tagSetForMeta.insert(tname) }
                if let sprintTag = makeSprintTag(from: context.sprintName) { tagSetForMeta.insert(sprintTag) }
                let tagListForMeta = Array(tagSetForMeta).sorted()
                if !tagListForMeta.isEmpty {
                    let joined = tagListForMeta.joined(separator: ", ")
                    if meta["tags"] != joined { meta["tags"] = joined; metaChanged = true }
                } else if meta.removeValue(forKey: "tags") != nil {
                    metaChanged = true
                }
                let resolvedCalendarInfo = await MainActor.run { (id: reminder.calendar.calendarIdentifier, name: reminder.calendar.title) }
                if meta["list"] != resolvedCalendarInfo.name { meta["list"] = resolvedCalendarInfo.name; metaChanged = true }
                if meta["listId"] != resolvedCalendarInfo.id { meta["listId"] = resolvedCalendarInfo.id; metaChanged = true }

                var tagChanged = false
                if includeMetadataInNotes {
                    tagChanged = await MainActor.run { reminder.rmbSetTagsList(newTags: tagListForMeta) }
                }
                if tagChanged { reminderChanged = true }

                let newNotes = composeBobNote(meta: meta, userLines: userLines, includeMetadataBlock: includeMetadataInNotes)
                if newNotes != notes {
                    if !dryRun {
                        await MainActor.run { reminder.notes = newNotes }
                    }
                    reminderChanged = true
                }

                if (reminderChanged || metaChanged) {
                    if !dryRun {
                        await MainActor.run { RemindersService.shared.save(reminder: reminder) }
                        if bobIsNewer { updated += 1 }
                    }
                }

                taskById[task.id] = task
            }

            // Tasks with reminderId whose reminder no longer exists → clear mapping
            let reminderIdsSet = Set(all.map { $0.calendarItemIdentifier })
            let orphanTasks = tasks.filter { if let rid = $0.reminderId { return !reminderIdsSet.contains(rid) } else { return false } }
            for orphanTask in orphanTasks {
                let ref = db.collection("tasks").document(orphanTask.id)
                if !dryRun {
                    batch.setData(["updatedAt": FieldValue.serverTimestamp(), "reminderId": FieldValue.delete(), "reminderMissingAt": FieldValue.serverTimestamp()], forDocument: ref, merge: true)
                }
                let context = await fetchStoryContext(storyId: orphanTask.storyId, goalId: orphanTask.goalId)
                var orphanMeta: [String: Any] = ["reason": "reminder missing"]
                let orphanTaskRef = (orphanTask.reference?.isEmpty == false) ? orphanTask.reference! : orphanTask.id
                orphanMeta["taskRef"] = orphanTaskRef
                if let storyRef = context.storyRef { orphanMeta["storyRef"] = storyRef }
                if let goalRef = context.goalRef { orphanMeta["goalRef"] = goalRef }
                SyncLogService.shared.logSyncDetail(direction: .toBob, action: "clearMissingReminder", taskId: orphanTask.id, storyId: orphanTask.storyId, metadata: orphanMeta, dryRun: dryRun)
                updated += 1
            }

            // Best-effort delete: if the task indicates deletion or conversion, complete the reminder and tag
            for task in tasks {
                guard let rid = task.reminderId, let reminder = remindersById[rid] else { continue }
                let context = await fetchStoryContext(storyId: task.storyId, goalId: task.goalId)
                let deleted =
                    (task.status as? String)?.lowercased() == "deleted" ||
                    (task.status as? NSNumber)?.intValue == -1 ||
                    ((task.deletedFlag as? Bool) == true) ||
                    (task.reminderSyncDirective?.lowercased() == "complete") ||
                    (task.reminderSyncDirective?.lowercased() == "delete") ||
                    (task.convertedToStoryId != nil)
                if deleted {
                    if !dryRun {
                        let wasCompleted = await MainActor.run { reminder.isCompleted }
                        await MainActor.run { reminder.isCompleted = true }
                        let notes = await MainActor.run { reminder.notes }
                        var (meta, userLines) = parseBobNote(notes: notes)
                        let includeMetadata = await shouldIncludeBobMetadataInNotes()
                        // Do not include internal taskId in the note
                        if let story = task.storyId { meta["storyId"] = story } else { meta.removeValue(forKey: "storyId") }
                        if let goal = task.goalId { meta["goalId"] = goal } else { meta.removeValue(forKey: "goalId") }
                        if let storyRef = context.storyRef { meta["storyRef"] = storyRef } else { meta.removeValue(forKey: "storyRef") }
                        if let goalRef = context.goalRef { meta["goalRef"] = goalRef } else { meta.removeValue(forKey: "goalRef") }
                        let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                        meta["taskRef"] = taskRefValue
                        meta["status"] = "complete"
                        meta["synced"] = isoNow()
                        // Add convertedtostory tag for conversions
                        if task.convertedToStoryId != nil {
                            meta["tags"] = "convertedtostory"
                            if includeMetadata {
                                _ = await MainActor.run { reminder.rmbUpdateTag(newTag: "convertedtostory", removing: nil) }
                            }
                        }
                        let newNotes = composeBobNote(meta: meta, userLines: userLines, includeMetadataBlock: includeMetadata)
                        if newNotes != notes {
                            await MainActor.run { reminder.notes = newNotes }
                        }
                        await MainActor.run { RemindersService.shared.save(reminder: reminder) }
                        if !wasCompleted { updated += 1 }
                    }
                    let reminderTitle = await MainActor.run { reminder.title ?? "" }
                    var completionMeta: [String: Any] = ["title": reminderTitle]
                    let taskRefValue = (task.reference?.isEmpty == false) ? task.reference! : task.id
                    completionMeta["taskRef"] = taskRefValue
                    if let storyRef = context.storyRef { completionMeta["storyRef"] = storyRef }
                    if let goalRef = context.goalRef { completionMeta["goalRef"] = goalRef }
                    if task.convertedToStoryId != nil { completionMeta["tags"] = ["convertedtostory"] }
                    SyncLogService.shared.logSyncDetail(direction: .toReminders, action: "markCompleteFromBobDelete", taskId: task.id, storyId: task.storyId, metadata: completionMeta, dryRun: dryRun)
                }
            }

            // Commit batched writes
            if !dryRun {
                let batchStart = Date()
                do {
                    try await batch.commit()
                    let batchElapsed = Int((Date().timeIntervalSince(batchStart)) * 1000)
                    SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: "Batch commit OK in \(batchElapsed)ms")
                } catch {
                    let msg = "Batch commit failed: \(error.localizedDescription)"
                    SyncLogService.shared.logEvent(tag: "sync", level: "ERROR", message: msg)
                    errors.append(msg)
                }
            }
            // Expose tasks for diff logging after leaving the scope
            fetchedTasksForDiff = tasks

        } catch {
            errors.append("Firestore query failed: \(error.localizedDescription)")
        }
        mark("firestore-sync-phase")

        // Title-based diff: Firestore vs Mac
        do {
            // Build normalized-title maps for tasks and reminders
            func normalizeTitle(_ text: String) -> String {
                return text.lowercased()
                    .replacingOccurrences(of: "\n", with: " ")
                    .components(separatedBy: CharacterSet.alphanumerics.inverted)
                    .filter { !$0.isEmpty }
                    .joined(separator: " ")
            }
            let reminderCalendars: [EKCalendar] = await MainActor.run { RemindersService.shared.getCalendars() }
            let remindersAll = await RemindersService.shared.fetchReminders(in: reminderCalendars)
            var reminderTitles: [String: [EKReminder]] = [:]
            for reminderItem in remindersAll {
                let titleText = await MainActor.run { reminderItem.title ?? "" }
                let key = normalizeTitle(titleText)
                if key.isEmpty { continue }
                reminderTitles[key, default: []].append(reminderItem)
            }
            var taskTitles: [String: [FbTask]] = [:]
            for taskItem in fetchedTasksForDiff where !isDone(taskItem.status) {
                let key = normalizeTitle(taskItem.title)
                if key.isEmpty { continue }
                taskTitles[key, default: []].append(taskItem)
            }
            // Firestore-only (by title)
            var firestoreOnlySample: [[String: Any]] = []
            let firestoreOnly = taskTitles.compactMap { (key, arr) -> [FbTask]? in
                return reminderTitles[key] == nil ? arr : nil
            }.flatMap { $0 }
            firestoreOnlyCount = firestoreOnly.count
            for taskItem in firestoreOnly.prefix(50) {
                var meta: [String: Any] = [
                    "title": taskItem.title,
                    "taskRef": (taskItem.reference?.isEmpty == false ? taskItem.reference! : taskItem.id),
                    "id": taskItem.id
                ]
                if let ca = taskItem.createdAt { meta["createdAt"] = isoFormatter.string(from: ca) }
                if let ua = taskItem.updatedAt { meta["updatedAt"] = isoFormatter.string(from: ua) }
                if let su = taskItem.serverUpdatedAt { meta["serverUpdatedAt"] = isoFormatter.string(from: su) }
                if let list = taskItem.reminderListName { meta["list"] = list }
                if let due = taskItem.dueDate { meta["due"] = isoString(forMillis: due) }
                if !taskItem.tags.isEmpty { meta["tags"] = taskItem.tags }
                firestoreOnlySample.append(meta)
                // Verbose local log line
                let parts = [
                    "FS-ONLY",
                    "ref=\(meta["taskRef"] as? String ?? taskItem.id)",
                    "title=\(taskItem.title)",
                    taskItem.reminderListName != nil ? "list=\(taskItem.reminderListName!)" : nil,
                    taskItem.dueDate != nil ? "due=\(isoString(forMillis: taskItem.dueDate!))" : nil
                ].compactMap { $0 }.joined(separator: " ")
                SyncLogService.shared.logEvent(tag: "diff", level: "INFO", message: parts)
            }
            // Mac-only (by title)
            var macOnlySample: [[String: Any]] = []
            let macOnly = reminderTitles.compactMap { (key, arr) -> [EKReminder]? in
                return taskTitles[key] == nil ? arr : nil
            }.flatMap { $0 }
            macOnlyCount = macOnly.count
            for reminderItem in macOnly.prefix(50) {
                let title = await MainActor.run { reminderItem.title ?? "" }
                let cal = await MainActor.run { reminderItem.calendar.title }
                let tags = await MainActor.run { reminderItem.rmbCurrentTags() }
                let due = await MainActor.run { reminderItem.dueDateComponents?.date }
                let created = await MainActor.run { reminderItem.creationDate }
                let updated = await MainActor.run { reminderItem.lastModifiedDate }
                var meta: [String: Any] = [
                    "title": title,
                    "calendar": cal,
                    "tags": tags
                ]
                if let due { meta["due"] = isoFormatter.string(from: due) }
                if let created { meta["createdAt"] = isoFormatter.string(from: created) }
                if let updated { meta["updatedAt"] = isoFormatter.string(from: updated) }
                macOnlySample.append(meta)
                let parts = [
                    "MAC-ONLY",
                    "title=\(title)",
                    "list=\(cal)",
                    due != nil ? "due=\(isoFormatter.string(from: due!))" : nil
                ].compactMap { $0 }.joined(separator: " ")
                SyncLogService.shared.logEvent(tag: "diff", level: "INFO", message: parts)
            }
            // Skipped summary
            if !skippedImports.isEmpty || !skippedMerges.isEmpty {
                let sampleImports = skippedImports.prefix(50).map { [
                    "title": $0.title,
                    "calendar": $0.calendar,
                    "reason": $0.reason,
                    "tags": $0.tags,
                    "due": $0.due != nil ? isoFormatter.string(from: $0.due!) : nil
                ].compactMapValues { $0 } }
                let sampleMerges = skippedMerges.prefix(50).map { [
                    "title": $0.title,
                    "calendar": $0.calendar,
                    "reason": $0.reason,
                    "tags": $0.tags,
                    "due": $0.due != nil ? isoFormatter.string(from: $0.due!) : nil
                ].compactMapValues { $0 } }
                SyncLogService.shared.logSyncDetail(
                    direction: .diagnostics,
                    action: "syncDiff",
                    taskId: nil,
                    storyId: nil,
                    metadata: [
                        "firestoreOnlyCount": firestoreOnly.count,
                        "macOnlyCount": macOnly.count,
                        "skippedImportsCount": skippedImports.count,
                        "skippedMergesCount": skippedMerges.count,
                        "firestoreOnlySample": firestoreOnlySample,
                        "macOnlySample": macOnlySample,
                        "skippedImportsSample": sampleImports,
                        "skippedMergesSample": sampleMerges
                    ]
                )
            } else {
                SyncLogService.shared.logSyncDetail(
                    direction: .diagnostics,
                    action: "syncDiff",
                    taskId: nil,
                    storyId: nil,
                    metadata: [
                        "firestoreOnlyCount": firestoreOnly.count,
                        "macOnlyCount": macOnly.count
                    ]
                )
            }
            mark("title-diff")
        }

        // Title-based duplicate diagnostics in Firestore tasks (by normalized title)
        do {
            var groups: [String: [FbTask]] = [:]
            func normalizeTitle(_ text: String) -> String {
                return text.lowercased().replacingOccurrences(of: "\n", with: " ")
                    .components(separatedBy: CharacterSet.alphanumerics.inverted)
                    .filter { !$0.isEmpty }
                    .joined(separator: " ")
            }
            for taskItem in fetchedTasksForDiff where !isDone(taskItem.status) {
                let key = normalizeTitle(taskItem.title)
                if key.isEmpty { continue }
                groups[key, default: []].append(taskItem)
            }
            let dupGroups = groups.filter { $0.value.count > 1 }
            if !dupGroups.isEmpty {
                let count = dupGroups.count
                let sample = dupGroups.prefix(5).map { (titleKey, arr) in
                    let items = arr.prefix(3).map { [
                        "id": $0.id,
                        "ref": ($0.reference?.isEmpty == false ? $0.reference! : $0.id),
                        "list": $0.reminderListName ?? "",
                        "updatedAt": $0.updatedAt != nil ? isoFormatter.string(from: $0.updatedAt!) : ""
                    ] }
                    return ["title": titleKey, "items": items] as [String: Any]
                }
                SyncLogService.shared.logSyncDetail(
                    direction: .diagnostics,
                    action: "titleDuplicateDiagnostics",
                    taskId: nil,
                    storyId: nil,
                    metadata: ["groups": count, "sample": sample]
                )
            }
            mark("dedupe-diagnostics")
        }

        let pipelineElapsedMs = Int(Date().timeIntervalSince(syncStart) * 1000)
        let timeline = milestones.map { "\($0.0):\($0.1)ms" }.joined(separator: ", ")
        let summaryMessage = "Sync pipeline finished in \(pipelineElapsedMs)ms; created=\(created) updated=\(updated) " +
            "repairs=\(repairs) mergesToBob=\(mergesToBob) updatesFromBob=\(updatesFromBob) " +
            "firestoreOnly=\(firestoreOnlyCount) macOnly=\(macOnlyCount)"
        SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: summaryMessage)
        SyncLogService.shared.logEvent(
            tag: "sync",
            level: "INFO",
            message: "Timeline: \(timeline.isEmpty ? "none" : timeline)"
        )
        if !errors.isEmpty {
            let sampleErrors = errors.prefix(5).joined(separator: " | ")
            SyncLogService.shared.logEvent(tag: "sync", level: "ERROR", message: "Sync completed with errors (\(errors.count)): \(sampleErrors)")
        } else {
            SyncLogService.shared.logEvent(tag: "sync", level: "INFO", message: "Sync completed cleanly; no errors recorded.")
        }

        // Refresh the in-app open counts only during syncs (delta/full)
        await MainActor.run {
            Task { await OpenCountsModel.shared.refresh() }
        }

        // Persist summary and mode-specific timestamps
        let summaryPref = "created=\(created) updated=\(updated) stories=\(createdLinkedStories) themes=\(createdWithTheme)"
        await MainActor.run {
            UserPreferences.shared.lastSyncSummary = summaryPref
            UserPreferences.shared.lastSyncDate = Date()
            switch mode {
            case .full:
                UserPreferences.shared.lastFullSyncDate = Date()
            case .delta:
                UserPreferences.shared.lastDeltaSyncDate = Date()
            }
        }
        SyncLogService.shared.logSync(
            userId: user.uid,
            counts: (created: created, updated: updated),
            linkedStories: createdLinkedStories,
            themed: createdWithTheme,
            errors: errors
        )

        // Post-sync duplicate cleanup: always run local sweep; also call server dedupe every sync
        if !dryRun {
            let cleanup = await fullSweepAndRemoveDuplicates(hardDelete: true)
            SyncLogService.shared.logEvent(
                tag: "dedupe",
                level: cleanup.error == nil ? "INFO" : "ERROR",
                message: "Post-sync full sweep: deleted=\(cleanup.deleted) groups=\(cleanup.groups) error=\(cleanup.error ?? "none")"
            )
            // Always invoke server-side dedupe with title matching
            await runServerDedupe(hardDelete: true, includeTitleDedupe: true)
            await runServerCleanupDuplicates(forceImmediate: true)
        }

        let totalElapsedMs = Int(Date().timeIntervalSince(syncStart) * 1000)
        let summaryText = [
            "Sync complete in \(totalElapsedMs)ms",
            "created=\(created)",
            "updated=\(updated)",
            "repairs=\(repairs)",
            "mergesToBob=\(mergesToBob)",
            "updatesFromBob=\(updatesFromBob)",
            "errors=\(errors.count)"
        ].joined(separator: " ")
        SyncLogService.shared.logEvent(
            tag: "sync",
            level: "INFO",
            message: summaryText
        )
        return (created, updated, errors)
    }

    private func runServerDedupe(hardDelete: Bool, includeTitleDedupe: Bool) async {
        #if canImport(FirebaseFunctions)
        do {
            // Try multiple common regions in case Functions are deployed outside default region
            let regions = [nil, "us-central1", "europe-west1", "europe-west2"]
            var lastError: Error? = nil
            for region in regions {
                do {
                    let client = (region == nil) ? Functions.functions() : Functions.functions(region: region!)
                    let fn = client.httpsCallable("deduplicateTasks")
                    let payload: [String: Any] = [
                        "hardDelete": hardDelete,
                        "dryRun": false,
                        "includeTitleDedupe": includeTitleDedupe
                    ]
                    let res = try await fn.call(payload)
                    if let dict = res.data as? [String: Any] {
                        let processed = dict["processed"] ?? 0
                        let resolved = dict["duplicatesResolved"] ?? 0
                        let groups = dict["groups"]
                        let regionLabel = region ?? "default"
                        let groupsDescription = String(describing: groups)
                        let message = "Server dedupe[\(regionLabel)]: processed=\(processed) resolved=\(resolved) groups=\(groupsDescription)"
                        SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: message)
                        SyncLogService.shared.logSyncDetail(direction: .diagnostics, action: "serverDedupe", taskId: nil, storyId: nil, metadata: dict)
                        return
                    }
                } catch {
                    lastError = error
                    continue
                }
            }
            if let lastError { throw lastError }
        } catch {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Server dedupe failed: \(error.localizedDescription)")
        }
        #else
        SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "FirebaseFunctions not available; skipping server dedupe")
        #endif
    }

    private func runServerCleanupDuplicates(forceImmediate: Bool) async {
        #if canImport(FirebaseFunctions)
        do {
            let regions = [nil, "us-central1", "europe-west1", "europe-west2"]
            var lastError: Error? = nil
            for region in regions {
                do {
                    let client = (region == nil) ? Functions.functions() : Functions.functions(region: region!)
                    let fn = client.httpsCallable("cleanupDuplicateTasksNow")
                    let res = try await fn.call(["forceImmediate": forceImmediate])
                    if let dict = res.data as? [String: Any] {
                        let regionLabel = region ?? "default"
                        SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "Server duplicate cleanup[\(regionLabel)] OK")
                        SyncLogService.shared.logSyncDetail(direction: .diagnostics, action: "serverDuplicateCleanup", taskId: nil, storyId: nil, metadata: dict)
                        return
                    }
                } catch {
                    lastError = error
                    continue
                }
            }
            if let lastError { throw lastError }
        } catch {
            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: "Server duplicate cleanup failed: \(error.localizedDescription)")
        }
        #else
        SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: "FirebaseFunctions not available; skipping server duplicate cleanup")
        #endif
    }

    private func makeLocalTaskRef() -> String {
        let alphabet = Array("23456789ABCDEFGHJKMNPQRSTUVWXYZ")
        var body = ""
        for _ in 0..<6 {
            if let ch = alphabet.randomElement() {
                body.append(ch)
            }
        }
        return "TK-\(body)"
    }

    private func isDone(_ status: Any?) -> Bool {
        if let numberStatus = status as? NSNumber { return numberStatus.intValue == 2 }
        if let stringStatus = status as? String { return stringStatus.lowercased() == "done" || stringStatus == "2" }
        return false
    }
}
// swiftlint:enable cyclomatic_complexity function_body_length type_body_length

#else

actor FirebaseSyncService {
    static let shared = FirebaseSyncService()
    private init() {}
    enum SyncMode { case full, delta }
    func syncNow(mode: SyncMode = .full, targetCalendar: EKCalendar?) async -> (created: Int, updated: Int, errors: [String]) {
        return (0, 0, ["Firebase SDK not available"])
    }
}

#endif
