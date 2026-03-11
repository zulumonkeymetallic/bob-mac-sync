import EventKit

class PriorityParser {
    struct PriorityParserResult {
        private let range: NSRange
        let string: String
        let priority: EKReminderPriority

        var highlightedText: RmbHighlightedTextField.HighlightedText {
            RmbHighlightedTextField.HighlightedText(range: range, color: .systemRed)
        }

        init() {
            range = NSRange()
            string = ""
            priority = .none
        }

        init(range: NSRange, string: String, priority: EKReminderPriority) {
            self.range = range
            self.string = string
            self.priority = priority
        }
    }

    private init() {
        // This prevents others from using the default '()' initializer for this class.
    }

    private static func exclamationCount(_ string: Substring) -> Int {
        string.count(where: { $0 == "!" })
    }

    private static func priority(forExclamationCount count: Int) -> EKReminderPriority {
        switch count {
        case 3:
            .high
        case 2:
            .medium
        case 1:
            .low
        default:
            .none
        }
    }

    static func getPriority(from textString: String) -> PriorityParserResult? {
        guard let substringMatch = textString
                .split(separator: " ")
                .first(where: { $0.first == "!" && $0.count <= 3 && $0.count == exclamationCount($0) })
        else {
            return nil
        }

        return PriorityParserResult(
            range: NSRange(substringMatch.startIndex ..< substringMatch.endIndex, in: textString),
            string: String(substringMatch),
            priority: priority(forExclamationCount: substringMatch.count)
        )
    }
}
