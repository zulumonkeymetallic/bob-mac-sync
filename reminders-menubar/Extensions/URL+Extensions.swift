import Foundation

extension URL {
    var displayedUrl: String {
        var displayedUrlString = absoluteString
        if absoluteString.starts(with: "http"), let host {
            displayedUrlString = host
        }
        return displayedUrlString.replacingOccurrences(of: "^www.", with: "", options: .regularExpression)
    }
}
