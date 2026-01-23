import Foundation

extension UserDefaults {
    func boolWithDefaultValueTrue(forKey key: String) -> Bool {
        guard object(forKey: key) != nil else {
            return true
        }
        return bool(forKey: key)
    }
}
