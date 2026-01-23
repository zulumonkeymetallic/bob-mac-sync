import SwiftUI

enum RmbColorScheme: String, CaseIterable {
    case system
    case light
    case dark

    var colorScheme: ColorScheme? {
        switch self {
        case .system:
            nil
        case .light:
            .light
        case .dark:
            .dark
        }
    }

    var title: String {
        switch self {
        case .system:
            rmbLocalized(.appAppearanceColorSystemModeOptionButton)
        case .light:
            rmbLocalized(.appAppearanceColorLightModeOptionButton)
        case .dark:
            rmbLocalized(.appAppearanceColorDarkModeOptionButton)
        }
    }
}
