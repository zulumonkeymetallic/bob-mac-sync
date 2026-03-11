import AppKit
import SwiftUI

// swiftlint:disable:next type_body_length
struct SettingsBarGearMenu: View {
    @EnvironmentObject var remindersData: RemindersData
    @ObservedObject var userPreferences = UserPreferences.shared
    @Environment(\.colorSchemeContrast) private var colorSchemeContrast

    @State var gearIsHovered = false
    @ObservedObject var appUpdateCheckHelper = AppUpdateCheckHelper.shared
    @ObservedObject var keyboardShortcutService = KeyboardShortcutService.shared
    @ObservedObject var manualSyncService = ManualSyncService.shared

    var body: some View {
        Menu {
            VStack {
                if appUpdateCheckHelper.isOutdated {
                    Button(action: {
                        if let url = URL(string: GithubConstants.latestReleasePage) {
                            NSWorkspace.shared.open(url)
                        }
                    }) {
                        Image(systemName: "exclamationmark.circle")
                        Text(rmbLocalized(.updateAvailableNoticeButton))
                    }

                    Divider()
                }

                // === SYNC CONTROLS ===
                Button("Full Sync with Bob") {
                    ManualSyncService.shared.triggerWithMode(reason: "Settings Menu - Full Sync", mode: .full)
                }
                .disabled(manualSyncService.isSyncing)
                
                Button("Delta Sync with Bob") {
                    ManualSyncService.shared.triggerWithMode(reason: "Settings Menu - Delta Sync", mode: .delta)
                }
                .disabled(manualSyncService.isSyncing)

                Button(action: {
                    userPreferences.enableBackgroundSync.toggle()
                    BackgroundSyncService.shared.applyPreference()
                }) {
                    SelectableView(
                        title: "Enable Background Sync",
                        isSelected: userPreferences.enableBackgroundSync
                    )
                }

                Button(action: {
                    if userPreferences.syncStories {
                        promptDisableStorySync()
                    } else {
                        userPreferences.syncStories = true
                    }
                }) {
                    SelectableView(
                        title: "Sync Standalone Stories",
                        isSelected: userPreferences.syncStories
                    )
                }

                Divider()

                // === AUTHENTICATION ===
                Button(action: {
                    FirebaseAuthView.showWindow()
                }) {
                    Label("Sign In to Bob…", systemImage: "person.crop.circle.badge.checkmark")
                }
                
                Button(action: {
                    userPreferences.launchAtLoginIsEnabled.toggle()
                }) {
                    let isSelected = userPreferences.launchAtLoginIsEnabled
                    SelectableView(
                        title: rmbLocalized(.launchAtLoginOptionButton),
                        isSelected: isSelected,
                        withPadding: false
                    )
                }

                Divider()

                // === METADATA DISPLAY OPTIONS (inline) ===
                Button(action: {
                    userPreferences.showBobMetadataInNotes = false
                }) {
                    SelectableView(
                        title: "No Metadata in Notes",
                        isSelected: !userPreferences.showBobMetadataInNotes,
                        withPadding: false
                    )
                }

                Button(action: {
                    userPreferences.showBobMetadataInNotes = true
                    userPreferences.metadataDetailLevel = .full
                }) {
                    SelectableView(
                        title: "Full Metadata Detail",
                        isSelected: userPreferences.showBobMetadataInNotes &&
                            userPreferences.metadataDetailLevel == .full,
                        withPadding: false
                    )
                }
                
                Button(action: {
                    userPreferences.showBobMetadataInNotes = true
                    userPreferences.metadataDetailLevel = .minimal
                }) {
                    SelectableView(
                        title: "Minimal Metadata Detail",
                        isSelected: userPreferences.showBobMetadataInNotes &&
                            userPreferences.metadataDetailLevel == .minimal,
                        withPadding: false
                    )
                }

                Divider()

                visualCustomizationOptions()

                Divider()

                // === LOGS & MAINTENANCE ===
                Button("Open Sync Log") {
                    SyncLogService.shared.revealLogInFinder()
                }

                Button("Open Log Folder") {
                    SyncLogService.shared.openLogsFolder()
                }

                Button("Mark Duplicates Complete (TTL)") {
                    Task {
                        let res = await FirebaseSyncService.shared.deleteAllDuplicates(hardDelete: false)
                        if let err = res.error {
                            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: err)
                            await MainActor.run {
                                SyncFeedbackService.shared.show(message: "Dedupe failed: \(err)")
                            }
                        } else {
                            let msg = "Completed \(res.deleted) duplicates across \(res.groups) groups"
                            SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: msg)
                            await MainActor.run {
                                SyncFeedbackService.shared.show(message: msg)
                            }
                        }
                    }
                }
                
                Button("Delete Duplicates Now (Hard Delete)") {
                    Task {
                        let res = await FirebaseSyncService.shared.deleteAllDuplicates(hardDelete: true)
                        if let err = res.error {
                            SyncLogService.shared.logEvent(tag: "dedupe", level: "ERROR", message: err)
                            await MainActor.run {
                                SyncFeedbackService.shared.show(message: "Dedupe failed: \(err)")
                            }
                        } else {
                            let msg = "Deleted \(res.deleted) duplicates across \(res.groups) groups"
                            SyncLogService.shared.logEvent(tag: "dedupe", level: "INFO", message: msg)
                            await MainActor.run {
                                SyncFeedbackService.shared.show(message: msg)
                            }
                        }
                    }
                }

                Button(action: { userPreferences.syncDryRun.toggle() }) {
                    SelectableView(title: "Dry-Run Mode (no writes)", isSelected: userPreferences.syncDryRun)
                }
                
                if let summary = UserPreferences.shared.lastSyncSummary, !summary.isEmpty {
                    Divider()
                    Text("Last Sync: \(summary)")
                        .font(.footnote)
                }

                Divider()
                
                // === SHORTCUTS & TOOLS ===
                Button {
                    KeyboardShortcutView.showWindow()
                } label: {
                    let activeShortcut = keyboardShortcutService.activeShortcut(for: .openRemindersMenuBar)
                    let activeShortcutText = Text(verbatim: "     \(activeShortcut)").foregroundColor(.gray)
                    Text(rmbLocalized(.keyboardShortcutOptionButton)) + activeShortcutText
                }

                Divider()

                Button(action: {
                    Task {
                        await remindersData.update()
                    }
                }) {
                    Text(rmbLocalized(.reloadRemindersDataButton))
                }

                Divider()

                Button(action: {
                    AboutView.showWindow()
                }) {
                    Text(rmbLocalized(.appAboutButton))
                }

                Button(action: {
                    NSApplication.shared.terminate(self)
                }) {
                    Text(rmbLocalized(.appQuitButton))
                }
            }
        } label: {
            Image(systemName: appUpdateCheckHelper.isOutdated ? "exclamationmark.circle" : "gear")
        }
        .menuStyle(BorderlessButtonMenuStyle())
        .frame(width: 32, height: 16)
        .padding(3)
        .background(gearIsHovered ? Color.rmbColor(for: .buttonHover, and: colorSchemeContrast) : nil)
        .cornerRadius(4)
        .onHover { isHovered in
            gearIsHovered = isHovered
        }
        .help(rmbLocalized(.settingsButtonHelp))
    }

    @ViewBuilder
    func visualCustomizationOptions() -> some View {
        Divider()

        appAppearanceMenu()

        menuBarIconMenu()

        menuBarCounterMenu()

        preferredLanguageMenu()

        Divider()
    }

    func appAppearanceMenu() -> some View {
        Menu {
            ForEach(RmbColorScheme.allCases, id: \.rawValue) { colorScheme in
                Button(action: { userPreferences.rmbColorScheme = colorScheme }) {
                    let isSelected = colorScheme == userPreferences.rmbColorScheme
                    SelectableView(title: colorScheme.title, isSelected: isSelected)
                }
            }

            Divider()

            let isIncreasedContrastEnabled = colorSchemeContrast == .increased
            let isTransparencyEnabled = userPreferences.backgroundIsTransparent && !isIncreasedContrastEnabled

            Button(action: {
                userPreferences.backgroundIsTransparent = false
            }) {
                let isSelected = !isTransparencyEnabled
                SelectableView(
                    title: rmbLocalized(.appAppearanceMoreOpaqueOptionButton),
                    isSelected: isSelected
                )
            }
            .disabled(isIncreasedContrastEnabled)

            Button(action: {
                userPreferences.backgroundIsTransparent = true
            }) {
                let isSelected = isTransparencyEnabled
                SelectableView(
                    title: rmbLocalized(.appAppearanceMoreTransparentOptionButton),
                    isSelected: isSelected
                )
            }
            .disabled(isIncreasedContrastEnabled)
        } label: {
            Text(rmbLocalized(.appAppearanceMenu))
        }
    }

    func menuBarIconMenu() -> some View {
        Menu {
            ForEach(RmbIcon.allCases, id: \.self) { icon in
                Button(action: {
                    userPreferences.reminderMenuBarIcon = icon
                    AppDelegate.shared?.loadMenuBarIcon()
                }) {
                    Image(nsImage: icon.image)
                    Text(icon.name)
                }
            }
        } label: {
            Text(rmbLocalized(.menuBarIconSettingsMenu))
        }
    }

    func menuBarCounterMenu() -> some View {
        Menu {
            ForEach(RmbMenuBarCounterType.allCases, id: \.rawValue) { counterType in
                Button(action: { userPreferences.menuBarCounterType = counterType }) {
                    let isSelected = counterType == userPreferences.menuBarCounterType
                    SelectableView(title: counterType.title, isSelected: isSelected)
                }
            }

            Divider()

            Button(action: {
                userPreferences.filterMenuBarCountByCalendar.toggle()
            }) {
                SelectableView(
                    title: rmbLocalized(.filterMenuBarCountByCalendarOptionButton),
                    isSelected: userPreferences.filterMenuBarCountByCalendar
                )
            }
        } label: {
            Text(rmbLocalized(.menuBarCounterSettingsMenu))
        }
    }

    func preferredLanguageMenu() -> some View {
        Menu {
            Button(action: {
                userPreferences.preferredLanguage = nil
            }) {
                let isSelected = userPreferences.preferredLanguage == nil
                SelectableView(
                    title: rmbLocalized(.preferredLanguageSystemOptionButton),
                    isSelected: isSelected
                )
            }

            Divider()

            ForEach(rmbAvailableLocales(), id: \.identifier) { locale in
                let localeIdentifier = locale.identifier
                Button(action: {
                    userPreferences.preferredLanguage = localeIdentifier
                }) {
                    let isSelected = userPreferences.preferredLanguage == localeIdentifier
                    SelectableView(title: locale.name, isSelected: isSelected)
                }
            }
        } label: {
            Text(rmbLocalized(.preferredLanguageMenu))
        }
    }

    private func promptDisableStorySync() {
        let alert = NSAlert()
        alert.messageText = "Turn off Story Sync?"
        alert.informativeText = "Disabling story sync stops new story reminders. " +
            "You can also remove existing story reminders from Reminders."
        alert.addButton(withTitle: "Turn Off Only")
        alert.addButton(withTitle: "Turn Off & Remove Story Reminders")
        alert.addButton(withTitle: "Cancel")
        let response = alert.runModal()
        switch response {
        case .alertFirstButtonReturn:
            userPreferences.syncStories = false
        case .alertSecondButtonReturn:
            userPreferences.syncStories = false
            Task {
                let result = await FirebaseSyncService.shared.removeStoryRemindersFromReminders()
                await MainActor.run {
                    SyncFeedbackService.shared.show(
                        message: "Removed \(result.removed) story reminders (skipped \(result.skipped))"
                    )
                }
            }
        default:
            break
        }
    }
}

struct SettingsBarGearMenu_Previews: PreviewProvider {
    static var previews: some View {
        SettingsBarGearMenu()
    }
}
