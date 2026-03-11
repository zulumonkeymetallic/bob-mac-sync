import SwiftUI

class AppUpdateCheckHelper: ObservableObject {
    static let shared = AppUpdateCheckHelper()

    private init() {
        // This prevents others from using the default '()' initializer for this class.

        checkLatestRelease()
    }

    private let updateScheduler: NSBackgroundActivityScheduler = {
        let activityScheduler = NSBackgroundActivityScheduler(identifier: AppConstants.mainBundleId + ".updatecheck")
        activityScheduler.repeats = true
        activityScheduler.interval = 10 * 60 * 60 // 10 hours
        return activityScheduler
    }()

    @Published private(set) var isOutdated = false

    private let currentRelease = Release(version: AppConstants.currentVersion)

    private(set) var latestRelease: Release? {
        didSet {
            guard let latestRelease else {
                return
            }

            DispatchQueue.main.async {
                self.isOutdated = self.currentRelease < latestRelease
            }
        }
    }

    func checkLatestRelease() {
        GithubService.getLatestRelease { result in
            switch result {
            case let .success(release):
                self.latestRelease = release
            case let .failure(error):
                print("Error getting latest release from github:", error.localizedDescription)
            }
        }
    }

    func startBackgroundActivity() {
        updateScheduler.schedule { completion in
            if self.updateScheduler.shouldDefer {
                completion(.deferred)
                return
            }

            self.checkLatestRelease()
            completion(.finished)
        }
    }
}
