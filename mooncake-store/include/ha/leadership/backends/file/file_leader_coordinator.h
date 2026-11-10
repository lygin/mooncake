#pragma once

#include <mutex>
#include <optional>
#include <string>

#include "ha/leadership/leader_coordinator.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace file {

// Client-only leader discovery for masters elected outside Mooncake.
//
// The connstring is an absolute path (`file:///run/x/master.addr` on the
// client side). An external elector writes the current master's
// "<host>:<port>" into that file, replacing it atomically (write a temp file,
// then rename). Clients read it on every poll, so a master that moves is
// followed in place: the client reconnects and remounts its segments, and the
// process hosting it does not restart.
//
// The file carries no version. This coordinator assigns one per process: 1
// for the first address it reads, +1 every time the address changes. A
// client only compares versions produced by its own coordinator, so the
// numbers never go backwards and an address that moves back still counts as
// a change.
//
// File states:
//   - missing: no leader published yet (std::nullopt), so the initial
//     connect waits for the elector to write it;
//   - empty or not "<host>:<port>": an error. The client keeps using the
//     master it already has instead of detaching on a bad write.
//
// The election methods return UNAVAILABLE_IN_CURRENT_MODE; a master cannot
// campaign on this backend (see HaBackendSupportsElection).
class FileLeaderCoordinator final : public LeaderCoordinator {
   public:
    explicit FileLeaderCoordinator(const HABackendSpec& spec);
    ~FileLeaderCoordinator() override = default;

    // Validates the path only; the file itself may not exist yet.
    ErrorCode Connect();

    tl::expected<std::optional<MasterView>, ErrorCode> ReadCurrentView()
        override;

    tl::expected<AcquireLeadershipResult, ErrorCode> TryAcquireLeadership(
        const std::string& leader_address) override;

    tl::expected<bool, ErrorCode> RenewLeadership(
        const LeadershipSession& session) override;

    tl::expected<ViewChangeResult, ErrorCode> WaitForViewChange(
        std::optional<ViewVersionId> known_version,
        std::chrono::milliseconds timeout) override;

    tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
    StartLeadershipMonitor(const LeadershipSession& session,
                           LeadershipLostCallback on_leadership_lost) override;

    ErrorCode ReleaseLeadership(const LeadershipSession& session) override;

    // Parses "<host>:<port>" with optional surrounding whitespace. Returns
    // the trimmed address, or std::nullopt when it is not one. Exposed for
    // tests.
    static std::optional<std::string> ParseLeaderAddress(
        const std::string& content);

   private:
    static bool IsSameViewVersion(const std::optional<MasterView>& current_view,
                                  std::optional<ViewVersionId> known_version);

    const std::string path_;

    // The leader monitor thread and the heartbeat recovery path both read
    // the view, so the version state is guarded.
    std::mutex mutex_;
    std::string last_address_;
    ViewVersionId last_version_ = 0;
};

}  // namespace file
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
