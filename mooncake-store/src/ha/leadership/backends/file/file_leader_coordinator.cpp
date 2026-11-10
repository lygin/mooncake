#include "ha/leadership/backends/file/file_leader_coordinator.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>

#include <glog/logging.h>

namespace mooncake {
namespace ha {
namespace backends {
namespace file {

namespace {

constexpr auto kViewChangePollInterval = std::chrono::milliseconds(200);
// An address file is one short line; anything longer is not ours.
constexpr size_t kMaxFileBytes = 4096;

enum class ReadResult { kOk, kMissing, kError };

ReadResult ReadSmallFile(const std::string& path, std::string* content,
                         int* err) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        *err = errno;
        return errno == ENOENT ? ReadResult::kMissing : ReadResult::kError;
    }
    content->clear();
    char buf[512];
    while (content->size() <= kMaxFileBytes) {
        const ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            *err = errno;
            ::close(fd);
            return ReadResult::kError;
        }
        if (n == 0) {
            break;
        }
        content->append(buf, static_cast<size_t>(n));
    }
    ::close(fd);
    if (content->size() > kMaxFileBytes) {
        *err = EFBIG;
        return ReadResult::kError;
    }
    return ReadResult::kOk;
}

}  // namespace

FileLeaderCoordinator::FileLeaderCoordinator(const HABackendSpec& spec)
    : path_(spec.connstring) {}

ErrorCode FileLeaderCoordinator::Connect() {
    if (path_.empty() || path_.front() != '/') {
        LOG(ERROR) << "file HA backend needs an absolute path, got '"
                   << path_ << "'";
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

std::optional<std::string> FileLeaderCoordinator::ParseLeaderAddress(
    const std::string& content) {
    const auto is_space = [](char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    };
    const auto begin = std::find_if_not(content.begin(), content.end(),
                                        is_space);
    const auto end =
        std::find_if_not(content.rbegin(), content.rend(), is_space).base();
    if (begin >= end) {
        return std::nullopt;
    }
    std::string addr(begin, end);
    if (std::any_of(addr.begin(), addr.end(), is_space)) {
        return std::nullopt;
    }

    const auto colon = addr.rfind(':');
    if (colon == std::string::npos || colon == 0 ||
        colon + 1 == addr.size()) {
        return std::nullopt;
    }
    int port = 0;
    for (size_t i = colon + 1; i < addr.size(); ++i) {
        if (addr[i] < '0' || addr[i] > '9') {
            return std::nullopt;
        }
        port = port * 10 + (addr[i] - '0');
        if (port > 65535) {
            return std::nullopt;
        }
    }
    if (port == 0) {
        return std::nullopt;
    }
    return addr;
}

tl::expected<std::optional<MasterView>, ErrorCode>
FileLeaderCoordinator::ReadCurrentView() {
    std::string content;
    int err = 0;
    switch (ReadSmallFile(path_, &content, &err)) {
        case ReadResult::kMissing:
            return std::optional<MasterView>{std::nullopt};
        case ReadResult::kError:
            LOG(WARNING) << "read leader address file '" << path_
                         << "' failed: " << strerror(err);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        case ReadResult::kOk:
            break;
    }

    auto addr = ParseLeaderAddress(content);
    if (!addr) {
        LOG(WARNING) << "leader address file '" << path_
                     << "' does not hold <host>:<port>, keeping the "
                        "current master";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (*addr != last_address_) {
        ++last_version_;
        LOG(INFO) << "leader address in '" << path_ << "' is now " << *addr
                  << " (was '" << last_address_
                  << "'), view_version=" << last_version_;
        last_address_ = *addr;
    }
    return std::optional<MasterView>{MasterView{
        .leader_address = last_address_,
        .view_version = last_version_,
    }};
}

tl::expected<AcquireLeadershipResult, ErrorCode>
FileLeaderCoordinator::TryAcquireLeadership(
    const std::string& leader_address) {
    (void)leader_address;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<bool, ErrorCode> FileLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    (void)session;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<ViewChangeResult, ErrorCode>
FileLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        auto current_view = ReadCurrentView();
        if (!current_view) {
            return tl::make_unexpected(current_view.error());
        }

        if (!IsSameViewVersion(current_view.value(), known_version)) {
            return ViewChangeResult{
                .changed = true,
                .timed_out = false,
                .current_view = current_view.value(),
            };
        }

        if (timeout <= std::chrono::milliseconds::zero() ||
            std::chrono::steady_clock::now() >= deadline) {
            return ViewChangeResult{
                .changed = false,
                .timed_out = true,
                .current_view = std::nullopt,
            };
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
        std::this_thread::sleep_for(
            std::min(kViewChangePollInterval, remaining));
    }
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
FileLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    (void)session;
    (void)on_leadership_lost;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode FileLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    (void)session;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

bool FileLeaderCoordinator::IsSameViewVersion(
    const std::optional<MasterView>& current_view,
    std::optional<ViewVersionId> known_version) {
    if (!current_view.has_value() && !known_version.has_value()) {
        return true;
    }
    if (!current_view.has_value() || !known_version.has_value()) {
        return false;
    }
    return current_view->view_version == known_version.value();
}

}  // namespace file
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
