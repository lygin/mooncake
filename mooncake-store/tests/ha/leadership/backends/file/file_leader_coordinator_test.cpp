#include "ha/leadership/backends/file/file_leader_coordinator.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>

#include "ha/leadership/leader_coordinator_factory.h"

namespace mooncake {

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry);

namespace ha {
namespace backends {
namespace file {
namespace {

class FileLeaderCoordinatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char tmpl[] = "/tmp/mc_file_leader_XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(tmpl));
        dir_ = tmpl;
        path_ = dir_ + "/master.addr";
    }

    void TearDown() override {
        std::remove(path_.c_str());
        std::remove((path_ + ".tmp").c_str());
        rmdir(dir_.c_str());
    }

    // Same way the elector publishes: temp file + rename.
    void Publish(const std::string& content) {
        const std::string tmp = path_ + ".tmp";
        {
            std::ofstream out(tmp, std::ios::trunc);
            out << content;
        }
        ASSERT_EQ(0, std::rename(tmp.c_str(), path_.c_str()));
    }

    std::unique_ptr<FileLeaderCoordinator> Make() {
        auto c = std::make_unique<FileLeaderCoordinator>(HABackendSpec{
            .type = HABackendType::FILE,
            .connstring = path_,
        });
        EXPECT_EQ(ErrorCode::OK, c->Connect());
        return c;
    }

    std::string dir_;
    std::string path_;
};

TEST(FileLeaderAddressTest, ParsesHostPort) {
    EXPECT_EQ("10.0.0.1:30071",
              FileLeaderCoordinator::ParseLeaderAddress("10.0.0.1:30071"));
    EXPECT_EQ("10.0.0.1:30071",
              FileLeaderCoordinator::ParseLeaderAddress(" 10.0.0.1:30071\n"));
    EXPECT_EQ("host-a:1", FileLeaderCoordinator::ParseLeaderAddress("host-a:1"));
}

TEST(FileLeaderAddressTest, RejectsGarbage) {
    for (const char* bad :
         {"", "\n", "10.0.0.1", "10.0.0.1:", ":30071", "10.0.0.1:0",
          "10.0.0.1:65536", "10.0.0.1:30x71", "10.0.0.1:1 10.0.0.2:2"}) {
        EXPECT_FALSE(FileLeaderCoordinator::ParseLeaderAddress(bad))
            << "'" << bad << "'";
    }
}

TEST_F(FileLeaderCoordinatorTest, ConnectNeedsAbsolutePath) {
    FileLeaderCoordinator c(HABackendSpec{.type = HABackendType::FILE,
                                          .connstring = "relative/path"});
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, c.Connect());
}

TEST_F(FileLeaderCoordinatorTest, MissingFileMeansNoLeader) {
    auto c = Make();
    auto view = c->ReadCurrentView();
    ASSERT_TRUE(view.has_value());
    EXPECT_FALSE(view->has_value());
}

TEST_F(FileLeaderCoordinatorTest, VersionBumpsOnlyOnAddressChange) {
    auto c = Make();
    Publish("10.0.0.1:30071\n");
    auto v1 = c->ReadCurrentView();
    ASSERT_TRUE(v1 && v1->has_value());
    EXPECT_EQ("10.0.0.1:30071", (*v1)->leader_address);
    EXPECT_EQ(1, (*v1)->view_version);

    // Rewriting the same address is not a change.
    Publish("10.0.0.1:30071");
    auto again = c->ReadCurrentView();
    ASSERT_TRUE(again && again->has_value());
    EXPECT_EQ(1, (*again)->view_version);

    Publish("10.0.0.2:30071");
    auto v2 = c->ReadCurrentView();
    ASSERT_TRUE(v2 && v2->has_value());
    EXPECT_EQ("10.0.0.2:30071", (*v2)->leader_address);
    EXPECT_EQ(2, (*v2)->view_version);

    // Moving back is still a change, and the version keeps growing.
    Publish("10.0.0.1:30071");
    auto v3 = c->ReadCurrentView();
    ASSERT_TRUE(v3 && v3->has_value());
    EXPECT_EQ(3, (*v3)->view_version);
}

TEST_F(FileLeaderCoordinatorTest, GarbageIsAnErrorAndKeepsVersion) {
    auto c = Make();
    Publish("10.0.0.1:30071");
    ASSERT_TRUE(c->ReadCurrentView());

    Publish("not-an-address");
    auto bad = c->ReadCurrentView();
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, bad.error());

    Publish("10.0.0.1:30071");
    auto ok = c->ReadCurrentView();
    ASSERT_TRUE(ok && ok->has_value());
    EXPECT_EQ(1, (*ok)->view_version);
}

TEST_F(FileLeaderCoordinatorTest, WaitForViewChangeSeesMove) {
    auto c = Make();
    Publish("10.0.0.1:30071");
    auto first = c->WaitForViewChange(std::nullopt,
                                      std::chrono::milliseconds(1000));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(first->changed);
    const ViewVersionId known = first->current_view->view_version;

    auto idle = c->WaitForViewChange(known, std::chrono::milliseconds(300));
    ASSERT_TRUE(idle.has_value());
    EXPECT_FALSE(idle->changed);
    EXPECT_TRUE(idle->timed_out);

    std::thread mover([this] {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        Publish("10.0.0.2:30071");
    });
    auto moved = c->WaitForViewChange(known, std::chrono::milliseconds(5000));
    mover.join();
    ASSERT_TRUE(moved.has_value());
    ASSERT_TRUE(moved->changed);
    EXPECT_EQ("10.0.0.2:30071", moved->current_view->leader_address);
    EXPECT_GT(moved->current_view->view_version, known);
}

TEST_F(FileLeaderCoordinatorTest, CannotCampaign) {
    auto c = Make();
    auto acquired = c->TryAcquireLeadership("10.0.0.1:30071");
    ASSERT_FALSE(acquired.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, acquired.error());
    EXPECT_FALSE(HaBackendSupportsElection(HABackendType::FILE));
}

TEST_F(FileLeaderCoordinatorTest, ClientSpecAndFactory) {
    auto spec = ParseHABackendSpec("file://" + path_);
    ASSERT_TRUE(spec.has_value());
    ASSERT_TRUE(spec->has_value());
    EXPECT_EQ(HABackendType::FILE, (*spec)->type);
    EXPECT_EQ(path_, (*spec)->connstring);

    auto coordinator = CreateLeaderCoordinator(**spec);
    ASSERT_TRUE(coordinator.has_value());
    Publish("10.0.0.3:30071");
    auto view = (*coordinator)->ReadCurrentView();
    ASSERT_TRUE(view && view->has_value());
    EXPECT_EQ("10.0.0.3:30071", (*view)->leader_address);
}

}  // namespace
}  // namespace file
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
