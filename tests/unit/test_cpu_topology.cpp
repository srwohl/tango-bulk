// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/cpu_topology.h>

#include <catch2/catch_test_macros.hpp>

#include <sched.h>

#include <algorithm>
#include <vector>

/// The OS-level half of placement.
///
/// Every one of these has to pass on a machine with NUMA and on one without, in a
/// container that hides the topology and outside one, so almost nothing here can
/// assert a *value*. What they assert instead is that the failure mode is a
/// documented -1 rather than a wrong number, and -- the one that matters -- that
/// binding a thread actually moves it, because that is the property
/// `engine_cpu_affinity` silently lacked.
namespace
{

using namespace TangoBulk;
using namespace TangoBulk::detail;

/// Restore the calling thread's affinity, whatever a test did to it.
///
/// Catch2 runs every case in one process, so a test that pins the main thread and
/// walks away has skewed every case after it.
class AffinityGuard
{
  public:
    AffinityGuard()
    {
        CPU_ZERO(&saved_);
        valid_ = ::sched_getaffinity(0, sizeof(saved_), &saved_) == 0;
    }

    ~AffinityGuard()
    {
        if(valid_)
        {
            ::sched_setaffinity(0, sizeof(saved_), &saved_);
        }
    }

    AffinityGuard(const AffinityGuard &) = delete;
    AffinityGuard &operator=(const AffinityGuard &) = delete;

  private:
    cpu_set_t saved_{};
    bool valid_{false};
};

/// CPUs this process is actually permitted to run on, which under a cgroup cpuset
/// is not the same as the CPUs the machine has.
std::vector<int> allowed_cpus()
{
    std::vector<int> cpus;
    cpu_set_t set;
    CPU_ZERO(&set);

    if(::sched_getaffinity(0, sizeof(set), &set) != 0)
    {
        return cpus;
    }

    for(int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
    {
        if(CPU_ISSET(static_cast<unsigned>(cpu), &set))
        {
            cpus.push_back(cpu);
        }
    }
    return cpus;
}

} // namespace

TEST_CASE("numa_node_of answers for real memory and refuses to guess otherwise", "[topology]")
{
    std::vector<unsigned char> touched(64 * 1024, 0x5A);

    // A node index or -1, never anything else.  -1 is legitimate: a kernel
    // without NUMA, a container that hides it, or a page not yet faulted.
    const int node = numa_node_of(touched.data());
    CHECK(node >= -1);

    // Same page, same answer.  A value that moved between two calls would mean
    // this is reporting something other than page placement.
    CHECK(numa_node_of(touched.data()) == node);

    CHECK(numa_node_of(nullptr) == -1);
}

TEST_CASE("current_cpu reports a CPU this thread is allowed to run on", "[topology]")
{
    const int cpu = current_cpu();
    CHECK(cpu >= -1);

    if(cpu >= 0)
    {
        const std::vector<int> allowed = allowed_cpus();
        if(!allowed.empty())
        {
            // Not merely "in range": in the *permitted* set.  A number outside it
            // would mean the report is fabricated rather than sampled.
            CHECK(std::find(allowed.begin(), allowed.end(), cpu) != allowed.end());
        }
    }
}

TEST_CASE("allowed_cpu_count reflects the cpuset, not the hardware", "[topology]")
{
    const std::size_t count = allowed_cpu_count();
    CHECK(count == allowed_cpus().size());
}

TEST_CASE("bind_thread_to_cpu moves the thread, and -1 leaves it alone", "[topology]")
{
    const AffinityGuard guard;
    const std::vector<int> allowed = allowed_cpus();

    if(allowed.empty())
    {
        SUCCEED("no affinity information available on this host");
        return;
    }

    // -1 is the documented "leave it alone", and it must not be an error: it is
    // the default, and the default exists so a launcher-level placement is never
    // fought.
    REQUIRE(bind_thread_to_cpu(-1) == Status::Ok);
    CHECK(allowed_cpu_count() == allowed.size());

    // The regression guard this file exists for.  `engine_cpu_affinity` was a
    // public field that nothing read, and no test could tell -- because nothing
    // asserted that asking to be pinned results in being pinned.
    const int target = allowed.back();
    REQUIRE(bind_thread_to_cpu(target) == Status::Ok);
    CHECK(allowed_cpu_count() == 1);
    CHECK(current_cpu() == target);
}

TEST_CASE("bind_thread_to_cpu reports a refusal instead of pretending", "[topology]")
{
    const AffinityGuard guard;

    // CPU_SETSIZE-1 is almost certainly not a CPU this machine has, and the
    // kernel refuses an empty resulting mask.  A device server must not fail to
    // start because it could not optimise itself, so this is a status and not an
    // exception -- but it must not be Ok either, or a typo in a config file would
    // read as success.
    CHECK(bind_thread_to_cpu(CPU_SETSIZE - 1) == Status::Internal);
}
