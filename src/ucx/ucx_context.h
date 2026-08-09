// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_UCX_CONTEXT_H
#define TANGO_BULK_SRC_UCX_UCX_CONTEXT_H

#include <ucp/api/ucp.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/// RAII around the two UCX handles every engine needs.
///
/// Both are internal to `tango-bulk-ucx` and neither is ever returned by a
/// public accessor.  IMPLEMENTATION_SPEC.md 5.1 makes that a structural claim
/// rather than a convention: "the worker handle is a private member of the
/// engine, is never returned by any public accessor, and every cross-thread
/// request reaches it through a queue".
namespace TangoBulk::detail
{

/// A `ucp_context_h` configured for active messages.
class UcxContext
{
  public:
    /// `tls` maps to UCX_TLS.  Empty means let UCX choose, which is what
    /// production does; tests pin it so a run cannot silently fall back to a
    /// transport that answers a different question than the one being asked.
    ///
    /// Throws BulkException on failure -- this is the control path.
    explicit UcxContext(const std::string &tls);
    ~UcxContext();

    UcxContext(const UcxContext &) = delete;
    UcxContext &operator=(const UcxContext &) = delete;

    ucp_context_h get() const noexcept
    {
        return context_;
    }

  private:
    ucp_context_h context_{nullptr};
};

/// A `ucp_worker_h` in UCS_THREAD_MODE_SINGLE, plus its address.
///
/// Single thread mode is not an optimisation: 5.1 gives the worker to exactly
/// one thread, and asking UCX for a multi-threaded worker would buy locking for
/// a sharing pattern that does not exist.
class UcxWorker
{
  public:
    explicit UcxWorker(UcxContext &context);
    ~UcxWorker();

    UcxWorker(const UcxWorker &) = delete;
    UcxWorker &operator=(const UcxWorker &) = delete;

    ucp_worker_h get() const noexcept
    {
        return worker_;
    }

    /// This worker's address, for the peer to create an endpoint from.  Carried
    /// in `Open` / `OpenReply` as an opaque blob.
    const std::vector<std::byte> &address() const noexcept
    {
        return address_;
    }

    std::size_t max_am_header() const noexcept
    {
        return max_am_header_;
    }

    /// Create an endpoint to a peer address.  Caller owns the result and must
    /// close it before this worker is destroyed.
    ///
    /// UCP_ERR_HANDLING_MODE_NONE by default, per 6.3: PEER excludes every
    /// transport without a peer-failure handler, which is what produced the
    /// "10x problem" that turned out to be a tcp/lo fallback.  Liveness comes
    /// from session leases, so PEER buys nothing on the critical path.
    ucp_ep_h create_endpoint(const std::byte *peer_address, std::size_t size);

  private:
    ucp_worker_h worker_{nullptr};
    std::vector<std::byte> address_;
    std::size_t max_am_header_{0};
};

/// Throw a BulkException carrying a UCX status and the call that produced it.
[[noreturn]] void throw_ucx_error(const char *what, ucs_status_t status, const char *origin);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_UCX_CONTEXT_H
