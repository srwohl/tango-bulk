// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_PINNED_LEDGER_H
#define TANGO_BULK_SRC_CORE_PINNED_LEDGER_H

#include <cstdint>

/// The private process-wide ledger for registered receive storage.
///
/// A Subscription's configured pinned budget is a planning constraint. This
/// ledger is the separate process-wide admission constraint: every registered
/// region, regardless of which owner requested it, contributes to one atomic
/// total. Callers pass the process limit explicitly when acquiring storage;
/// the limit is never hidden in an owner-local counter.
namespace TangoBulk::detail
{

class PinnedLedger
{
  public:
    /// An owning, move-only charge against the process-wide ledger.
    ///
    /// The reservation is acquired before an adapter maps memory. If mapping
    /// fails, normal destruction rolls the charge back. Moving transfers the
    /// one release obligation; a moved-from reservation is empty.
    class Reservation
    {
      public:
        Reservation() noexcept = default;
        ~Reservation() noexcept;

        Reservation(const Reservation &) = delete;
        Reservation &operator=(const Reservation &) = delete;

        Reservation(Reservation &&other) noexcept;
        Reservation &operator=(Reservation &&other) noexcept;

        /// Try to own `bytes` under the explicit process-wide limit.
        ///
        /// Failure is represented by an empty reservation and leaves the
        /// ledger unchanged. This operation performs no heap allocation.
        static Reservation try_acquire(std::uint64_t bytes,
                                        std::uint64_t process_limit) noexcept;

        /// Add another mapped range to this reservation atomically.
        ///
        /// This is used when a memory adapter reports more physical capacity
        /// than it was asked to map. Failure leaves both the reservation and
        /// the process total unchanged.
        bool extend(std::uint64_t bytes, std::uint64_t process_limit) noexcept;

        void reset() noexcept;

        explicit operator bool() const noexcept
        {
            return bytes_ != 0;
        }

        std::uint64_t bytes() const noexcept
        {
            return bytes_;
        }

      private:
        explicit Reservation(std::uint64_t bytes) noexcept : bytes_(bytes) {}

        std::uint64_t bytes_{0};
    };

    /// Compute a ring's exact logical registered bytes without wrapping.
    ///
    /// On failure `result` is left untouched. Keeping this beside the ledger
    /// makes planning and admission use one product rule.
    static bool checked_bytes(std::uint64_t frame_bytes,
                              std::uint32_t ring_depth,
                              std::uint64_t &result) noexcept;

    static std::uint64_t current() noexcept;

  private:
    static bool try_reserve(std::uint64_t bytes,
                            std::uint64_t process_limit) noexcept;

    static void release(std::uint64_t bytes) noexcept;

    /// Warn once for each configured limit that exceeds RLIMIT_MEMLOCK.
    static void check_memlock(std::uint64_t process_limit) noexcept;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_PINNED_LEDGER_H
