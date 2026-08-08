// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/protocol.h>

#include "core/byte_order.h"

#include <cerrno>
#include <cstdio>
#include <cstring>

#include <sys/random.h>
#include <unistd.h>

namespace TangoBulk::Protocol
{

namespace
{

[[noreturn]] void throw_rng_failure(const char *what, int err)
{
    BulkError error;
    error.status = Status::Internal;
    error.origin = "protocol";
    error.message = std::string{"cannot obtain cryptographic randomness: "} + what +
                    ": " + std::strerror(err);
    throw BulkException{std::move(error)};
}

/// Fill `out` from the system CSPRNG.
///
/// getrandom(2) first, /dev/urandom as the fallback for a kernel or container
/// that does not provide it.  std::random_device is deliberately not used: on
/// some libstdc++ configurations it is a deterministic PRNG, and a session id
/// that can be predicted makes a late frame from a dead session
/// indistinguishable from a live one.
void fill_random(std::byte *out, std::size_t size)
{
    std::size_t filled = 0;

    while(filled < size)
    {
        const ssize_t got = ::getrandom(out + filled, size - filled, 0);

        if(got > 0)
        {
            filled += static_cast<std::size_t>(got);
            continue;
        }

        if(got < 0 && errno == EINTR)
        {
            continue;
        }

        if(got < 0 && (errno == ENOSYS || errno == EPERM))
        {
            break; // fall through to /dev/urandom
        }

        throw_rng_failure("getrandom", errno);
    }

    if(filled == size)
    {
        return;
    }

    std::FILE *urandom = std::fopen("/dev/urandom", "rb");
    if(urandom == nullptr)
    {
        throw_rng_failure("open /dev/urandom", errno);
    }

    const std::size_t read = std::fread(out + filled, 1, size - filled, urandom);
    const int read_errno = errno;
    std::fclose(urandom);

    if(read != size - filled)
    {
        throw_rng_failure("read /dev/urandom", read_errno);
    }
}

std::uint64_t random_u64()
{
    std::array<std::byte, 8> bytes{};
    fill_random(bytes.data(), bytes.size());
    return wire::get64(bytes.data());
}

char hex_digit(unsigned value) noexcept
{
    return static_cast<char>(value < 10 ? '0' + static_cast<int>(value)
                                        : 'a' + static_cast<int>(value - 10));
}

std::string hex_of(const std::byte *data, std::size_t size)
{
    std::string out;
    out.reserve(size * 2);

    for(std::size_t i = 0; i < size; ++i)
    {
        const auto value = static_cast<unsigned>(wire::get8(data + i));
        out.push_back(hex_digit(value >> 4));
        out.push_back(hex_digit(value & 0xFu));
    }

    return out;
}

/// Eight hex characters plus an ellipsis.
///
/// Truncation is not cosmetic.  A full session id in a log is a credential for
/// the coordination plane, and a stream id in a log is one for the data plane.
std::string truncate_for_log(std::string hex)
{
    constexpr std::size_t k_logged_chars = 8;

    if(hex.size() > k_logged_chars)
    {
        hex.resize(k_logged_chars);
        hex += "…";
    }

    return hex;
}

bool all_zero(const std::byte *data, std::size_t size) noexcept
{
    for(std::size_t i = 0; i < size; ++i)
    {
        if(data[i] != std::byte{0})
        {
            return false;
        }
    }

    return true;
}

} // namespace

bool SessionId::is_zero() const noexcept
{
    return all_zero(bytes.data(), bytes.size());
}

bool operator==(const SessionId &a, const SessionId &b) noexcept
{
    return a.bytes == b.bytes;
}

bool operator!=(const SessionId &a, const SessionId &b) noexcept
{
    return !(a == b);
}

bool ClientInstanceId::is_zero() const noexcept
{
    return all_zero(bytes.data(), bytes.size());
}

bool operator==(const ClientInstanceId &a, const ClientInstanceId &b) noexcept
{
    return a.bytes == b.bytes;
}

bool operator!=(const ClientInstanceId &a, const ClientInstanceId &b) noexcept
{
    return !(a == b);
}

std::string to_hex(const SessionId &id)
{
    return hex_of(id.bytes.data(), id.bytes.size());
}

std::string to_hex(const ClientInstanceId &id)
{
    return hex_of(id.bytes.data(), id.bytes.size());
}

std::string to_log_string(const SessionId &id)
{
    return truncate_for_log(to_hex(id));
}

std::string to_log_string(const ClientInstanceId &id)
{
    return truncate_for_log(to_hex(id));
}

std::string to_log_string(StreamId id)
{
    std::array<std::byte, 8> bytes{};
    wire::put64(bytes.data(), id);
    return truncate_for_log(hex_of(bytes.data(), bytes.size()));
}

SessionId generate_session_id()
{
    SessionId id;

    // An all-zero session_id is the "server-wide status" selector in Query, so
    // it must never be issued as a real identifier.  The retry costs nothing and
    // removes a case that would otherwise be one in 2^128 and impossible to test.
    do
    {
        fill_random(id.bytes.data(), id.bytes.size());
    } while(id.is_zero());

    return id;
}

ClientInstanceId generate_client_instance_id()
{
    ClientInstanceId id;

    do
    {
        fill_random(id.bytes.data(), id.bytes.size());
    } while(id.is_zero());

    return id;
}

StreamId generate_stream_id()
{
    StreamId id = 0;

    // Zero is reserved as a local "no stream" sentinel for the same reason.
    do
    {
        id = random_u64();
    } while(id == 0);

    return id;
}

std::uint64_t generate_server_epoch_id()
{
    std::uint64_t id = 0;

    do
    {
        id = random_u64();
    } while(id == 0);

    return id;
}

std::uint64_t generate_probe_token()
{
    std::uint64_t token = 0;

    do
    {
        token = random_u64();
    } while(token == 0);

    return token;
}

} // namespace TangoBulk::Protocol
