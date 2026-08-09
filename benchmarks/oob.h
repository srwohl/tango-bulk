// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_BENCHMARKS_OOB_H
#define TANGO_BULK_BENCHMARKS_OOB_H

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

/// A length-prefixed blob channel over TCP, standing in for a Tango command.
///
/// `BulkPublisher::handle_coordination()` takes encoded bytes and returns
/// encoded bytes, which is the whole of the coordination contract -- 7.1 makes
/// `BulkOpen` / `BulkRenew` / `BulkClose` ordinary commands carrying a
/// `DevVarCharArray`, and the M4 adapter is a wrapper over exactly this call.
/// Until that adapter exists, something has to carry the blobs between two
/// processes, and 96 lines of TCP is that something.
///
/// It is deliberately not a transport: no framing beyond a u32 length, no
/// retries, no concurrency.  A benchmark that needed more than this from its
/// control plane would be measuring the wrong thing.
namespace TangoBulkBench
{

class OobChannel
{
  public:
    OobChannel() = default;

    ~OobChannel()
    {
        close_fd(fd_);
        close_fd(listen_fd_);
    }

    OobChannel(OobChannel &&other) noexcept :
        fd_(other.fd_),
        listen_fd_(other.listen_fd_)
    {
        other.fd_ = -1;
        other.listen_fd_ = -1;
    }

    OobChannel(const OobChannel &) = delete;
    OobChannel &operator=(const OobChannel &) = delete;
    OobChannel &operator=(OobChannel &&) = delete;

    /// Bind, listen, and accept exactly one peer.  Blocks until it arrives.
    static OobChannel accept_one(std::uint16_t port)
    {
        OobChannel channel;

        channel.listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if(channel.listen_fd_ < 0)
        {
            throw std::runtime_error("socket() failed");
        }

        int reuse = 1;
        ::setsockopt(channel.listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);

        if(::bind(channel.listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        {
            throw std::runtime_error("bind() failed on port " + std::to_string(port));
        }
        if(::listen(channel.listen_fd_, 1) < 0)
        {
            throw std::runtime_error("listen() failed");
        }

        channel.fd_ = ::accept(channel.listen_fd_, nullptr, nullptr);
        if(channel.fd_ < 0)
        {
            throw std::runtime_error("accept() failed");
        }

        set_nodelay(channel.fd_);
        return channel;
    }

    static OobChannel connect_to(const std::string &host, std::uint16_t port)
    {
        OobChannel channel;

        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo *result = nullptr;
        const std::string service = std::to_string(port);
        if(::getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0 || result == nullptr)
        {
            throw std::runtime_error("could not resolve " + host);
        }

        channel.fd_ = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        const bool ok =
            channel.fd_ >= 0 && ::connect(channel.fd_, result->ai_addr, result->ai_addrlen) == 0;
        ::freeaddrinfo(result);

        if(!ok)
        {
            throw std::runtime_error("could not connect to " + host + ":" + service);
        }

        set_nodelay(channel.fd_);
        return channel;
    }

    void send(const std::vector<std::byte> &payload) const
    {
        const auto length = static_cast<std::uint32_t>(payload.size());
        write_all(&length, sizeof(length));
        write_all(payload.data(), payload.size());
    }

    std::vector<std::byte> recv() const
    {
        std::uint32_t length = 0;
        read_all(&length, sizeof(length));

        // The coordination plane is bounded by 3.9 at 65 536 bytes; anything
        // larger is a desynchronised stream, not a big message.
        if(length > 65'536)
        {
            throw std::runtime_error("oob: implausible message length");
        }

        std::vector<std::byte> payload(length);
        read_all(payload.data(), payload.size());
        return payload;
    }

  private:
    static void set_nodelay(int fd)
    {
        int flag = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    }

    static void close_fd(int &fd)
    {
        if(fd >= 0)
        {
            ::close(fd);
            fd = -1;
        }
    }

    void write_all(const void *data, std::size_t size) const
    {
        const auto *p = static_cast<const char *>(data);
        while(size > 0)
        {
            const ssize_t n = ::send(fd_, p, size, MSG_NOSIGNAL);
            if(n <= 0)
            {
                throw std::runtime_error("oob: peer closed during send");
            }
            p += n;
            size -= static_cast<std::size_t>(n);
        }
    }

    void read_all(void *data, std::size_t size) const
    {
        auto *p = static_cast<char *>(data);
        while(size > 0)
        {
            const ssize_t n = ::recv(fd_, p, size, 0);
            if(n <= 0)
            {
                throw std::runtime_error("oob: peer closed during recv");
            }
            p += n;
            size -= static_cast<std::size_t>(n);
        }
    }

    int fd_{-1};
    int listen_fd_{-1};
};

} // namespace TangoBulkBench

#endif // TANGO_BULK_BENCHMARKS_OOB_H
