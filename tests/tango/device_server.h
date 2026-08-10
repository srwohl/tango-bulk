// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_TANGO_DEVICE_SERVER_H
#define TANGO_BULK_TESTS_TANGO_DEVICE_SERVER_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

/// Start `fixture_device` as a real, separate device server process.
///
/// No Tango database: the server runs `-nodb -dlist`, which is what lets the M4
/// suite run on a machine that has cppTango installed and nothing else
/// configured.  The alternative -- linking a device object into the test binary
/// -- would prove the adapter compiles and nothing about whether a
/// `DeviceProxy` can drive it.
///
/// One server for the whole executable, started on first use and reaped by the
/// static destructor.  Starting one per test case would add several seconds of
/// CORBA wireup to every case and prove nothing extra.
namespace TangoBulkTests
{

class DeviceServer
{
  public:
    static DeviceServer &instance()
    {
        static DeviceServer server;
        return server;
    }

    /// A device name a stock `Tango::DeviceProxy` accepts with no database.
    const std::string &device() const noexcept
    {
        return device_;
    }

    DeviceServer(const DeviceServer &) = delete;
    DeviceServer &operator=(const DeviceServer &) = delete;

  private:
    /// Ask the kernel for a port nobody is using, then let it go.
    ///
    /// Racy in principle and fine in practice: the window is microseconds and
    /// the alternative -- a hard-coded port -- fails whenever two checkouts
    /// build at once, which is not a rare event on a shared machine.
    static int free_port()
    {
        const int socket_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if(socket_fd < 0)
        {
            throw std::runtime_error("could not create a socket to find a free port");
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        address.sin_port = 0;

        socklen_t length = sizeof(address);
        if(::bind(socket_fd, reinterpret_cast<sockaddr *>(&address), length) != 0 ||
           ::getsockname(socket_fd, reinterpret_cast<sockaddr *>(&address), &length) != 0)
        {
            ::close(socket_fd);
            throw std::runtime_error("could not find a free port");
        }

        const int port = ::ntohs(address.sin_port);
        ::close(socket_fd);
        return port;
    }

    DeviceServer()
    {
        const int port = free_port();
        const std::string endpoint = "giop:tcp:127.0.0.1:" + std::to_string(port);

        int pipe_fds[2] = {-1, -1};
        if(::pipe(pipe_fds) != 0)
        {
            throw std::runtime_error("could not create a pipe for the device server");
        }

        pid_ = ::fork();
        if(pid_ < 0)
        {
            throw std::runtime_error("could not fork the device server");
        }

        if(pid_ == 0)
        {
            // Child.  The readiness channel is descriptor 3, deliberately not
            // stdout: cppTango takes `std::cout` over for its own logging in
            // Util::init(), so a line printed there would never reach this
            // pipe.  stdout and stderr stay attached to the test's, which is
            // what makes a server that fails to start visible in the log.
            ::close(pipe_fds[0]);
            if(pipe_fds[1] != 3)
            {
                ::dup2(pipe_fds[1], 3);
                ::close(pipe_fds[1]);
            }

            std::vector<std::string> arguments{TANGO_BULK_FIXTURE_DEVICE,
                                               "test",
                                               "-nodb",
                                               "-dlist",
                                               "test/bulk/1",
                                               "-ORBendPoint",
                                               endpoint};

            std::vector<char *> argv;
            argv.reserve(arguments.size() + 1);
            for(std::string &argument : arguments)
            {
                argv.push_back(argument.data());
            }
            argv.push_back(nullptr);

            ::execv(TANGO_BULK_FIXTURE_DEVICE, argv.data());
            ::_exit(127); // execv only returns on failure
        }

        ::close(pipe_fds[1]);
        await_ready(pipe_fds[0]);
        ::close(pipe_fds[0]);

        device_ = "127.0.0.1:" + std::to_string(port) + "/test/bulk/1#dbase=no";
    }

    ~DeviceServer()
    {
        if(pid_ > 0)
        {
            ::kill(pid_, SIGTERM);

            // Give it a moment to run its own teardown -- which is the part that
            // destroys the publisher and unmaps its ring -- then insist.
            for(int i = 0; i < 100; ++i)
            {
                int status = 0;
                if(::waitpid(pid_, &status, WNOHANG) == pid_)
                {
                    return;
                }
                ::usleep(50'000);
            }

            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
        }
    }

    /// Block until the server prints READY, or give up loudly.
    ///
    /// Reading a line the server prints after `server_init()` beats sleeping:
    /// CORBA wireup takes as long as it takes, and a sleep that is long enough
    /// on this machine is a flake on a busier one.
    void await_ready(int fd)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        std::string output;

        while(std::chrono::steady_clock::now() < deadline)
        {
            pollfd descriptor{};
            descriptor.fd = fd;
            descriptor.events = POLLIN;

            const int ready = ::poll(&descriptor, 1, 250);
            if(ready < 0)
            {
                break;
            }
            if(ready == 0)
            {
                continue;
            }

            std::array<char, 512> buffer{};
            const ssize_t bytes = ::read(fd, buffer.data(), buffer.size());
            if(bytes <= 0)
            {
                break; // The server closed its stdout, which means it died.
            }

            output.append(buffer.data(), static_cast<std::size_t>(bytes));
            if(output.find("READY") != std::string::npos)
            {
                return;
            }
        }

        ::kill(pid_, SIGKILL);
        int status = 0;
        ::waitpid(pid_, &status, 0);
        pid_ = -1;

        throw std::runtime_error("the fixture device server did not become ready; it said: " +
                                 output);
    }

    pid_t pid_{-1};
    std::string device_;
};

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_TANGO_DEVICE_SERVER_H
