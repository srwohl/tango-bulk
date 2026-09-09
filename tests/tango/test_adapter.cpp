// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "device_server.h"

#include <tango-bulk/protocol.h>
#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

/// M4 end to end: a stock device server, a stock `Tango::DeviceProxy`, and a
/// `Subscription` that carries its coordination plane over ordinary commands.
///
/// Everything here goes through the same door an application would use.  There
/// is no encoded-adapter call in this file and no `SubscriberEngine`:
/// if the adapter is wrong, these tests are how it shows.
namespace
{

using namespace TangoBulk;
using namespace TangoBulkTests;
using namespace std::chrono_literals;

constexpr std::uint64_t k_frame_bytes = 64u << 10;

/// The fixture device's frame pattern, computed independently of it.
unsigned char pattern_byte(unsigned seed, std::uint64_t i)
{
    return static_cast<unsigned char>((seed * 31u + static_cast<unsigned>(i)) & 0xFFu);
}

SubscriberConfig subscriber_config()
{
    SubscriberConfig config;
    config.stream_name = "bulk.tango";
    config.max_frame_bytes = k_frame_bytes;
    config.ring_depth = 8;
    config.credit_window = 4;
    config.delivery_queue_depth = 32;
    config.command_timeout_ms = 5'000;
    return config;
}

template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds budget = 10s)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(std::chrono::steady_clock::now() < deadline)
    {
        if(predicate())
        {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return predicate();
}

/// Everything a test needs to watch one subscriber, without a race of its own.
struct Sink
{
    std::mutex mutex;
    std::vector<FrameView> frames;
    std::vector<std::thread::id> callback_threads;
    std::atomic<std::size_t> frame_count{0};

    SubscriptionCallbacks callbacks()
    {
        SubscriptionCallbacks out;

        out.on_frame = [this](FrameView view)
        {
            std::lock_guard<std::mutex> lock(mutex);
            callback_threads.push_back(std::this_thread::get_id());
            frames.push_back(std::move(view));
            frame_count.fetch_add(1, std::memory_order_relaxed);
        };

        return out;
    }
};

Tango::DeviceData publish(Tango::DeviceProxy &proxy, Tango::DevLong count)
{
    Tango::DeviceData argument;
    argument << count;
    return proxy.command_inout("Publish", argument);
}

/// Send raw coordination bytes to a named command, the way a hostile or
/// mismatched client would.
std::vector<std::byte> raw_command(Tango::DeviceProxy &proxy,
                                   const std::string &command,
                                   const std::vector<std::byte> &request)
{
    std::vector<unsigned char> in(request.size());
    std::memcpy(in.data(), request.data(), request.size());

    Tango::DeviceData argument;
    argument << in;

    Tango::DeviceData reply = proxy.command_inout(command, argument);

    std::vector<unsigned char> out;
    REQUIRE(reply >> out);

    std::vector<std::byte> bytes(out.size());
    std::memcpy(bytes.data(), out.data(), out.size());
    return bytes;
}

} // namespace

TEST_CASE("The three bulk commands are ordinary commands on a stock device", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    Tango::CommandInfoList *commands = proxy.command_list_query();
    REQUIRE(commands != nullptr);

    const auto find = [commands](const std::string &name) -> const Tango::CommandInfo *
    {
        for(const Tango::CommandInfo &info : *commands)
        {
            if(info.cmd_name == name)
            {
                return &info;
            }
        }
        return nullptr;
    };

    for(const std::string name : {"BulkOpen", "BulkRenew", "BulkClose"})
    {
        const Tango::CommandInfo *info = find(name);
        INFO("command " << name);
        REQUIRE(info != nullptr);

        // 7.1: DevVarCharArray in both directions, because the payload is binary
        // and must not be subject to encoding or NUL-termination semantics.
        CHECK(info->in_type == Tango::DEVVAR_CHARARRAY);
        CHECK(info->out_type == Tango::DEVVAR_CHARARRAY);
    }

    // 7.3: BulkOpen allocates pinned memory and BulkClose terminates a stream,
    // so all three are write-level.
    CHECK(find("BulkOpen")->disp_level == Tango::EXPERT);
    CHECK(find("BulkRenew")->disp_level == Tango::EXPERT);
    CHECK(find("BulkClose")->disp_level == Tango::EXPERT);

    delete commands;
}

TEST_CASE("BulkStreams reports the fixed conservative offer row", "[tango][discovery]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    Tango::DeviceAttribute attribute = proxy.read_attribute("BulkStreams");
    CHECK(attribute.get_type() == Tango::DEV_STRING);
    CHECK(attribute.get_data_format() == Tango::SPECTRUM);
    CHECK_FALSE(attribute.has_failed());
    CHECK_FALSE(attribute.is_empty());
    std::vector<std::string> rows;
    REQUIRE(attribute.extract_read(rows));
    REQUIRE(rows.size() == 1);

    const StreamOffer offer = StreamOffer::from_bulk_stream_row(rows.front());
    CHECK(offer.stream_name == "bulk.tango");
    CHECK(offer.outcome() == StreamOffer::Outcome::Available);
    CHECK(offer.geometry.max_frame_bytes == k_frame_bytes);
    CHECK(offer.geometry.ring_depth == 8);
    CHECK(offer.geometry.credit_window == 4);
}

TEST_CASE("A subscriber opens over DeviceProxy and receives real frames", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    Sink sink;
    auto subscriber = subscribe(proxy, subscriber_config(), sink.callbacks());

    // `Active` means the bounded initial establishment is complete and frames can flow.
    REQUIRE(subscriber->state() == SubscriberState::Active);

    Tango::DeviceData accepted = publish(proxy, 4);
    Tango::DevLong count = 0;
    accepted >> count;
    REQUIRE(count == 4);

    REQUIRE(eventually([&sink] { return sink.frame_count.load() >= 4; }));

    {
        std::lock_guard<std::mutex> lock(sink.mutex);

        // 5.2: user callbacks never run on the UCX engine thread, and in
        // DispatchThread mode they do not run on the caller's either.
        for(const std::thread::id &id : sink.callback_threads)
        {
            CHECK(id != std::this_thread::get_id());
        }

        // The fixture's frame counter is per device, not per session, and this
        // executable shares one device with every other case -- so the first
        // frame's counter is the baseline and what matters is that it advances
        // by one per frame.  Asserting an absolute value here would make the
        // case depend on which order Catch2 ran the others in.
        const std::uint64_t base = sink.frames.front().event_counter();

        for(std::size_t i = 0; i < 4; ++i)
        {
            const FrameView &view = sink.frames[i];
            INFO("frame " << i);

            REQUIRE(view);
            CHECK(view.size() == k_frame_bytes);

            // The sequence *is* per session, and this session is new.
            CHECK(view.sequence() == i);
            CHECK(view.event_counter() == base + i);
            CHECK(view.element_type() == ElementType::UInt8);
            CHECK(view.rank() == 1);
            CHECK(view.shape()[0] == k_frame_bytes);
            CHECK(view.quality() == 7);

            // The payload check is against the pattern for *this frame's* own
            // counter, so a frame delivered from the wrong slot or truncated
            // mid-payload cannot accidentally match.
            const auto seed = static_cast<unsigned>(view.event_counter());
            const auto *bytes = reinterpret_cast<const unsigned char *>(view.data());
            bool matches = true;
            for(std::uint64_t b = 0; b < k_frame_bytes && matches; ++b)
            {
                matches = bytes[b] == pattern_byte(seed, b);
            }
            CHECK(matches);
        }

        sink.frames.clear();
    }

    CHECK(subscriber->counters().frames_delivered >= 4);

    REQUIRE(eventually([&subscriber] { return subscriber->counters().frames_received >= 4; }));

    const SubscriberCounters counters = subscriber->counters();
    CHECK(counters.frames_dropped_bad_header == 0);
    CHECK(counters.frames_dropped_oversize == 0);

    subscriber->close();
    CHECK(subscriber->state() == SubscriberState::Closed);
    subscriber.reset();

    // `Close` releases the session without waiting out its lease.
}

TEST_CASE("The granted geometry reaches the application", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    Sink sink;
    auto subscriber = subscribe(proxy, subscriber_config(), sink.callbacks());

    REQUIRE(subscriber->state() == SubscriberState::Active);

    const Geometry granted = subscriber->geometry();

    CHECK(granted.generation != 0);
    CHECK(granted.element_type == ElementType::UInt8);
    CHECK(granted.element_size == 1);
    CHECK(granted.rank == 1);
    CHECK(granted.shape[0] == k_frame_bytes);
    CHECK(granted.strides[0] == 1);

    CHECK(granted.max_frame_bytes <= subscriber_config().max_frame_bytes);
    CHECK(granted.ring_depth <= subscriber_config().ring_depth);
    CHECK(granted.credit_window <= granted.ring_depth);

    CHECK(granted.validate() == Status::Ok);

    subscriber.reset();
}

TEST_CASE("The renew timer keeps a session past its lease", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    Sink sink;
    auto subscriber = subscribe(proxy, subscriber_config(), sink.callbacks());

    REQUIRE(subscriber->state() == SubscriberState::Active);

    // The fixture's lease is 6.1's 1 000 ms floor, so this is two full TTLs of
    // real time rather than a mocked clock.  A subscriber with no renew timer
    // would be expired and gone well before it elapses.
    std::this_thread::sleep_for(2'200ms);

    CHECK(subscriber->state() == SubscriberState::Active);
    CHECK(subscriber->counters().renewals_sent >= 3);
    CHECK(subscriber->counters().renewals_failed == 0);

    // Asserted about *this* session rather than about the device's counters: the
    // fixture is shared by every case in the executable, so `sessions_expired`
    // is whatever the cases before this one left behind.  A test that reads a
    // global counter to make a local claim passes for the wrong reason exactly
    // when the suite is run in one process.
    CHECK(subscriber->state() != SubscriberState::Reconnecting);

    // A session that is still alive can still carry data, which is the thing the
    // renewal is for.
    Tango::DeviceData accepted = publish(proxy, 1);
    Tango::DevLong count = 0;
    accepted >> count;
    CHECK(count == 1);
    REQUIRE(eventually([&sink] { return sink.frame_count.load() >= 1; }));

    {
        std::lock_guard<std::mutex> lock(sink.mutex);
        sink.frames.clear();
    }

    subscriber.reset();
}

TEST_CASE("Manual delivery runs the callback on the polling thread", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    SubscriberConfig config = subscriber_config();
    config.delivery_mode = DeliveryMode::Manual;

    Sink sink;
    auto subscriber = subscribe(proxy, config, sink.callbacks());

    REQUIRE(subscriber->state() == SubscriberState::Active);

    Tango::DeviceData accepted = publish(proxy, 2);
    Tango::DevLong count = 0;
    accepted >> count;
    REQUIRE(count == 2);

    std::size_t delivered = 0;
    REQUIRE(eventually(
        [&]
        {
            delivered += subscriber->poll(20ms, 8);
            return delivered >= 2;
        }));

    {
        std::lock_guard<std::mutex> lock(sink.mutex);
        for(const std::thread::id &id : sink.callback_threads)
        {
            CHECK(id == std::this_thread::get_id());
        }
        sink.frames.clear();
    }

    subscriber.reset();
}

TEST_CASE("A device with no publisher answers, and does not throw", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    proxy.command_inout("Detach");

    // A device server that has not finished init_device() is a normal transient
    // state. The command returns an encoded Error rather than throwing.
    const std::vector<std::byte> open(16, std::byte{0});
    const std::vector<std::byte> detached =
        raw_command(proxy, "BulkOpen", open);
    Protocol::ErrorMessage detached_error;
    REQUIRE(Protocol::decode(detached.data(), detached.size(), detached_error) == Status::Ok);
    CHECK(detached_error.status == Status::UnknownStream);

    SubscriberConfig config = subscriber_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    Sink sink;

    CHECK_THROWS_AS(subscribe(proxy, config, sink.callbacks()), BulkException);

    proxy.command_inout("Attach");

    // The device recovers without a restart.
    auto recovered = subscribe(proxy, config, sink.callbacks());
    REQUIRE(recovered->state() == SubscriberState::Active);
}

TEST_CASE("A command answers only for the message it is the door for", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    // A Close body, sent to BulkOpen.  Both are DevVarCharArray commands, so
    // nothing below the protocol can tell them apart -- which is exactly why the
    // command checks.
    Protocol::CloseRequest close;
    const std::vector<std::byte> reply =
        raw_command(proxy, "BulkOpen", Protocol::encode(close, 77));

    Protocol::Envelope envelope;
    REQUIRE(Protocol::decode_envelope(reply.data(), reply.size(), envelope) == Status::Ok);
    REQUIRE(envelope.msg_type == Protocol::CoordType::Error);
    CHECK(envelope.correlation_id == 77);

    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(reply.data(), reply.size(), error) == Status::Ok);
    CHECK(error.status == Status::MalformedMessage);

    // Garbage, to the same command.  Neither is a DevFailed.
    const std::vector<std::byte> garbage(16, std::byte{0xAB});
    const std::vector<std::byte> answer = raw_command(proxy, "BulkOpen", garbage);
    REQUIRE(Protocol::decode_envelope(answer.data(), answer.size(), envelope) == Status::Ok);
    CHECK(envelope.msg_type == Protocol::CoordType::Error);
}

TEST_CASE("A subscriber reopens its session after the stream comes back", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    SubscriberConfig config = subscriber_config();
    config.reconnect_policy = ReconnectPolicy::BoundedRetry;
    config.reconnect_backoff_ms = 200;
    config.reconnect_max_attempts = 20;

    Sink sink;
    auto subscriber = subscribe(proxy, config, sink.callbacks());

    REQUIRE(subscriber->state() == SubscriberState::Active);

    // Detaching the publisher is the cheapest honest way to break a live
    // session: the device stays up and its commands stay callable, but they
    // answer Error{UnknownStream}, which is what a client sees while a device
    // server is between init_device() calls.  7.4 makes that a recovery hint --
    // the client must not treat it as authority to release anything.
    proxy.command_inout("Detach");

    REQUIRE(eventually([&subscriber] {
        return subscriber->state() == SubscriberState::Reconnecting;
    }));
    CHECK(subscriber->state() != SubscriberState::Closed);

    proxy.command_inout("Attach");

    // 4.1's BoundedRetry: back off, try again, and stop being broken when the
    // world stops being broken.  Nothing had to restart.
    REQUIRE(eventually([&subscriber] { return subscriber->state() == SubscriberState::Active; }));
    CHECK(subscriber->counters().reconnects >= 1);

    // And the reopened session is a working one, not merely a connected one.
    Tango::DeviceData accepted = publish(proxy, 1);
    Tango::DevLong count = 0;
    accepted >> count;
    REQUIRE(count == 1);

    const std::uint64_t before = sink.frame_count.load();
    REQUIRE(eventually([&sink, before] { return sink.frame_count.load() > before; }));

    {
        std::lock_guard<std::mutex> lock(sink.mutex);
        sink.frames.clear();
    }

    subscriber.reset();
}

TEST_CASE("The command names given at subscribe are the ones used", "[tango][m4]")
{
    Tango::DeviceProxy proxy(DeviceServer::instance().device());

    SubscriberConfig config = subscriber_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    Sink wrong;
    CHECK_THROWS_AS(
        subscribe(proxy, config, wrong.callbacks(), CommandNames::with_prefix("Xyz")),
        BulkException);

    Sink right;
    auto subscriber = subscribe(proxy, config, right.callbacks(), CommandNames{});
    REQUIRE(subscriber->state() == SubscriberState::Active);
    subscriber.reset();
}
