// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "oob.h"

#include <ucx/subscriber_engine.h>

#include <tango-bulk/publisher.h>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

/// Two-process throughput and correctness harness for the bulk path.
///
/// This replaces `experiments/ucx-bulk-spike/` in the cppTango prototype tree.
/// The spike hand-rolled a registered ring, a negotiation protocol and both data
/// paths, because at the time there was no library to measure.  There is now, so
/// this measures *that* -- the same `BulkPublisher` and `SubscriberEngine` the
/// tests exercise and a device server would link, with nothing reimplemented for
/// the benchmark's convenience.  A number produced here is a number about the
/// shipping code.
///
/// What that costs: knobs the spike had and this does not.  `--transport rma`
/// and `--flush-every` (the K of 9.4's P0-10) belong to 3.2's Path B, which this
/// library does not implement; `--no-register` and `--progress-thread` are
/// settled by 6.3 and 5.1 respectively.  They are absent rather than stubbed,
/// because a flag that silently does nothing is worse than a missing one.
///
/// The subscriber side drives `detail::SubscriberEngine` directly, since
/// `BulkSubscriber` needs a `Tango::DeviceProxy` and arrives with M4.  The
/// coordination bytes travel over `oob.h` exactly as the M4 adapter will carry
/// them over a Tango command.
namespace
{

using namespace TangoBulk;
using namespace TangoBulkBench;
using Clock = std::chrono::steady_clock;

struct Options
{
    std::string role;
    std::string host{"127.0.0.1"};
    std::uint16_t port{18515};
    std::uint64_t size{1ull << 20};
    std::uint64_t iters{2000};
    std::uint64_t warmup{100};
    std::uint32_t ring_depth{32};
    std::uint32_t credit_window{16};
    std::string tls;
    bool verify{false};
    std::uint64_t corrupt_every{0};
    std::string stream{"bulk.bench"};
};

void usage()
{
    std::puts(
        "tango-bulk-bench --role publisher|subscriber [options]\n"
        "\n"
        "  --host <addr>          subscriber: where the publisher listens (127.0.0.1)\n"
        "  --port <n>             coordination port (18515)\n"
        "  --size <bytes>         payload per frame (1048576)\n"
        "  --iters <n>            frames to measure (2000)\n"
        "  --warmup <n>           frames before the clock starts (100)\n"
        "  --ring-depth <n>       registered slots per ring (32)\n"
        "  --credit-window <n>    frames outstanding, <= ring-depth (16)\n"
        "  --tls <list>           pin UCX_TLS, e.g. rc_verbs,ud_verbs (unset)\n"
        "  --verify               checksum every payload; costs real bandwidth\n"
        "  --corrupt-every <n>    publisher: damage every nth frame before publish.\n"
        "                         The negative control for --verify: without it a\n"
        "                         checker that always passes looks identical to one\n"
        "                         that works.\n"
        "  --stream <name>        stream name on both sides (bulk.bench)\n"
        "\n"
        "Both roles must agree on --size, --ring-depth, --credit-window and --stream.\n");
}

[[noreturn]] void fail(const std::string &message)
{
    std::fprintf(stderr, "tango-bulk-bench: %s\n", message.c_str());
    std::exit(2);
}

Options parse(int argc, char **argv)
{
    Options options;

    const auto value = [&](int &i) -> std::string
    {
        if(i + 1 >= argc)
        {
            fail(std::string(argv[i]) + " needs a value");
        }
        return argv[++i];
    };

    for(int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if(arg == "--help" || arg == "-h")
        {
            usage();
            std::exit(0);
        }
        else if(arg == "--role") { options.role = value(i); }
        else if(arg == "--host") { options.host = value(i); }
        else if(arg == "--port") { options.port = static_cast<std::uint16_t>(std::stoul(value(i))); }
        else if(arg == "--size") { options.size = std::stoull(value(i)); }
        else if(arg == "--iters") { options.iters = std::stoull(value(i)); }
        else if(arg == "--warmup") { options.warmup = std::stoull(value(i)); }
        else if(arg == "--ring-depth") { options.ring_depth = static_cast<std::uint32_t>(std::stoul(value(i))); }
        else if(arg == "--credit-window") { options.credit_window = static_cast<std::uint32_t>(std::stoul(value(i))); }
        else if(arg == "--tls") { options.tls = value(i); }
        else if(arg == "--verify") { options.verify = true; }
        else if(arg == "--corrupt-every") { options.corrupt_every = std::stoull(value(i)); }
        else if(arg == "--stream") { options.stream = value(i); }
        else { fail("unknown option " + arg); }
    }

    if(options.role != "publisher" && options.role != "subscriber")
    {
        usage();
        fail("--role must be publisher or subscriber");
    }

    return options;
}

/// A pattern that depends on the frame index and the offset, so a payload
/// delivered from the wrong slot, torn across a slot reuse, or truncated
/// mid-transfer does not accidentally match.
inline unsigned char pattern_byte(std::uint64_t frame, std::uint64_t offset) noexcept
{
    return static_cast<unsigned char>((frame * 2'654'435'761ull + offset * 31ull) & 0xFFull);
}

void fill_pattern(void *data, std::uint64_t bytes, std::uint64_t frame) noexcept
{
    auto *p = static_cast<unsigned char *>(data);
    for(std::uint64_t i = 0; i < bytes; ++i)
    {
        p[i] = pattern_byte(frame, i);
    }
}

bool pattern_matches(const void *data, std::uint64_t bytes, std::uint64_t frame) noexcept
{
    const auto *p = static_cast<const unsigned char *>(data);
    for(std::uint64_t i = 0; i < bytes; ++i)
    {
        if(p[i] != pattern_byte(frame, i))
        {
            return false;
        }
    }
    return true;
}

void report_rate(const char *what, std::uint64_t frames, std::uint64_t bytes, double seconds)
{
    const double gib = static_cast<double>(frames) * static_cast<double>(bytes) /
                       (1024.0 * 1024.0 * 1024.0);
    std::printf("%-11s %8" PRIu64 " frames  %8.3f GiB  %7.3f s  %8.3f GiB/s  %10.1f frames/s\n",
                what,
                frames,
                gib,
                seconds,
                seconds > 0 ? gib / seconds : 0.0,
                seconds > 0 ? static_cast<double>(frames) / seconds : 0.0);
}

// -- publisher ---------------------------------------------------------------

int run_publisher(const Options &options)
{
    PublisherConfig config;
    config.stream_name = options.stream;
    config.max_frame_bytes = options.size;
    config.ring_depth = options.ring_depth;
    config.credit_window = options.credit_window;
    config.publish_queue_depth = std::max<std::uint32_t>(256, options.ring_depth * 2);
    config.ucx_tls = options.tls;
    // A benchmark is one long uninterrupted run and never renews, so the lease
    // is set wide enough that 4.2's expiry timer cannot fire mid-measurement.
    // Lifecycle is what tests/ucx/test_session_lifecycle.cpp is for.
    config.lease_ttl_ms = k_max_lease_ttl_ms;
    config.renew_interval_ms = k_max_lease_ttl_ms / 3;

    BulkPublisher publisher(config);

    std::printf("publisher   ring %" PRIu32 " x %" PRIu64 " B  window %" PRIu32
                "  pinned %.1f MiB  waiting on :%u\n",
                config.ring_depth,
                config.max_frame_bytes,
                config.credit_window,
                static_cast<double>(publisher.counters().pinned_bytes) / (1024.0 * 1024.0),
                options.port);

    const OobChannel oob = OobChannel::accept_one(options.port);

    // 7.1's command, minus Tango: encoded bytes in, encoded bytes out.
    const std::vector<std::byte> open_request = oob.recv();
    oob.send(publisher.handle_coordination(open_request.data(), open_request.size()));

    // 4.2 arms on ProbeAck, so the grant precedes eligibility by one round trip.
    const auto armed_by = Clock::now() + std::chrono::seconds(10);
    while(publisher.session_count() == 0 && Clock::now() < armed_by)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(publisher.session_count() == 0)
    {
        fail("session never armed: no ProbeAck within 10 s");
    }

    const std::uint64_t total = options.warmup + options.iters;
    std::uint64_t published = 0;
    std::uint64_t stalled = 0;
    std::uint64_t queue_full = 0;
    Clock::time_point start{};

    while(published < total)
    {
        // Wait for window headroom before touching a slot.
        //
        // CreditStalled is flow control, not a frame the benchmark is willing to
        // lose: a run that counted refusals as progress would report a rate for
        // frames that never left. Filling first and being refused afterwards
        // would throw away a whole memset, so the loop watches the gauge 2.5
        // already publishes and only then acquires.
        while(publisher.counters().credits_outstanding >= config.credit_window)
        {
            std::this_thread::yield();
        }

        BulkSource::Lease lease = publisher.source().try_acquire();
        if(!lease)
        {
            // 5.3: try_acquire never blocks. Every slot is retained by a frame
            // the consumer has not credited yet, so the only thing to do is ask
            // again.
            std::this_thread::yield();
            continue;
        }

        fill_pattern(lease.data(), options.size, published);

        if(options.corrupt_every != 0 && published % options.corrupt_every == 0)
        {
            static_cast<unsigned char *>(lease.data())[options.size / 2] ^= 0xFFu;
        }

        FrameMetadata meta;
        meta.element_type = ElementType::UInt8;
        meta.rank = 1;
        meta.shape[0] = options.size;
        meta.event_counter = published;

        // 5.4: QueueFull leaves the lease with the caller, and every other
        // result consumes it. Retrying in place is the whole point of that rule
        // -- the slot is already filled, and re-acquiring would mean re-filling
        // it for a queue that drains in microseconds.
        PublishResult result = PublishResult::Accepted;
        for(;;)
        {
            result = publisher.publish(std::move(lease), meta);
            if(result != PublishResult::QueueFull)
            {
                break;
            }
            ++queue_full;
            std::this_thread::yield();
        }

        if(result == PublishResult::CreditStalled)
        {
            // The gauge was stale by the time publish() looked. The frame was
            // never assigned a sequence, so the next pass re-sends this index.
            ++stalled;
            continue;
        }
        if(result != PublishResult::Accepted)
        {
            fail(std::string("publish() returned ") + to_string(result));
        }

        ++published;
        if(published == options.warmup)
        {
            start = Clock::now();
        }
    }

    // The clock stops when the last frame is credited, not when publish()
    // accepted it: 5.4 keeps the slot until the consumer releases its view, so
    // stopping at acceptance would measure how fast this loop can talk to a
    // queue.
    const auto credited_by = Clock::now() + std::chrono::seconds(60);
    while(publisher.counters().frames_credited < total && Clock::now() < credited_by)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();

    const PublisherCounters counters = publisher.counters();
    if(counters.frames_credited < total)
    {
        std::printf("            INCOMPLETE: %" PRIu64 " of %" PRIu64 " frames credited\n",
                    counters.frames_credited, total);
    }
    report_rate("publisher", options.iters, options.size, seconds);
    std::printf("            submitted %" PRIu64 "  credited %" PRIu64 "  credit-stalled %" PRIu64
                "  queue-full %" PRIu64 "  transport-errors %" PRIu64 "\n",
                counters.frames_submitted,
                counters.frames_credited,
                stalled,
                queue_full,
                counters.transport_errors);

    const std::vector<std::byte> close_request = oob.recv();
    oob.send(publisher.handle_coordination(close_request.data(), close_request.size()));

    return counters.frames_credited >= total ? 0 : 1;
}

// -- subscriber --------------------------------------------------------------

int run_subscriber(const Options &options)
{
    SubscriberConfig config;
    config.stream_name = options.stream;
    config.max_frame_bytes = options.size;
    config.ring_depth = options.ring_depth;
    config.credit_window = options.credit_window;
    config.delivery_queue_depth = std::max<std::uint32_t>(64, options.ring_depth * 2);
    config.delivery_mode = DeliveryMode::Manual;
    config.ucx_tls = options.tls;

    detail::SubscriberEngine engine(config);

    const OobChannel oob = OobChannel::connect_to(options.host, options.port);

    oob.send(engine.make_open_request(1));
    const std::vector<std::byte> reply = oob.recv();
    if(const Status status = engine.adopt_open_reply(reply.data(), reply.size()); status != Status::Ok)
    {
        fail(std::string("Open was refused: ") + to_string(status));
    }

    const auto active_by = Clock::now() + std::chrono::seconds(10);
    while(engine.state() != SubscriberState::Active && Clock::now() < active_by)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(engine.state() != SubscriberState::Active)
    {
        fail("never reached Active: the probe went unanswered");
    }

    std::printf("subscriber  ring %" PRIu32 " x %" PRIu64 " B  window %" PRIu32
                "  pinned %.1f MiB  verify %s\n",
                config.ring_depth,
                config.max_frame_bytes,
                config.credit_window,
                static_cast<double>(engine.counters().pinned_bytes) / (1024.0 * 1024.0),
                options.verify ? "on" : "off");

    const std::uint64_t total = options.warmup + options.iters;
    std::uint64_t received = 0;
    std::uint64_t mismatched = 0;
    std::uint64_t short_frames = 0;
    Clock::time_point start{};

    const auto deadline = Clock::now() + std::chrono::seconds(120);
    while(received < total && Clock::now() < deadline)
    {
        engine.poll(std::chrono::milliseconds(10),
                    [&](FrameView view)
                    {
                        if(view.size() != options.size)
                        {
                            ++short_frames;
                        }
                        else if(options.verify &&
                                !pattern_matches(view.data(), view.size(), view.event_counter()))
                        {
                            ++mismatched;
                        }

                        ++received;
                        if(received == options.warmup)
                        {
                            start = Clock::now();
                        }
                        // The view is released here, at the end of the callback,
                        // and that release *is* the credit return (5.5).
                    });
    }

    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();

    const SubscriberCounters counters = engine.counters();
    report_rate("subscriber", received > options.warmup ? received - options.warmup : 0,
                options.size, seconds);
    std::printf("            dropped: queue-full %" PRIu64 "  bad-header %" PRIu64
                "  oversize %" PRIu64 "  duplicate-seq %" PRIu64 "  stale-epoch %" PRIu64 "\n",
                counters.frames_dropped_queue_full,
                counters.frames_dropped_bad_header,
                counters.frames_dropped_oversize,
                counters.frames_dropped_duplicate_seq,
                counters.frames_dropped_stale_epoch);

    // Credit messages against credits returned is the direct measurement of
    // 3.12 coalescing, and the reason both are counters rather than log lines.
    std::printf("            credits %" PRIu64 " in %" PRIu64 " messages (%.1fx coalescing)"
                "  staged-copy bytes %" PRIu64 "\n",
                counters.credits_returned,
                counters.credit_messages_sent,
                counters.credit_messages_sent > 0
                    ? static_cast<double>(counters.credits_returned) /
                          static_cast<double>(counters.credit_messages_sent)
                    : 0.0,
                engine.bytes_copied());

    int status = 0;

    if(received < total)
    {
        std::printf("            INCOMPLETE: %" PRIu64 " of %" PRIu64 " frames arrived\n",
                    received, total);
        status = 1;
    }

    if(options.verify)
    {
        // The zero-copy claim, asserted rather than assumed: bytes_copied()
        // counts payload that went through an eager staging copy, so for a
        // rendezvous-sized frame it must stay at zero even when every byte was
        // read back and checked.
        std::printf("            verified %" PRIu64 " frames: %" PRIu64 " mismatched, %" PRIu64
                    " wrong size\n",
                    received, mismatched, short_frames);
        if(mismatched != 0 || short_frames != 0)
        {
            status = 1;
        }
    }
    else if(short_frames != 0)
    {
        std::printf("            %" PRIu64 " frames had the wrong size\n", short_frames);
        status = 1;
    }

    oob.send(engine.make_close_request(2));
    const std::vector<std::byte> close_reply = oob.recv();
    (void) close_reply;

    return status;
}

} // namespace

int main(int argc, char **argv)
{
    try
    {
        const Options options = parse(argc, argv);
        return options.role == "publisher" ? run_publisher(options) : run_subscriber(options);
    }
    catch(const BulkException &e)
    {
        std::fprintf(stderr,
                     "tango-bulk-bench: %s (%s, from %s)\n",
                     e.error().message.c_str(),
                     to_string(e.error().status),
                     e.error().origin.c_str());
        return 2;
    }
    catch(const std::exception &e)
    {
        std::fprintf(stderr, "tango-bulk-bench: %s\n", e.what());
        return 2;
    }
}
