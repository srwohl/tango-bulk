// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "oob.h"

#include <ucx/subscriber_engine.h>

#include <core/delivery_queue.h>
#include <core/session_client.h>

#include <tango-bulk/publisher.h>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
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
    std::string fill{"each"};
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
        "  --fill each|once       each: regenerate the payload per frame (default),\n"
        "                         which is honest but puts ~0.3 ms per 8 MiB of\n"
        "                         pattern generation on the critical path. once:\n"
        "                         fill every slot at startup and publish without\n"
        "                         touching it again, which is both closer to a\n"
        "                         detector DMA-ing into a slot and the only way to\n"
        "                         get a transport number with no generation in it.\n"
        "                         Implies no --verify.\n"
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
        else if(arg == "--fill") { options.fill = value(i); }
        else if(arg == "--corrupt-every") { options.corrupt_every = std::stoull(value(i)); }
        else if(arg == "--stream") { options.stream = value(i); }
        else { fail("unknown option " + arg); }
    }

    if(options.role != "publisher" && options.role != "subscriber")
    {
        usage();
        fail("--role must be publisher or subscriber");
    }

    if(options.fill != "each" && options.fill != "once")
    {
        fail("--fill must be each or once");
    }

    // A slot filled once carries the same bytes every time it is reused, so a
    // frame's identity is not in its payload and there is nothing to check
    // against. Refusing beats reporting "0 mismatched" from a vacuous compare.
    if(options.fill == "once" && options.verify)
    {
        fail("--fill once cannot be verified: the payload does not identify the frame");
    }

    return options;
}

/// A pattern keyed on both the frame index and the position within the frame, so
/// a payload delivered from the wrong slot, torn across a slot reuse, or
/// truncated mid-transfer does not accidentally match.
///
/// Written a 64-bit word at a time, and that is not micro-optimisation.  The
/// first version of this was a scalar byte loop, which ran at 5.9 GiB/s and so
/// accounted for more than half of every measurement -- the benchmark was
/// reporting a number about its own pattern generator.  This runs at ~25 GiB/s.
/// It is still on the critical path, which is why `generation_seconds` below is
/// measured and reported rather than hoped to be small.
inline std::uint64_t pattern_word(std::uint64_t frame, std::uint64_t index) noexcept
{
    return (frame * 2'654'435'761ull) ^ (index * 0x9E37'79B9'7F4A'7C15ull);
}

void fill_pattern(void *data, std::uint64_t bytes, std::uint64_t frame) noexcept
{
    auto *words = static_cast<std::uint64_t *>(data);
    const std::uint64_t whole = bytes / sizeof(std::uint64_t);

    for(std::uint64_t j = 0; j < whole; ++j)
    {
        words[j] = pattern_word(frame, j);
    }

    // Sizes are usually a multiple of 8, but a tail that was never written would
    // be a hole the verifier could not see.
    const std::uint64_t tail = bytes % sizeof(std::uint64_t);
    if(tail != 0)
    {
        const std::uint64_t last = pattern_word(frame, whole);
        std::memcpy(static_cast<unsigned char *>(data) + whole * sizeof(std::uint64_t), &last, tail);
    }
}

bool pattern_matches(const void *data, std::uint64_t bytes, std::uint64_t frame) noexcept
{
    const auto *words = static_cast<const std::uint64_t *>(data);
    const std::uint64_t whole = bytes / sizeof(std::uint64_t);

    for(std::uint64_t j = 0; j < whole; ++j)
    {
        if(words[j] != pattern_word(frame, j))
        {
            return false;
        }
    }

    const std::uint64_t tail = bytes % sizeof(std::uint64_t);
    if(tail != 0)
    {
        const std::uint64_t last = pattern_word(frame, whole);
        if(std::memcmp(static_cast<const unsigned char *>(data) + whole * sizeof(std::uint64_t),
                       &last, tail) != 0)
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

    if(options.fill == "once")
    {
        // Every slot gets its bytes before the clock starts, and the publish loop
        // never touches a payload again. This is both the only way to get a rate
        // with no pattern generation in it, and the closer analogue of a detector
        // whose DMA engine put the frame there.
        std::vector<BulkSource::Lease> all;
        for(std::uint32_t i = 0; i < options.ring_depth; ++i)
        {
            BulkSource::Lease lease = publisher.source().try_acquire();
            if(!lease)
            {
                fail("could not acquire every slot to pre-fill it");
            }
            fill_pattern(lease.data(), options.size, i);
            all.push_back(std::move(lease));
        }
        // Destroying them returns every slot to the free list with its bytes
        // intact: 5.4 says an unpublished lease releases its slot, not that it
        // scrubs it.
        all.clear();
    }

    const std::uint64_t total = options.warmup + options.iters;
    std::uint64_t published = 0;
    std::uint64_t stalled = 0;
    std::uint64_t queue_full = 0;
    double generation_seconds = 0.0;
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

        if(options.fill == "each")
        {
            // Timed separately and reported, because this is the benchmark's own
            // cost and not the library's. A harness that hides it reports its own
            // memory bandwidth as a transport result.
            const auto fill_start = Clock::now();
            fill_pattern(lease.data(), options.size, published);
            generation_seconds += std::chrono::duration<double>(Clock::now() - fill_start).count();
        }

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

    // The measured rate includes whatever the loop spent generating payloads, so
    // it is a floor on the harness rather than a statement about the transport.
    // Both are printed: the difference is the correction, and stating it is what
    // stops it being rediscovered later as a mystery.
    if(options.fill == "each" && generation_seconds > 0.0)
    {
        const double share = 100.0 * generation_seconds / seconds;
        std::printf("            payload generation %.3f s of %.3f s (%.0f%%)\n",
                    generation_seconds, seconds, share);
        report_rate("  ex-gen", options.iters, options.size,
                    seconds > generation_seconds ? seconds - generation_seconds : seconds);
    }

    std::printf("            submitted %" PRIu64 "  credited %" PRIu64 "  credit-stalled %" PRIu64
                "  queue-full %" PRIu64 "  transport-errors %" PRIu64 "  fill %s\n",
                counters.frames_submitted,
                counters.frames_credited,
                stalled,
                queue_full,
                counters.transport_errors,
                options.fill.c_str());

    const std::vector<std::byte> close_request = oob.recv();
    oob.send(publisher.handle_coordination(close_request.data(), close_request.size()));

    return counters.frames_credited >= total ? 0 : 1;
}

// -- subscriber --------------------------------------------------------------

/// Take up to `max_frames` within `timeout`, invoking `cb` on this thread.
///
/// The engine used to offer this. It no longer does: the delivery queue belongs
/// to the subscription, and here the benchmark *is* the subscription, so the
/// loop lives with the consumer rather than with the transport.
std::size_t take_frames(detail::DeliveryQueue &delivery,
                        std::chrono::milliseconds timeout,
                        const FrameCallback &cb,
                        std::size_t max_frames = 0)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t dispatched = 0;

    while(max_frames == 0 || dispatched < max_frames)
    {
        FrameView view;
        const bool got =
            dispatched == 0 ? delivery.take(view, deadline) : delivery.try_take(view);

        if(!got)
        {
            break;
        }

        ++dispatched;
        cb(std::move(view));
        view.reset();
    }

    return dispatched;
}

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

    // The benchmark is the subscription here: it owns the delivery queue the
    // engine pushes into, and the session contract the engine is started on.
    // A transport owns neither.
    const auto delivery = std::make_shared<detail::DeliveryQueue>(config.delivery_queue_depth,
                                                                  config.drop_policy);
    detail::SessionClient session;
    detail::SubscriberEngine engine(config, delivery);

    const OobChannel oob = OobChannel::connect_to(options.host, options.port);

    oob.send(session.make_open_request(config, engine.local_address(), 1));
    const std::vector<std::byte> reply = oob.recv();
    if(const Status status = session.adopt_open_reply(reply.data(), reply.size(), config);
       status != Status::Ok)
    {
        fail(std::string("Open was refused: ") + to_string(status));
    }

    // Only now, and only with a grant that survived validation.
    if(const Status status = engine.activate(
           session.stream_id(), session.granted_geometry(), session.server_address());
       status != Status::Ok)
    {
        fail(std::string("the transport could not adopt the grant: ") + to_string(status));
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
        take_frames(*delivery,
                    std::chrono::milliseconds(10),
                    [&](FrameView view)
                    {
                        if(view.size() != options.size)
                        {
                            ++short_frames;
                        }
                        else if(options.verify && options.fill == "each" &&
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

    // `frames_dropped_queue_full` is the queue's, not the engine's: the queue
    // outlives any one session, so it is the thing that knows.
    const std::uint64_t dropped_queue_full = delivery->stats().dropped;

    report_rate("subscriber", received > options.warmup ? received - options.warmup : 0,
                options.size, seconds);
    std::printf("            dropped: queue-full %" PRIu64 "  bad-header %" PRIu64
                "  oversize %" PRIu64 "  duplicate-seq %" PRIu64 "  stale-epoch %" PRIu64 "\n",
                dropped_queue_full,
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

    oob.send(session.make_close_request(2));
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
