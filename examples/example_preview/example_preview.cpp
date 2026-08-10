// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

/// The compatibility path: a bulk stream *and* an ordinary Tango preview.
///
/// 7.5 makes one demand of this example, and it is a negative one: the preview
/// must be a **separate code path** from the bulk publisher.  Concretely --
///
///   * the preview is an ordinary image attribute with an ordinary change event;
///   * its rate is capped (10 Hz here) and independent of the frame rate;
///   * it never carries anything needed to interpret a bulk frame, because a
///     bulk frame is self-contained;
///   * the bulk path constructs no `AttributeValue_5` and calls no
///     `push_change_event()`.
///
/// The last one is what acceptance test A10 counts: during a bulk-only run the
/// number of Tango events emitted is the preview rate, not the frame rate.  In
/// this file that property is visible as a single `if` -- everything Tango-shaped
/// happens inside `if(preview_due())`, and the bulk `publish()` call sits outside
/// it.
///
///     tango-bulk-example-preview instance -nodb -dlist bulk/preview/1
///         -ORBendPoint giop:tcp::10001
namespace
{

using namespace TangoBulk;
using namespace std::chrono_literals;

constexpr std::uint64_t k_width = 2048;
constexpr std::uint64_t k_height = 2048;
constexpr std::uint64_t k_frame_bytes = k_width * k_height * 2;

/// The preview is 64x64 8-bit, which is 4 KiB -- three orders of magnitude below
/// the frame it summarises, and small enough that its cost is a rounding error
/// on the acquisition thread even at 10 Hz.
constexpr long k_preview_width = 64;
constexpr long k_preview_height = 64;
constexpr auto k_preview_period = 100ms; ///< 10 Hz, capped and rate-independent

class PreviewDetector : public TANGO_BASE_CLASS
{
  public:
    PreviewDetector(Tango::DeviceClass *owner, const std::string &identifier) :
        TANGO_BASE_CLASS(owner, identifier.c_str()),
        preview_(static_cast<std::size_t>(k_preview_width * k_preview_height), 0)
    {
        PreviewDetector::init_device();
    }

    ~PreviewDetector() override
    {
        PreviewDetector::delete_device();
    }

    void init_device() override
    {
        PublisherConfig config;
        config.stream_name = "image";
        config.max_frame_bytes = k_frame_bytes;
        config.ring_depth = 16;
        config.credit_window = 8;

        publisher_ = std::make_unique<BulkPublisher>(std::move(config));
        attach_publisher(*this, *publisher_);

        // An ordinary change event on an ordinary attribute.  `detect` is false
        // because the device pushes deliberately; there is no criterion to
        // evaluate and nothing to compare against.
        set_change_event("preview", true, false);

        acquiring_ = true;
        acquisition_ = std::thread([this] { acquire(); });

        set_state(Tango::RUNNING);
        set_status("bulk stream 'image', with a 10 Hz preview attribute");
    }

    void delete_device() override
    {
        acquiring_ = false;
        if(acquisition_.joinable())
        {
            acquisition_.join();
        }

        detach_publisher(*this);
        publisher_.reset();
    }

    /// The attribute read, for a client that polls rather than subscribes.
    void read_preview(Tango::Attribute &attribute)
    {
        const std::lock_guard<std::mutex> lock(preview_mutex_);
        attribute.set_value(preview_.data(), k_preview_width, k_preview_height);
    }

  private:
    bool preview_due()
    {
        const auto now = std::chrono::steady_clock::now();
        if(now - last_preview_ < k_preview_period)
        {
            return false;
        }
        last_preview_ = now;
        return true;
    }

    /// Reduce one frame to the preview buffer by point sampling.
    ///
    /// An explicit, small copy -- and the only copy in this file.  It is here
    /// because the preview is a *different product* from the frame, not a view
    /// of it: it is lossy, it is 8-bit, and nothing downstream may reconstruct a
    /// frame from it.
    void downsample(const void *frame)
    {
        const auto *pixels = static_cast<const std::uint16_t *>(frame);

        const std::lock_guard<std::mutex> lock(preview_mutex_);
        for(long y = 0; y < k_preview_height; ++y)
        {
            for(long x = 0; x < k_preview_width; ++x)
            {
                const auto source_y = static_cast<std::uint64_t>(y) * k_height /
                                      static_cast<std::uint64_t>(k_preview_height);
                const auto source_x = static_cast<std::uint64_t>(x) * k_width /
                                      static_cast<std::uint64_t>(k_preview_width);

                const std::uint16_t sample = pixels[source_y * k_width + source_x];
                preview_[static_cast<std::size_t>(y * k_preview_width + x)] =
                    static_cast<Tango::DevUChar>(sample >> 8);
            }
        }
    }

    void acquire()
    {
        std::uint64_t frame = 0;

        while(acquiring_)
        {
            BulkSource::Lease lease = publisher_->source().try_acquire();
            if(!lease)
            {
                std::this_thread::sleep_for(1ms);
                continue;
            }

            std::memset(lease.data(), static_cast<int>(frame & 0xFF), k_frame_bytes);

            // ---- the preview path, and its whole extent ------------------
            //
            // Decimated, rate-capped, and reading the slot *before* the lease is
            // published -- after `publish()` the slot belongs to the publisher
            // and the NIC may be reading it.
            if(preview_due())
            {
                downsample(lease.data());

                const std::lock_guard<std::mutex> lock(preview_mutex_);
                push_change_event("preview", preview_.data(), k_preview_width, k_preview_height);
            }
            // -------------------------------------------------------------

            FrameMetadata meta;
            meta.element_type = ElementType::UInt16;
            meta.rank = 2;
            meta.shape[0] = k_height;
            meta.shape[1] = k_width;
            meta.event_counter = frame;

            // ---- the bulk path ------------------------------------------
            //
            // No AttributeValue_5, no push_change_event, no CDR marshalling of
            // anything payload-sized.  At 100 Hz this line runs ten times for
            // every event the block above emits, and that ratio is the whole
            // claim the preview example exists to demonstrate.
            if(publisher_->publish(std::move(lease), meta) == PublishResult::Accepted)
            {
                ++frame;
            }
            // -------------------------------------------------------------

            std::this_thread::sleep_for(10ms); // ~100 Hz
        }
    }

    std::unique_ptr<BulkPublisher> publisher_;
    std::thread acquisition_;
    std::atomic<bool> acquiring_{false};

    std::mutex preview_mutex_;
    std::vector<Tango::DevUChar> preview_;
    std::chrono::steady_clock::time_point last_preview_{};
};

class PreviewAttr : public Tango::ImageAttr
{
  public:
    PreviewAttr() :
        Tango::ImageAttr("preview", Tango::DEV_UCHAR, k_preview_width, k_preview_height)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        static_cast<PreviewDetector *>(device)->read_preview(attribute);
    }
};

class PreviewDetectorClass : public Tango::DeviceClass
{
  public:
    explicit PreviewDetectorClass(const std::string &class_name) :
        Tango::DeviceClass(class_name)
    {
    }

    void command_factory() override
    {
        install_bulk_commands(*this);
    }

    void attribute_factory(std::vector<Tango::Attr *> &attributes) override
    {
        attributes.push_back(new PreviewAttr());
    }

    void device_factory(const Tango::DevVarStringArray *devices) override
    {
        for(CORBA::ULong i = 0; i < devices->length(); ++i)
        {
            const std::string identifier((*devices)[i]);
            device_list.push_back(new PreviewDetector(this, identifier));
        }

        for(auto *device : device_list)
        {
            export_device(device, device->get_name().c_str());
        }
    }
};

class PreviewDServer : public Tango::DServer
{
  public:
    using Tango::DServer::DServer;

  private:
    void class_factory() override
    {
        add_class(new PreviewDetectorClass("PreviewDetector"));
    }
};

} // namespace

int main(int argc, char *argv[])
{
    try
    {
        Tango::Util *util = Tango::Util::init(argc, argv);

        util->register_dserver_constructor(
            [](Tango::DeviceClass *device_class,
               const std::string &name,
               const std::string &description,
               Tango::DevState state,
               const std::string &status) -> Tango::DServer *
            {
                return new PreviewDServer(
                    device_class, name.c_str(), description.c_str(), state, status.c_str());
            });

        util->server_init();
        util->server_run();
        util->server_cleanup();
    }
    catch(const Tango::DevFailed &failure)
    {
        Tango::Except::print_exception(failure);
        return 1;
    }
    catch(const std::exception &e)
    {
        std::cerr << "example_preview: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
