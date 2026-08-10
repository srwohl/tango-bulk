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
#include <string>
#include <thread>
#include <vector>

/// A detector-shaped device server that publishes a bulk stream.
///
/// The integration is three lines, and they are marked below:
///
///     install_bulk_commands(*this)          in DeviceClass::command_factory()
///     attach_publisher(*this, *publisher_)  in DeviceImpl::init_device()
///     detach_publisher(*this)               in DeviceImpl::delete_device()
///
/// Everything else is an ordinary Tango device.  There is no bulk-specific IDL,
/// no DServer command, no event-system hook, and no cppTango patch: this file
/// compiles against an installed cppTango package and the extension's public
/// headers, and nothing else.
///
/// Run it against a Tango database:
///
///     tango-bulk-example-device instance
///
/// or without one:
///
///     tango-bulk-example-device instance -nodb -dlist bulk/example/1
///         -ORBendPoint giop:tcp::10000
namespace
{

using namespace TangoBulk;

constexpr std::uint64_t k_width = 2048;
constexpr std::uint64_t k_height = 2048;
constexpr std::uint64_t k_frame_bytes = k_width * k_height * 2; ///< 8 MiB of u16

class ExampleDetector : public TANGO_BASE_CLASS
{
  public:
    ExampleDetector(Tango::DeviceClass *owner, const std::string &identifier) :
        TANGO_BASE_CLASS(owner, identifier.c_str())
    {
        ExampleDetector::init_device();
    }

    ~ExampleDetector() override
    {
        ExampleDetector::delete_device();
    }

    void init_device() override
    {
        PublisherConfig config;

        // The stream name a client asks for by name in its SubscriberConfig.  A
        // device with several detectors has several publishers, one per stream.
        config.stream_name = "image";

        // 6.1's defaults are 8 MiB x 32 = 256 MiB pinned, which is what this
        // geometry works out to.  Sizing the ring is the one decision a detector
        // integrator genuinely has to make: it is the depth of the buffer
        // between acquisition and a consumer that stops consuming.
        config.max_frame_bytes = k_frame_bytes;
        config.ring_depth = 32;
        config.credit_window = 16;

        // How many clients may hold a session at once.  Above one because 4.4's
        // crash-and-restart case needs the old session and the new one to
        // coexist until the old lease expires.
        config.max_sessions = 4;

        publisher_ = std::make_unique<BulkPublisher>(std::move(config));

        // ---- 7.2, line two of three -------------------------------------
        attach_publisher(*this, *publisher_);
        // -----------------------------------------------------------------

        acquiring_ = true;
        acquisition_ = std::thread([this] { acquire(); });

        set_state(Tango::RUNNING);
        set_status("publishing the 'image' bulk stream");
    }

    void delete_device() override
    {
        acquiring_ = false;
        if(acquisition_.joinable())
        {
            acquisition_.join();
        }

        // ---- 7.2, line three of three -----------------------------------
        //
        // Before the publisher is destroyed, and not merely tidiness:
        // detach_publisher() returns only once no command is still inside
        // handle_coordination(), which is what makes the next line safe.
        detach_publisher(*this);
        // -----------------------------------------------------------------

        publisher_.reset();
    }

  private:
    /// Stand-in for a detector's acquisition loop.
    ///
    /// The shape is what matters, and it is the same shape a real driver has:
    /// take a registered slot, fill it *in place*, hand it over.  There is no
    /// intermediate buffer, no `AttributeValue_5`, and no `push_change_event()`
    /// anywhere on this path -- which is the whole point of the bulk path and
    /// the thing acceptance test A10 counts.
    void acquire()
    {
        using namespace std::chrono_literals;

        std::uint64_t frame = 0;

        while(acquiring_)
        {
            BulkSource::Lease lease = publisher_->source().try_acquire();
            if(!lease)
            {
                // 5.3: acquisition drops, it never waits.  A consumer holding
                // every slot must not be able to stall the detector.
                std::this_thread::sleep_for(1ms);
                continue;
            }

            // The DMA target a real driver would hand to its hardware.
            std::memset(lease.data(), static_cast<int>(frame & 0xFF), k_frame_bytes);

            FrameMetadata meta;
            meta.element_type = ElementType::UInt16;
            meta.rank = 2;
            meta.shape[0] = k_height;
            meta.shape[1] = k_width;
            meta.event_counter = frame;

            // Left at 0 so the library stamps it at publish.  A real detector
            // sets the hardware's acquisition time here instead: the timestamp
            // that matters is when the photons arrived, not when the software
            // got around to sending them.
            meta.timestamp_ns = 0;

            const PublishResult result = publisher_->publish(std::move(lease), meta);

            // Nothing here throws and nothing blocks.  A drop is a counter, and
            // an operator reads it through BulkQuery.
            if(result == PublishResult::Accepted)
            {
                ++frame;
            }

            std::this_thread::sleep_for(10ms); // ~100 Hz
        }
    }

    std::unique_ptr<BulkPublisher> publisher_;
    std::thread acquisition_;
    std::atomic<bool> acquiring_{false};
};

class ExampleDetectorClass : public Tango::DeviceClass
{
  public:
    explicit ExampleDetectorClass(const std::string &class_name) :
        Tango::DeviceClass(class_name)
    {
    }

    void command_factory() override
    {
        // ---- 7.2, line one of three -------------------------------------
        //
        // Adds BulkOpen, BulkRenew, BulkClose and BulkQuery as ordinary
        // commands.  If this device class already had a command by one of those
        // names, this throws rather than shadowing it, and
        // CommandNames::with_prefix("Xyz") is the way out.
        install_bulk_commands(*this);
        // -----------------------------------------------------------------
    }

    void device_factory(const Tango::DevVarStringArray *devices) override
    {
        for(CORBA::ULong i = 0; i < devices->length(); ++i)
        {
            const std::string identifier((*devices)[i]);
            device_list.push_back(new ExampleDetector(this, identifier));
        }

        for(auto *device : device_list)
        {
            export_device(device, device->get_name().c_str());
        }
    }
};

class ExampleDServer : public Tango::DServer
{
  public:
    using Tango::DServer::DServer;

  private:
    void class_factory() override
    {
        add_class(new ExampleDetectorClass("ExampleDetector"));
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
                return new ExampleDServer(
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
        std::cerr << "example_device: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
