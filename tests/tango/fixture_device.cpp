// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

/// A stock cppTango device server that publishes a bulk stream.
///
/// This is the fixture the M4 tests run against, and it is deliberately a
/// *separate process* built from installed cppTango headers plus the extension's
/// own.  A test that linked a fake device object would prove the adapter
/// compiles; only a real device server proves a real `DeviceProxy` can drive it,
/// which is what MVP_PLAN M4 asks for.
///
/// It is also the shortest possible statement of the integration contract (7.2):
/// one `install_bulk_commands()` in the class, one `attach_publisher()` in
/// `init_device()`, one `detach_publisher()` in `delete_device()`.  Everything
/// else here -- `Publish`, `Detach`, `Attach` -- exists so a test can drive the
/// device from outside, and is not part of the contract.
namespace
{

using namespace TangoBulk;

/// Small on purpose: the tests care about lifecycle, not throughput, and a lease
/// at 6.1's 1 000 ms floor is what makes the renewal observable inside a test
/// rather than mocked.
constexpr std::uint64_t k_frame_bytes = 64u << 10;
constexpr std::uint32_t k_ring_depth = 8;
constexpr std::uint32_t k_credit_window = 4;
constexpr std::uint32_t k_lease_ttl_ms = 1'000;
constexpr std::uint32_t k_renew_interval_ms = 333;

/// The byte at offset `i` of frame `seed`.  Shared with the test by repetition
/// rather than by a header, because the two live in different processes and a
/// pattern both sides compute independently is a stronger check than one they
/// share a definition of.
unsigned char pattern_byte(unsigned seed, std::uint64_t i)
{
    return static_cast<unsigned char>((seed * 31u + static_cast<unsigned>(i)) & 0xFFu);
}

class BulkTestDevice : public TANGO_BASE_CLASS
{
  public:
    BulkTestDevice(Tango::DeviceClass *owner, const std::string &identifier) :
        TANGO_BASE_CLASS(owner, identifier.c_str())
    {
        BulkTestDevice::init_device();
    }

    ~BulkTestDevice() override
    {
        BulkTestDevice::delete_device();
    }

    void init_device() override
    {
        PublisherConfig config;
        config.stream_name = "bulk.tango";
        config.max_frame_bytes = k_frame_bytes;
        config.ring_depth = k_ring_depth;
        config.credit_window = k_credit_window;
        config.publish_queue_depth = 8;

        // Declare the array this stream carries, rather than leaving it to be
        // defaulted to an opaque byte stream. Without this the grant says
        // "rank 0, Byte" while every frame says "rank 1, UInt8" -- the two
        // disagreeing, which is precisely what the subscriber now refuses.
        config.frame_metadata.element_type = ElementType::UInt8;
        config.frame_metadata.element_size = 1;
        config.frame_metadata.rank = 1;
        config.frame_metadata.shape[0] = k_frame_bytes;

        config.lease_ttl_ms = k_lease_ttl_ms;
        config.renew_interval_ms = k_renew_interval_ms;

        publisher_ = std::make_unique<BulkPublisher>(std::move(config));

        // 7.2, line two of three.
        attach_publisher(*this, *publisher_);
        attached_ = true;

        set_state(Tango::ON);
        set_status("bulk.tango is open for business");
    }

    void delete_device() override
    {
        // 7.2, line three of three.  Detaching before the publisher is destroyed
        // is not tidiness: detach_publisher() returns only once no command is
        // still inside handle_coordination(), which is what makes the next
        // statement safe.
        if(attached_)
        {
            detach_publisher(*this);
            attached_ = false;
        }
        publisher_.reset();
    }

    /// Publish `count` frames, and report how many were accepted.
    Tango::DevLong publish(Tango::DevLong count)
    {
        Tango::DevLong accepted = 0;

        for(Tango::DevLong i = 0; i < count; ++i)
        {
            BulkSource::Lease lease = publisher_->source().try_acquire();
            if(!lease)
            {
                break;
            }

            const auto seed = static_cast<unsigned>(next_seed_);
            auto *bytes = static_cast<unsigned char *>(lease.data());
            for(std::uint64_t b = 0; b < k_frame_bytes; ++b)
            {
                bytes[b] = pattern_byte(seed, b);
            }

            FrameMetadata meta;
            meta.element_type = ElementType::UInt8;
            meta.rank = 1;
            meta.shape[0] = k_frame_bytes;
            meta.event_counter = next_seed_;
            meta.quality = 7;

            if(publisher_->publish(std::move(lease), meta) == PublishResult::Accepted)
            {
                ++accepted;
                ++next_seed_;
            }
        }

        return accepted;
    }

    /// Put the device back into the state 7.2 describes as normal and transient:
    /// commands installed, no publisher attached.
    void detach()
    {
        if(attached_)
        {
            detach_publisher(*this);
            attached_ = false;
        }
    }

    void attach()
    {
        if(!attached_ && publisher_)
        {
            attach_publisher(*this, *publisher_);
            attached_ = true;
        }
    }

  private:
    std::unique_ptr<BulkPublisher> publisher_;
    std::uint64_t next_seed_{0};
    bool attached_{false};
};

class PublishCommand : public Tango::Command
{
  public:
    PublishCommand() :
        Tango::Command("Publish",
                       Tango::DEV_LONG,
                       Tango::DEV_LONG,
                       "frames to publish",
                       "frames accepted",
                       Tango::OPERATOR)
    {
    }

    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &in_any) override
    {
        Tango::DevLong count = 0;
        extract(in_any, count);
        return insert(static_cast<BulkTestDevice *>(device)->publish(count));
    }
};

class DetachCommand : public Tango::Command
{
  public:
    DetachCommand() :
        Tango::Command("Detach", Tango::DEV_VOID, Tango::DEV_VOID, Tango::EXPERT)
    {
    }

    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &) override
    {
        static_cast<BulkTestDevice *>(device)->detach();
        return insert();
    }
};

class AttachCommand : public Tango::Command
{
  public:
    AttachCommand() :
        Tango::Command("Attach", Tango::DEV_VOID, Tango::DEV_VOID, Tango::EXPERT)
    {
    }

    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &) override
    {
        static_cast<BulkTestDevice *>(device)->attach();
        return insert();
    }
};

class BulkTestClass : public Tango::DeviceClass
{
  public:
    explicit BulkTestClass(const std::string &class_name) :
        Tango::DeviceClass(class_name)
    {
    }

    void command_factory() override
    {
        command_list.push_back(new PublishCommand());
        command_list.push_back(new DetachCommand());
        command_list.push_back(new AttachCommand());

        // 7.2, line one of three.  Appended after the device's own commands
        // precisely so a collision would be found here rather than by whichever
        // one cppTango happened to look up first.
        install_bulk_commands(*this);
    }

    void device_factory(const Tango::DevVarStringArray *devices) override
    {
        for(CORBA::ULong i = 0; i < devices->length(); ++i)
        {
            const std::string identifier((*devices)[i]);
            device_list.push_back(new BulkTestDevice(this, identifier));
        }

        for(auto *device : device_list)
        {
            // The second argument becomes the CORBA object key, and its default
            // is the literal string "Unused".  With a database that does not
            // matter -- clients find the device through its registered IOR --
            // but with `-nodb` the key *is* how a client addresses the device,
            // so leaving it at the default publishes every device in the server
            // under one name and makes all of them unreachable.
            export_device(device, device->get_name().c_str());
        }
    }
};

/// cppTango 10 asks for the class list through a `DServer` subclass registered
/// on `Util`, rather than through the free `Tango::DServer::class_factory()`
/// that older versions took.  Using the current mechanism is the point: this
/// fixture exists to prove the adapter works with a *stock, installed* cppTango.
class BulkTestDServer : public Tango::DServer
{
  public:
    using Tango::DServer::DServer;

  private:
    void class_factory() override
    {
        add_class(new BulkTestClass("BulkTest"));
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
                return new BulkTestDServer(
                    device_class, name.c_str(), description.c_str(), state, status.c_str());
            });

        util->server_init();

        // Readiness goes out on inherited descriptor 3, not on stdout.
        //
        // `Util::init()` replaces `std::cout`'s streambuf with cppTango's own
        // logging one, so anything printed there after this point lands in the
        // device server's log rather than on the pipe the harness is watching.
        // A descriptor the harness opened is immune to that, and it also keeps
        // stdout and stderr free to carry a real failure to the test log.
        static const char ready[] = "READY\n";
        const ssize_t written = ::write(3, ready, sizeof(ready) - 1);
        static_cast<void>(written); // run by hand: no fd 3, and nobody waiting

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
        std::cerr << "fixture_device: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
