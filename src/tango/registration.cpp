// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "bulk_commands.h"

#include <tango-bulk/protocol.h>

#include <tango/tango.h>

#include <algorithm>
#include <cctype>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

/// Device integration, in the three lines 7.2 promises:
///
///     install_bulk_commands(*this)          in DeviceClass::command_factory()
///     attach_publisher(*this, *publisher_)  in DeviceImpl::init_device()
///     detach_publisher(*this)               in DeviceImpl::delete_device()
///
/// The split between the three exists because cppTango builds a device class
/// once and its devices many times: the commands belong to the class and the
/// publisher belongs to the device, so the binding between them cannot be made
/// at either end alone.
namespace TangoBulk
{
namespace
{

/// Which publisher answers for which device.
///
/// A `shared_mutex` rather than a `mutex` for one reason that matters:
/// `detach_publisher()` must not return while a command is still inside
/// the encoded coordination adapter on that publisher, and a reader/writer
/// lock is how
/// that is expressed without also serialising unrelated devices behind one
/// publisher's slowest call -- `Open` waits on endpoint creation, which is
/// bounded in seconds, not microseconds.
///
/// Tango's own serialisation would very nearly cover this: the default
/// SERIAL_BY_DEVICE model does not run `delete_device()` concurrently with a
/// command on the same device.  "Very nearly" is not a memory-safety argument,
/// and the model is configurable by the device server, so the lock is here.
struct Registry
{
    std::shared_mutex mutex;
    std::unordered_map<const Tango::DeviceImpl *, BulkPublisher *> publishers;
};

Registry &registry()
{
    static Registry instance;
    return instance;
}

PublisherSnapshot snapshot_for(Tango::DeviceImpl *device) noexcept
{
    try
    {
        Registry &reg = registry();
        const std::shared_lock<std::shared_mutex> lock(reg.mutex);
        const auto found = device == nullptr ? reg.publishers.end() : reg.publishers.find(device);
        return found == reg.publishers.end() || found->second == nullptr
                   ? PublisherSnapshot{}
                   : found->second->snapshot();
    }
    catch(...)
    {
        // Attribute reads must not take a device server down if a diagnostic
        // copy cannot be made. The empty snapshot is an unavailable reading.
        return PublisherSnapshot{};
    }
}

void set_string_spectrum(Tango::Attribute &attribute,
                         const std::vector<std::string> &values)
{
    auto *rows = new Tango::DevString[values.size()];
    for(std::size_t i = 0; i < values.size(); ++i)
    {
        rows[i] = Tango::string_dup(values[i].c_str());
    }
    attribute.set_value(rows, static_cast<long>(values.size()), 0, true);
}

std::vector<std::string> session_rows(const PublisherSnapshot &snapshot)
{
    std::vector<std::string> rows;
    rows.reserve(snapshot.sessions.size());
    for(const PublisherSnapshot::SessionObservation &session : snapshot.sessions)
    {
        // RFC fixed row order: session_id|state|lag_frames.
        rows.push_back(session.session_id + "|" + session.state + "|" +
                       std::to_string(session.lag_frames));
    }
    return rows;
}

class BulkStreamsAttribute final : public Tango::SpectrumAttr
{
  public:
    BulkStreamsAttribute() :
        Tango::SpectrumAttr("BulkStreams", Tango::DEV_STRING, 8, Tango::OPERATOR)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        const PublisherSnapshot snapshot = snapshot_for(device);
        const StreamOffer offer = snapshot.stream_offer();
        const std::vector<std::string> rows =
            offer.status == Status::Ok ? std::vector<std::string>{offer.to_bulk_stream_row()}
                                       : std::vector<std::string>{};
        set_string_spectrum(attribute, rows);
    }
};

class BulkSessionsAttribute final : public Tango::SpectrumAttr
{
  public:
    BulkSessionsAttribute() :
        Tango::SpectrumAttr("BulkSessions", Tango::DEV_STRING, k_max_sessions, Tango::OPERATOR)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        set_string_spectrum(attribute, session_rows(snapshot_for(device)));
    }
};

enum class ScalarObservation
{
    FramesPublished,
    FramesDropped,
    WorstLagFrames,
};

class ScalarObservationAttribute final : public Tango::Attr
{
  public:
    ScalarObservationAttribute(const char *attribute_name, ScalarObservation observation) :
        Tango::Attr(attribute_name, Tango::DEV_ULONG64, Tango::OPERATOR, Tango::READ),
        observation_(observation)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        const PublisherSnapshot snapshot = snapshot_for(device);
        Tango::DevULong64 value = 0;
        switch(observation_)
        {
        case ScalarObservation::FramesPublished:
            value = snapshot.counters.frames_published;
            break;
        case ScalarObservation::FramesDropped:
            value = snapshot.frames_dropped();
            break;
        case ScalarObservation::WorstLagFrames:
            value = snapshot.worst_lag_frames;
            break;
        }
        attribute.set_value(&value);
    }

  private:
    ScalarObservation observation_;
};

class BulkTransportAttribute final : public Tango::Attr
{
  public:
    BulkTransportAttribute() :
        Tango::Attr("BulkTransport", Tango::DEV_STRING, Tango::OPERATOR, Tango::READ)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        const PublisherSnapshot snapshot = snapshot_for(device);
        // Tango retains scalar string storage until the read is serialized;
        // thread-local backing keeps separate device reads independent.
        thread_local std::string value;
        value = snapshot.transport;
        Tango::DevString raw_value = const_cast<char *>(value.c_str());
        attribute.set_value(&raw_value);
    }
};

std::string lowercase(const std::string &text)
{
    std::string out = text;
    std::transform(out.begin(),
                   out.end(),
                   out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::vector<std::byte> encode_error_noexcept(Status status,
                                             const char *message,
                                             std::uint64_t correlation_id) noexcept
{
    try
    {
        return Protocol::encode(Protocol::ErrorMessage{status, message}, correlation_id);
    }
    catch(...)
    {
        // This is the last-resort path for a noexcept Tango adapter. There is
        // no allocation-free encoded Error representation to return, so an
        // empty byte array is safer than terminating the device server.
        return {};
    }
}

/// Reject a name that would shadow, or be shadowed by, a command the device
/// class already owns.
///
/// Tango resolves command names case-insensitively, so the comparison is too.
/// This check is the entire reason `CommandNames` has an override: a collision
/// discovered here is a clear failure at class-construction time, where the
/// alternative is two commands with one name and a lookup that picks whichever
/// cppTango finds first.
void reject_collisions(Tango::DeviceClass &device_class, const CommandNames &names)
{
    const std::vector<std::string> wanted{names.open, names.renew, names.close};

    for(std::size_t i = 0; i < wanted.size(); ++i)
    {
        if(wanted[i].empty())
        {
            throw BulkException(BulkError{
                Status::MalformedMessage, "a bulk command name is empty", "tango"});
        }

        for(std::size_t j = i + 1; j < wanted.size(); ++j)
        {
            if(lowercase(wanted[i]) == lowercase(wanted[j]))
            {
                throw BulkException(BulkError{Status::MalformedMessage,
                                              "two bulk commands share the name '" + wanted[i] +
                                                  "'",
                                              "tango"});
            }
        }
    }

    for(Tango::Command *existing : device_class.get_command_list())
    {
        if(existing == nullptr)
        {
            continue;
        }

        const std::string have = lowercase(existing->get_name());
        for(const std::string &name : wanted)
        {
            if(have == lowercase(name))
            {
                throw BulkException(
                    BulkError{Status::MalformedMessage,
                              "the device class already has a command named '" + name +
                                  "'; pass CommandNames::with_prefix() to move the bulk commands",
                              "tango"});
            }
        }
    }
}

} // namespace

CommandNames CommandNames::with_prefix(const std::string &prefix)
{
    CommandNames names;
    names.open = prefix + names.open;
    names.renew = prefix + names.renew;
    names.close = prefix + names.close;
    return names;
}

void install_bulk_commands(Tango::DeviceClass &device_class, const CommandNames &names)
{
    reject_collisions(device_class, names);

    // Allocated only once nothing can reject them, so there is no half-installed
    // state to unwind.  Ownership passes to the command list, which cppTango
    // deletes with the class.
    for(Tango::Command *command : detail::make_bulk_commands(names))
    {
        device_class.get_command_list().push_back(command);
    }
}

void install_bulk_attributes(std::vector<Tango::Attr *> &attributes)
{
    attributes.push_back(new BulkStreamsAttribute());
    attributes.push_back(new BulkSessionsAttribute());
    attributes.push_back(new ScalarObservationAttribute(
        "BulkFramesPublished", ScalarObservation::FramesPublished));
    attributes.push_back(new ScalarObservationAttribute(
        "BulkFramesDropped", ScalarObservation::FramesDropped));
    attributes.push_back(new BulkTransportAttribute());
    attributes.push_back(new ScalarObservationAttribute(
        "BulkWorstLagFrames", ScalarObservation::WorstLagFrames));
}

void attach_publisher(Tango::DeviceImpl &device, BulkPublisher &publisher)
{
    Registry &reg = registry();
    const std::unique_lock<std::shared_mutex> lock(reg.mutex);
    reg.publishers[&device] = &publisher;
}

void detach_publisher(Tango::DeviceImpl &device) noexcept
{
    Registry &reg = registry();

    // Exclusive: on return, no command is inside the coordination adapter on
    // this device's publisher, which is what makes it safe for the caller to
    // destroy it in the next statement of delete_device().
    const std::unique_lock<std::shared_mutex> lock(reg.mutex);
    reg.publishers.erase(&device);
}

namespace detail
{

std::vector<std::byte> dispatch_coordination(Tango::DeviceImpl *device,
                                             const std::byte *data,
                                             std::size_t size,
                                             std::optional<Protocol::CoordType> expected) noexcept
{
    Protocol::Envelope envelope;
    const bool decoded = Protocol::decode_envelope(data, size, envelope) == Status::Ok;
    const std::uint64_t correlation_id = decoded ? envelope.correlation_id : 0;

    Registry &reg = registry();

    // Shared, and held across the call: see Registry.  A publisher cannot be
    // detached out from under a command in flight.
    const std::shared_lock<std::shared_mutex> lock(reg.mutex);

    const auto found = device == nullptr ? reg.publishers.end() : reg.publishers.find(device);
    if(found == reg.publishers.end() || found->second == nullptr)
    {
        // 7.2: a device that has not finished init_device() is a normal
        // transient state, not a fault, so this is an encoded Error and not a
        // DevFailed.  UnknownStream is the honest code -- this device serves no
        // stream at all right now.
        return encode_error_noexcept(
            Status::UnknownStream, "no bulk publisher is attached to this device", correlation_id);
    }

    return PublisherAccess::coordination(*found->second, data, size, expected);
}

} // namespace detail

} // namespace TangoBulk
