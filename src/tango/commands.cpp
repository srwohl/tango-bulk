// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Quoted and unqualified on purpose; see tango_support.cpp for why a sibling
// header inside src/tango/ cannot be spelled <tango/...>.
#include "bulk_commands.h"

#include <tango-bulk/protocol.h>

#include <tango/tango.h>

#include <cstring>
#include <string>
#include <vector>

/// `BulkOpen`, `BulkRenew`, `BulkClose` and `BulkQuery` as ordinary Tango
/// commands (7.1).
///
/// Each is `DevVarCharArray -> DevVarCharArray` carrying an encoded coordination
/// message (3.3), and each is a thin wrapper over
/// `BulkPublisher::handle_coordination()`.  There is no bulk-specific IDL, no
/// DServer command, and nothing here that a stock device server does not already
/// know how to expose.
///
/// The rule that shapes this file: **a command MUST NOT throw `DevFailed` for a
/// protocol-level failure.**  Protocol failures come back as an encoded `Error`
/// with a `Status`, so a client handles them in one place.  `DevFailed` is left
/// for what the extension did not model -- an input that is not a
/// `DevVarCharArray` at all, which means the call never reached the protocol.
namespace TangoBulk::detail
{
namespace
{

/// Copy an encoded reply into a freshly allocated `DevVarCharArray`.
///
/// `Command::insert(DevVarCharArray *)` consumes the array, so the CORBA layer
/// frees it with the `Any`.  That is the documented recommendation and it is the
/// only variant that does not copy the buffer a second time.
Tango::DevVarCharArray *to_char_array(const std::vector<std::byte> &bytes)
{
    auto *array = new Tango::DevVarCharArray();
    array->length(static_cast<CORBA::ULong>(bytes.size()));
    if(!bytes.empty())
    {
        std::memcpy(array->get_buffer(), bytes.data(), bytes.size());
    }
    return array;
}

/// One command, parameterised by the message it is the door for.
///
/// The four commands do the same thing, which is why there is one class -- but
/// they are not interchangeable: a `Close` body arriving at `BulkOpen` is a
/// client bug, and saying so beats quietly doing what the body asked for.  The
/// name a client called is then always the name of the thing that happened,
/// which is what makes a device's black box readable after a fault.
class CoordinationCommand : public Tango::Command
{
  public:
    CoordinationCommand(const std::string &command_name,
                        Protocol::CoordType expected,
                        Tango::DispLevel level,
                        const char *description) :
        Tango::Command(command_name,
                       Tango::DEVVAR_CHARARRAY,
                       Tango::DEVVAR_CHARARRAY,
                       description,
                       "Encoded tango-bulk coordination reply",
                       level),
        expected_(expected)
    {
    }

    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &in_any) override
    {
        // The one place a DevFailed is correct: an argument that is not a
        // DevVarCharArray never reached the protocol, so there is no Status to
        // report it with.  `extract` raises it for us.
        const Tango::DevVarCharArray *argument = nullptr;
        extract(in_any, argument);

        const CORBA::ULong length = argument->length();
        const std::byte *data =
            length == 0 ? nullptr : reinterpret_cast<const std::byte *>(argument->get_buffer());

        const std::vector<std::byte> reply = run(device, data, length);
        return insert(to_char_array(reply));
    }

  private:
    /// Everything from here on is `noexcept`-shaped by hand: `execute` may throw
    /// `DevFailed` above, but nothing below it may throw at all.
    std::vector<std::byte> run(Tango::DeviceImpl *device,
                               const std::byte *data,
                               std::size_t size) const noexcept
    {
        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
        {
            return Protocol::encode(
                Protocol::ErrorMessage{Status::MalformedMessage, "undecodable envelope"}, 0);
        }

        if(envelope.msg_type != expected_)
        {
            return Protocol::encode(
                Protocol::ErrorMessage{Status::MalformedMessage,
                                       std::string("this command carries ") +
                                           Protocol::to_string(expected_) + ", not " +
                                           Protocol::to_string(envelope.msg_type)},
                envelope.correlation_id);
        }

        return dispatch_coordination(device, data, size);
    }

    Protocol::CoordType expected_;
};

} // namespace

std::vector<Tango::Command *> make_bulk_commands(const CommandNames &names)
{
    // 7.3: BulkOpen allocates pinned memory and BulkClose terminates a data
    // stream, so both are write-level operations; BulkRenew keeps a stream
    // alive and is the same kind of thing.  EXPERT is how that intent is
    // expressed to a facility whose access control distinguishes read from
    // write.  BulkQuery only reads counters, so it stays at OPERATOR.
    //
    // This is not a security boundary and must not be described as one: the
    // device server's existing Tango policy is the authority, and this is the
    // extension telling it which side of the line each command is on.
    return {
        new CoordinationCommand(names.open,
                                Protocol::CoordType::Open,
                                Tango::EXPERT,
                                "Encoded tango-bulk Open request"),
        new CoordinationCommand(names.renew,
                                Protocol::CoordType::Renew,
                                Tango::EXPERT,
                                "Encoded tango-bulk Renew request"),
        new CoordinationCommand(names.close,
                                Protocol::CoordType::Close,
                                Tango::EXPERT,
                                "Encoded tango-bulk Close request"),
        new CoordinationCommand(names.query,
                                Protocol::CoordType::Query,
                                Tango::OPERATOR,
                                "Encoded tango-bulk Query request"),
    };
}

} // namespace TangoBulk::detail
