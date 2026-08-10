<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Examples

Three programs, and between them the whole M4 surface:

| | What it shows |
|---|---|
| `example_device/` | A device server that publishes a bulk stream. The integration is the three lines of §7.2, marked in the source. |
| `example_client/` | A subscriber driven by a stock `Tango::DeviceProxy`. Construct, two callbacks, `start()`, `stop()`. |
| `example_preview/` | The compatibility path: the same bulk stream plus a decimated, rate-capped Tango image attribute with an ordinary change event (§7.5). |

They are built with `-DTANGO_BULK_BUILD_EXAMPLES=ON`, which `pixi run build` sets. They link
`tango-bulk::tango` and nothing from `src/`, which is the M4 exit criterion stated as a build
property: an example that reached inside the library would fail to compile rather than quietly work
in this tree and nowhere else.

## Running them without a Tango database

Two terminals, no database, no configuration:

```sh
./build/examples/tango-bulk-example-device demo -nodb \
    -dlist bulk/example/1 -ORBendPoint giop:tcp::10000

./build/examples/tango-bulk-example-client "localhost:10000/bulk/example/1#dbase=no"
```

The client prints a frame rate once a second and its counters on exit. `Ctrl-C` closes the session;
the publisher gets its slots back immediately rather than one lease TTL later.

The preview server is the same shape:

```sh
./build/examples/tango-bulk-example-preview demo -nodb \
    -dlist bulk/preview/1 -ORBendPoint giop:tcp::10001

./build/examples/tango-bulk-example-client "localhost:10001/bulk/preview/1#dbase=no"
```

Point any Tango client at its `preview` attribute at the same time: it updates at 10 Hz while the
bulk stream runs at 100, which is the ratio §7.5 exists to demonstrate.

## Running them with a database

Register the server and its device the usual way (`tango_admin --add-server`, Jive, or a startup
script), then drop `-nodb -dlist -ORBendPoint`:

```sh
./build/examples/tango-bulk-example-device demo
./build/examples/tango-bulk-example-client bulk/example/1
```

Nothing in the extension depends on which of the two you use. The bulk data plane never goes through
the database, and the coordination plane is three ordinary commands.

## Watching a running publisher

`BulkQuery` is a read-only command any Tango client can call, and `TangoBulk::bulk_query()` is the
typed wrapper:

```cpp
Tango::DeviceProxy proxy("bulk/example/1");
const TangoBulk::BulkQueryResult status = TangoBulk::bulk_query(proxy);
std::cout << status.active_sessions << " session(s): " << status.counters << "\n";
```

The `counters` blob carries no UCX address, no memory key, and no untruncated session identifier —
§3.8 forbids all three, and the UCX tests assert it.
