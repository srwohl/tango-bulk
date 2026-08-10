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

The database is [PyDatabaseds](https://gitlab.com/tango-controls/incubator/pytango-db), a
pure-Python Tango database server backed by a single SQLite file. `pixi install` brings it in, so
there is no MySQL, no system service and nothing to run as root; the whole database is
`demo-tango.db` in the working directory, and deleting that file resets the demo.

Two tasks, run once each. First terminal — leave it running:

```sh
pixi run db
```

Second terminal, once, to register the servers and their devices:

```sh
pixi run db-setup
```

That waits for the database (`tango_admin --ping-database 30`) and then declares what
[`tango_admin`](https://tango-controls.readthedocs.io/en/latest/tools/tango-admin.html) calls a
server: an executable/instance pair, the class it exports, and the devices of that class.

| Server | Class | Device |
|---|---|---|
| `tango-bulk-example-device/demo` | `ExampleDetector` | `bulk/example/1` |
| `tango-bulk-example-preview/demo` | `PreviewDetector` | `bulk/preview/1` |

Re-running `db-setup` is harmless — `--add-server` replaces the entry.

Now start the servers by hand. There is no pixi task for them on purpose: a device server is the
part you write and launch yourself, and the argument that matters is `demo`, the instance name that
matches the registration above.

```sh
echo $TANGO_HOST                      # pixi sets it: <hostname>:11000

./build/examples/tango-bulk-example-device demo
./build/examples/tango-bulk-example-preview demo
./build/examples/tango-bulk-example-client bulk/example/1
```

Note what is gone compared with the `-nodb` form: no `-dlist`, no `-ORBendPoint`, and the client
takes a plain device name. The database supplies all three.

If a server exits with *device not defined in the database*, the instance name and the registration
disagree; `tango_admin --check-device bulk/example/1` says which of the two is wrong.

### About `TANGO_HOST`

Port 11000 rather than the customary 10000, so an existing local Tango installation keeps working.

The host half matters more than it looks. PyDatabaseds takes no endpoint argument — it builds one
from `TANGO_HOST` and hands it to omniORB, so that name is also the interface the database *binds*.
`TANGO_HOST=localhost:11000` binds loopback and nothing else:

```console
$ ss -ltnp | grep :11000
LISTEN 0 128  [::1]:11000  [::]:*  users:(("PyDatabaseds",pid=1167360,fd=13))
```

That is correct for a demo on one machine, and unreachable from a second one. To run the client
elsewhere, put this machine's own name in `TANGO_HOST` — edit it in `pixi.toml`, or override it in
both terminals before starting anything:

```sh
export TANGO_HOST=$(hostname):11000
```

Then check `ss -ltnp | grep :11000` again: the database should no longer be listening on `::1`
alone. Export the same value on the client machine. The bulk data plane makes its own connection and
never goes through the database, so this only affects finding the device.

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
