<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Examples

The examples cover the whole M4 surface plus a file-backed detector:

| | What it shows |
|---|---|
| `example_device/` | A device server that publishes a bulk stream. The integration is the three lines of §7.2, marked in the source. |
| `example_hdf5_device/` | Replays an image stack from a NeXus/HDF5 dataset, using file metadata for frame geometry, numeric type, byte order, and default rate. |
| `example_client/` | A subscriber driven by a stock `Tango::DeviceProxy`. Construct, two callbacks, `start()`, `stop()`. |
| `example_hdf5_client/` | Copies received frames into bounded blocks, writes round-robin HDF5 shards in parallel, and exposes them through one VDS master file. |
| `example_cuda_client/` | Discovers the stream geometry and receives directly into a CUDA allocation through UCX/GPUDirect RDMA. |
| `example_preview/` | The compatibility path: the same bulk stream plus a decimated, rate-capped Tango image attribute with an ordinary change event (§7.5). |

They are built with `-DTANGO_BULK_BUILD_EXAMPLES=ON`, which `pixi run build` sets. They link
`tango-bulk::tango` and nothing from `src/`, which is the M4 exit criterion stated as a build
property: an example that reached inside the library would fail to compile rather than quietly work
in this tree and nowhere else.

## Building against an installed tango-bulk

The examples above are in this tree, so they get their targets from the build tree. A real
application gets them from the installed package instead:

```sh
pixi run install                 # or: cmake --install build --prefix /your/prefix
```

```cmake
find_package(tango-bulk 0.2 REQUIRED)

add_executable(my-client my_client.cpp)
target_link_libraries(my-client PRIVATE tango-bulk::tango)
```

```sh
cmake -S . -B build -DCMAKE_PREFIX_PATH=/your/prefix
```

The target names are the same either way — `tango-bulk::tango`, `::ucx`, `::core`, and the
`tango-bulk::tango-bulk` umbrella — and the package config finds UCX (through the `FindUCX.cmake`
installed beside it) and cppTango on the consumer's behalf. A device server links
`tango-bulk::tango`; a client that never touches Tango can link `tango-bulk::ucx`.

## Running them without a Tango database

Two terminals, no database, no configuration:

```sh
./build/examples/tango-bulk-example-device demo -nodb \
    -dlist bulk/example/1 -ORBendPoint giop:tcp::10000

./build/examples/tango-bulk-example-client "tango://localhost:10000/bulk/example/1#dbase=no"
```

The client prints a frame rate once a second and its counters on exit. `Ctrl-C` closes the session;
the publisher gets its slots back immediately rather than one lease TTL later.

## Writing a stream to parallel HDF5 shards

The HDF5 client is a zero-copy throughput spike built from ordinary synchronous HDF5 calls. It
queries the device for frame shape and type before subscribing, receives into a caller-owned
registered shared-memory ring, and distributes blocks of retained receive-slot views across
independent shard files. Every shard has one dedicated writer process and a bounded coordinator
queue; no two processes ever write the same HDF5 file. Separate processes avoid serialization by
the thread-safe HDF5 library's process-wide API lock. HDF5 reads directly from the shared
registered slots, and the subscriber credit is returned only after the writer process confirms
that the corresponding synchronous write completed.

Blocks are striped round-robin by default so sequential acquisition keeps all writers active. Once
the shard queues drain, the client creates the requested output file as a VDS master whose
`/images` dataset reconstructs global frame order. Each shard is also a valid standalone HDF5 file.

```sh
./build/examples/tango-bulk-example-hdf5-client \
    "tango://localhost:10000/bulk/example/1#dbase=no" capture.h5 \
    --frames 4096 --writers 4 --frames-per-block 32 \
    --blocks-per-stripe 1 --queue-depth 4 --ring-depth 32 --overwrite
```

For `capture.h5`, the physical files are named `capture.part_000.h5`,
`capture.part_001.h5`, and so on. Existing master or shard outputs are refused unless `--overwrite`
is present. `--frames` must be a multiple of `--frames-per-block`; `--blocks-per-stripe` groups
adjacent blocks onto one writer before moving to the next. `--batch-frames` remains accepted as an
alias for `--frames-per-block`. Add `--contiguous` to compare contiguous shard datasets against the
default block-aligned chunks, and use `--stream NAME` for another stream.

`--frames-per-block` must not exceed the publisher's granted credit window; otherwise a complete
block could never reach a writer. Queue backpressure blocks the manual subscriber dispatch loop,
and the publisher's ring/credit window provides the hard bound on retained image memory. The final
report includes aggregate and per-writer throughput, child process IDs, time spent inside
`H5Dwrite`, and peak queue occupancy. `hdf5_write_concurrency` is the sum of every child's
`H5Dwrite` time divided by aggregate acquisition time: values above one directly show overlapping
HDF5 calls in separate processes.

The example publisher keeps its normal 32-slot/16-credit defaults, but larger benchmark rings can
be requested without recompiling it:

```sh
./build/examples/tango-bulk-example-device gpfs -nodb \
    -dlist bulk/example/gpfs -ORBendPoint giop:tcp::10000 \
    --publisher-ring-depth 128 --publisher-credit-window 64
```

`example_hdf5_client/sweep.sh` expands the writer, queue, block, ring, stripe, layout, and repeat
dimensions into isolated acquisitions and writes one CSV row plus one full client log per run.
Successful data files are removed by default; set `KEEP_OUTPUTS=1` to retain them. `OUTPUT_DIR` is
the filesystem under test, while `RESULTS_DIR` defaults to the current directory, so logging does
not add traffic to the GPFS mount. Frame and block byte sizes are queried from the device and
recorded automatically; they are not sweep inputs.

```sh
DEVICE='tango://localhost:10000/bulk/example/gpfs#dbase=no' \
OUTPUT_DIR=/path/to/gpfs/allocation \
WRITERS='1 2 4 8' QUEUE_DEPTHS='1 2 4' \
FRAMES_PER_BLOCKS='4 8 16' RING_DEPTHS='64 128' \
BLOCKS_PER_STRIPES='1 4 16' REPEATS=3 FRAMES=4096 \
PUBLISHER_CREDIT_WINDOW=64 \
GPFS_BLOCK_SIZE=8MiB GPFS_STRIPE_WIDTH=8 \
PLACEMENT_NOTE='fileset policy spreads shards across NSDs' \
./examples/example_hdf5_client/sweep.sh
```

Use `DRY_RUN=1` to materialize and inspect the matrix without contacting the device. The GPFS block
size, stripe width, placement note, filesystem type, host, kernel, and free-form `TAG` are repeated
in every CSV row so result files remain interpretable after they leave the machine. Set the
device's `frameRate` to `0` and `fillPayload` to false for a storage-bound sweep. The harness records
application write time, total client wall time, and a separate filesystem-sync time; set
`SYNC_AFTER_RUN=0` only when durability is deliberately outside the measurement.

Before the GPFS allocation, obtain the filesystem block size (`mmlsfs <device> -B`, when permitted),
the storage stripe width, and the site's file-placement policy. In particular, confirm whether
separate shard files are automatically spread over NSDs or need a fileset/placement-policy hint.
Record those answers through `GPFS_BLOCK_SIZE`, `GPFS_STRIPE_WIDTH`, and `PLACEMENT_NOTE` rather than
baking site-specific assumptions into the sweep.

The preview server is the same shape:

```sh
./build/examples/tango-bulk-example-preview demo -nodb \
    -dlist bulk/preview/1 -ORBendPoint giop:tcp::10001

./build/examples/tango-bulk-example-client "tango://localhost:10001/bulk/preview/1#dbase=no"
```

Point any Tango client at its `preview` attribute at the same time: it updates at 10 Hz while the
bulk stream runs at 100, which is the ratio §7.5 exists to demonstrate.

## Replaying a NeXus/HDF5 image stack

The HDF5 server follows the NeXus `default` attributes to an `NXdata` group and its `signal`
attribute, so the dataset path is normally unnecessary. It treats a rank-three dataset as
`[frame, height, width]`, derives the wire element type and byte order from HDF5, and loops forever.

```sh
./build/examples/tango-bulk-example-hdf5-device demo -nodb \
    -dlist bulk/hdf5/1 -ORBendPoint giop:tcp::10002 \
    --hdf5-file /path/to/cb1_image0000.hdf5 --prefetch-frames 8 \
    --fanout-mode all-active

./build/examples/tango-bulk-example-client "tango://localhost:10002/bulk/hdf5/1#dbase=no"

# Or receive into GPU 0:
./build/examples/tango-bulk-example-cuda-client \
    "tango://localhost:10002/bulk/hdf5/1#dbase=no" image 0
```

Both example clients call `BulkQuery` before opening the stream and size their receive slots from
the publisher geometry. The CUDA client does this before allocating its GPU receive ring. This is
required for this detector shape: a `2208 x 3216 x uint16` frame is 14,201,856 bytes, larger than
the subscriber API's general-purpose 8 MiB default.

One loader thread reads HDF5 frames directly into registered publisher slots, so the replay path
does not copy frames through an intermediate application cache. Prepared slots are kept in a
bounded queue while the replay thread publishes earlier frames. `--prefetch-frames N` controls the
ready queue and credit window; the publisher ring contains `2 * N` slots. `--frame-rate 0` removes
pacing. Otherwise the server uses `exposure_time + latency_time` when present, falling back to 100
Hz. `--fanout-mode best-effort` lets a slow client miss frames while healthy clients continue;
`--fanout-mode all-active` retries each frame until every currently active client has credit. Use
`--hdf5-dataset PATH` for a non-NeXus file and `--hdf5-help` for all replay options.

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
| `tango-bulk-example-hdf5-device/demo` | `Hdf5ReplayDetector` | `bulk/hdf5/1` |
| `tango-bulk-example-preview/demo` | `PreviewDetector` | `bulk/preview/1` |

Re-running `db-setup` is harmless — `--add-server` replaces the entry.

Configure the HDF5 device in the database before starting it. `Hdf5File` is required; the
remaining properties are optional:

```sh
tango_admin --add-property bulk/hdf5/1 Hdf5File \
    /ufs/bl31/controls/phantom/cb1_image/scan_0071/cb1_image0000.hdf5
tango_admin --add-property bulk/hdf5/1 PrefetchFrames 8
tango_admin --add-property bulk/hdf5/1 FanoutMode all-active

# Optional for a non-NeXus file or to override the acquisition timing:
tango_admin --add-property bulk/hdf5/1 Hdf5Dataset \
    /entry_0000/instrument/Areascan/data
tango_admin --add-property bulk/hdf5/1 FrameRate 100
```

When `Hdf5Dataset` is absent, the server follows the NeXus `default` and `signal` attributes. When
`FrameRate` is absent, it uses `exposure_time + latency_time`, falling back to 100 Hz.
`PrefetchFrames` defaults to 8 and `FanoutMode` defaults to `best-effort`.

Now start the servers by hand. There is no pixi task for them on purpose: a device server is the
part you write and launch yourself, and the argument that matters is `demo`, the instance name that
matches the registration above.

```sh
echo $TANGO_HOST                      # pixi sets it: <hostname>:11000

./build/examples/tango-bulk-example-device demo
./build/examples/tango-bulk-example-hdf5-device demo
./build/examples/tango-bulk-example-preview demo
./build/examples/tango-bulk-example-client bulk/example/1
```

Note what is gone compared with the `-nodb` form: no `-dlist`, no `-ORBendPoint`, and the client
takes a plain device name. The database supplies all three.

If startup says it cannot connect to the Tango database, check that `TANGO_HOST` names the running
database and that it is reachable. This happens before the device reads `Hdf5File` and is unrelated
to HDF5 configuration.

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
