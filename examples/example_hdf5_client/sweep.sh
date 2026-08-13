#!/bin/sh
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Sweep the parallel HDF5 client. OUTPUT_DIR contains only measured data;
# logs and CSV stay in RESULTS_DIR so pointing OUTPUT_DIR at GPFS is enough.

set -u

CLIENT=${CLIENT:-build/examples/tango-bulk-example-hdf5-client}
DEVICE=${DEVICE:?set DEVICE to the Tango device URI or name}
OUTPUT_DIR=${OUTPUT_DIR:?set OUTPUT_DIR to the filesystem being measured}
FRAMES=${FRAMES:-1024}
WRITERS=${WRITERS:-"1 2 4"}
QUEUE_DEPTHS=${QUEUE_DEPTHS:-"1 2"}
FRAMES_PER_BLOCKS=${FRAMES_PER_BLOCKS:-"4 8"}
RING_DEPTHS=${RING_DEPTHS:-"32 128"}
BLOCKS_PER_STRIPES=${BLOCKS_PER_STRIPES:-"1 4"}
LAYOUTS=${LAYOUTS:-"contiguous"}
REPEATS=${REPEATS:-1}
KEEP_OUTPUTS=${KEEP_OUTPUTS:-0}
DRY_RUN=${DRY_RUN:-0}
SYNC_AFTER_RUN=${SYNC_AFTER_RUN:-1}
COOLDOWN_SECONDS=${COOLDOWN_SECONDS:-0}
STREAM=${STREAM:-image}
TAG=${TAG:-}
PUBLISHER_CREDIT_WINDOW=${PUBLISHER_CREDIT_WINDOW:-unknown}
GPFS_BLOCK_SIZE=${GPFS_BLOCK_SIZE:-unknown}
GPFS_STRIPE_WIDTH=${GPFS_STRIPE_WIDTH:-unknown}
PLACEMENT_NOTE=${PLACEMENT_NOTE:-unknown}

SWEEP_ID=${SWEEP_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}
RESULTS_DIR=${RESULTS_DIR:-hdf5-sweep-results-$SWEEP_ID}
CSV=${CSV:-$RESULTS_DIR/results.csv}

if [ ! -x "$CLIENT" ]; then
    echo "hdf5 sweep: client is not executable: $CLIENT" >&2
    exit 2
fi
if { [ "$KEEP_OUTPUTS" != 0 ] && [ "$KEEP_OUTPUTS" != 1 ]; } ||
   { [ "$DRY_RUN" != 0 ] && [ "$DRY_RUN" != 1 ]; } ||
   { [ "$SYNC_AFTER_RUN" != 0 ] && [ "$SYNC_AFTER_RUN" != 1 ]; }; then
    echo "hdf5 sweep: KEEP_OUTPUTS, DRY_RUN, and SYNC_AFTER_RUN must be 0 or 1" >&2
    exit 2
fi

mkdir -p "$OUTPUT_DIR" "$RESULTS_DIR"
if [ -e "$CSV" ]; then
    echo "hdf5 sweep: refusing to overwrite CSV: $CSV" >&2
    exit 2
fi

csv_row()
{
    destination=$1
    shift
    first=1
    for field in "$@"; do
        escaped=$(printf '%s' "$field" | sed 's/"/""/g')
        if [ "$first" -eq 0 ]; then printf ',' >> "$destination"; fi
        printf '"%s"' "$escaped" >> "$destination"
        first=0
    done
    printf '\n' >> "$destination"
}

csv_row "$CSV" \
    timestamp_utc host kernel filesystem output_dir tag gpfs_block_size gpfs_stripe_width \
    placement_note publisher_credit_window run repeat frames writers queue_depth \
    frames_per_block frame_bytes block_bytes ring_depth blocks_per_stripe layout status exit_code \
    bytes write_seconds gbps hdf5_write_concurrency sustained_samples sustained_gbps_mean \
    sustained_gbps_min sustained_gbps_max wall_seconds sync_seconds peak_queue_max log

host=$(hostname)
kernel=$(uname -sr)
filesystem=$(stat -f -c %T "$OUTPUT_DIR" 2>/dev/null || echo unknown)
run=0
failures=0
active_run_dir=

cleanup_active()
{
    if [ -n "$active_run_dir" ] && [ -d "$active_run_dir" ] && [ "$KEEP_OUTPUTS" -eq 0 ]; then
        find "$active_run_dir" -mindepth 1 -maxdepth 1 -type f -delete
        rmdir "$active_run_dir" 2>/dev/null || true
    fi
}
trap 'cleanup_active; exit 130' HUP INT TERM

repeat=1
while [ "$repeat" -le "$REPEATS" ]; do
    for writers in $WRITERS; do
        for queue_depth in $QUEUE_DEPTHS; do
            for frames_per_block in $FRAMES_PER_BLOCKS; do
                for ring_depth in $RING_DEPTHS; do
                    for blocks_per_stripe in $BLOCKS_PER_STRIPES; do
                        for layout in $LAYOUTS; do
                            run=$((run + 1))
                            run_name=$(printf 'run-%04d' "$run")
                            log=$RESULTS_DIR/$run_name.log
                            timing=$RESULTS_DIR/$run_name.time
                            sync_timing=$RESULTS_DIR/$run_name.sync-time
                            active_run_dir=$OUTPUT_DIR/tango-bulk-hdf5-$SWEEP_ID-$run_name
                            master=$active_run_dir/capture.h5
                            timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
                            status=ok
                            exit_code=0
                            bytes=
                            frame_bytes=
                            block_bytes=
                            write_seconds=
                            gbps=
                            hdf5_write_concurrency=
                            sustained_samples=
                            sustained_gbps_mean=
                            sustained_gbps_min=
                            sustained_gbps_max=
                            wall_seconds=
                            sync_seconds=
                            peak_queue_max=

                            if [ $((FRAMES % frames_per_block)) -ne 0 ]; then
                                status=skipped_frames_not_multiple_of_block
                                exit_code=2
                            elif [ "$writers" -gt $((FRAMES / frames_per_block)) ]; then
                                status=skipped_more_writers_than_blocks
                                exit_code=2
                            elif [ "$frames_per_block" -gt "$ring_depth" ]; then
                                status=skipped_block_larger_than_ring
                                exit_code=2
                            elif [ "$layout" != contiguous ] && [ "$layout" != chunked ]; then
                                status=skipped_unknown_layout
                                exit_code=2
                            elif [ "$DRY_RUN" -eq 1 ]; then
                                status=dry_run
                            else
                                if [ -e "$active_run_dir" ]; then
                                    echo "hdf5 sweep: run directory exists: $active_run_dir" >&2
                                    exit 2
                                fi
                                mkdir "$active_run_dir"
                                layout_arg=
                                if [ "$layout" = contiguous ]; then layout_arg=--contiguous; fi
                                {
                                    echo "# timestamp=$timestamp"
                                    echo "# output_dir=$active_run_dir"
                                    echo "# $CLIENT $DEVICE $master --frames $FRAMES --writers $writers --queue-depth $queue_depth --frames-per-block $frames_per_block --ring-depth $ring_depth --blocks-per-stripe $blocks_per_stripe $layout_arg"
                                } > "$log"

                                # shellcheck disable=SC2086
                                /usr/bin/time -f %e -o "$timing" \
                                    "$CLIENT" "$DEVICE" "$master" --frames "$FRAMES" \
                                    --stream "$STREAM" --writers "$writers" \
                                    --queue-depth "$queue_depth" \
                                    --frames-per-block "$frames_per_block" \
                                    --ring-depth "$ring_depth" \
                                    --blocks-per-stripe "$blocks_per_stripe" \
                                    $layout_arg >> "$log" 2>&1
                                exit_code=$?
                                wall_seconds=$(cat "$timing" 2>/dev/null || true)
                                geometry=$(awk '/^publisher geometry:/{print; exit}' "$log")
                                if [ -n "$geometry" ]; then
                                    frame_bytes=$(printf '%s\n' "$geometry" | awk '
                                        {
                                            for (i = 1; i <= NF; ++i) {
                                                if ($i ~ /^frame_bytes=/) {
                                                    split($i, value, "="); print value[2]; exit
                                                }
                                            }
                                        }
                                    ')
                                    block_bytes=$(printf '%s\n' "$geometry" | awk '
                                        {
                                            for (i = 1; i <= NF; ++i) {
                                                if ($i ~ /^block_bytes=/) {
                                                    split($i, value, "="); print value[2]; exit
                                                }
                                            }
                                        }
                                    ')
                                fi
                                summary=$(awk '/^wrote [0-9]+ frame/{print; exit}' "$log")
                                if [ -n "$summary" ]; then
                                    bytes=$(printf '%s\n' "$summary" | awk '{print $4}')
                                    write_seconds=$(printf '%s\n' "$summary" | awk '{print $7}')
                                    gbps=$(printf '%s\n' "$summary" | awk '{print $9}')
                                    hdf5_write_concurrency=$(printf '%s\n' "$summary" | awk '
                                        {
                                            for (i = 1; i <= NF; ++i) {
                                                if ($i ~ /^hdf5_write_concurrency=/) {
                                                    split($i, value, "="); print value[2]; exit
                                                }
                                            }
                                        }
                                    ')
                                fi
                                peak_queue_max=$(awk '
                                    /peak_queue=/ {
                                        split($0, a, "peak_queue="); split(a[2], b, " ");
                                        if (b[1] + 0 > max) max = b[1] + 0
                                    }
                                    END { if (max != "") print max }
                                ' "$log")
                                sustained=$(awk '
                                    /^progress / {
                                        value = ""
                                        for (i = 1; i <= NF; ++i) {
                                            if ($i ~ /^interval_GBps=/) {
                                                split($i, field, "="); value = field[2] + 0
                                            }
                                        }
                                        if (value != "") {
                                            ++n; sum += value
                                            if (n == 1 || value < min) min = value
                                            if (n == 1 || value > max) max = value
                                        }
                                    }
                                    END {
                                        if (n != 0) printf "%d %.6f %.6f %.6f", n, sum / n, min, max
                                    }
                                ' "$log")
                                if [ -n "$sustained" ]; then
                                    sustained_samples=$(printf '%s\n' "$sustained" | awk '{print $1}')
                                    sustained_gbps_mean=$(printf '%s\n' "$sustained" | awk '{print $2}')
                                    sustained_gbps_min=$(printf '%s\n' "$sustained" | awk '{print $3}')
                                    sustained_gbps_max=$(printf '%s\n' "$sustained" | awk '{print $4}')
                                fi
                                if [ "$exit_code" -ne 0 ] || [ -z "$gbps" ]; then
                                    status=failed
                                    failures=$((failures + 1))
                                fi
                                if [ "$SYNC_AFTER_RUN" -eq 1 ]; then
                                    /usr/bin/time -f %e -o "$sync_timing" sync -f "$active_run_dir"
                                    sync_seconds=$(cat "$sync_timing" 2>/dev/null || true)
                                fi
                                cleanup_active
                            fi

                            csv_row "$CSV" \
                                "$timestamp" "$host" "$kernel" "$filesystem" "$OUTPUT_DIR" \
                                "$TAG" "$GPFS_BLOCK_SIZE" "$GPFS_STRIPE_WIDTH" "$PLACEMENT_NOTE" \
                                "$PUBLISHER_CREDIT_WINDOW" "$run" "$repeat" "$FRAMES" "$writers" \
                                "$queue_depth" "$frames_per_block" "$frame_bytes" "$block_bytes" \
                                "$ring_depth" "$blocks_per_stripe" "$layout" "$status" \
                                "$exit_code" "$bytes" "$write_seconds" "$gbps" \
                                "$hdf5_write_concurrency" "$sustained_samples" \
                                "$sustained_gbps_mean" "$sustained_gbps_min" \
                                "$sustained_gbps_max" "$wall_seconds" "$sync_seconds" \
                                "$peak_queue_max" "$log"
                            printf 'run=%s repeat=%s writers=%s queue=%s block=%s ring=%s stripe=%s layout=%s status=%s gbps=%s hdf5_concurrency=%s sustained_mean=%s sustained_min=%s sustained_max=%s\n' \
                                "$run" "$repeat" "$writers" "$queue_depth" "$frames_per_block" \
                                "$ring_depth" "$blocks_per_stripe" "$layout" "$status" \
                                "${gbps:-n/a}" "${hdf5_write_concurrency:-n/a}" \
                                "${sustained_gbps_mean:-n/a}" "${sustained_gbps_min:-n/a}" \
                                "${sustained_gbps_max:-n/a}"
                            active_run_dir=
                            rm -f "$timing" "$sync_timing"
                            if [ "$COOLDOWN_SECONDS" != 0 ]; then sleep "$COOLDOWN_SECONDS"; fi
                        done
                    done
                done
            done
        done
    done
    repeat=$((repeat + 1))
done

echo "hdf5 sweep: results=$CSV runs=$run failures=$failures"
if [ "$failures" -ne 0 ]; then exit 1; fi
