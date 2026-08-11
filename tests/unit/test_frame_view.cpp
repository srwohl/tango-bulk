// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

using namespace TangoBulk;

namespace
{

struct Frame
{
    std::shared_ptr<std::vector<std::uint16_t>> pixels;
    FrameView::Fields fields;
};

Frame make_frame(std::uint64_t height, std::uint64_t width)
{
    Frame frame;
    frame.pixels = std::make_shared<std::vector<std::uint16_t>>(
        static_cast<std::size_t>(height * width));

    for(std::size_t i = 0; i < frame.pixels->size(); ++i)
    {
        (*frame.pixels)[i] = static_cast<std::uint16_t>(i);
    }

    frame.fields.rank = 2;
    frame.fields.shape = {height, width, 0, 0};
    frame.fields.strides = {width * 2, 2, 0, 0};
    frame.fields.element_type = ElementType::UInt16;
    frame.fields.element_size = 2;
    frame.fields.payload_bytes = height * width * 2;
    frame.fields.sequence = 42;
    frame.fields.timestamp_ns = 1'700'000'000'000'000'000ull;
    frame.fields.dropped_before = 7;
    frame.fields.generation = 3;

    return frame;
}

FrameView detach(const Frame &frame)
{
    return FrameView::detached(
        frame.pixels,
        reinterpret_cast<const std::byte *>(frame.pixels->data()),
        frame.fields);
}

} // namespace

TEST_CASE("a detached view reports the fields it was given", "[frame]")
{
    const Frame frame = make_frame(4, 8);
    const FrameView view = detach(frame);

    REQUIRE(static_cast<bool>(view));

    CHECK(view.size() == 4 * 8 * 2);
    CHECK(view.rank() == 2);
    CHECK(view.shape()[0] == 4);
    CHECK(view.shape()[1] == 8);
    CHECK(view.strides()[0] == 16);
    CHECK(view.strides()[1] == 2);
    CHECK(view.element_type() == ElementType::UInt16);
    CHECK(view.element_size() == 2);
    CHECK(view.sequence() == 42);
    CHECK(view.dropped_before() == 7);
    CHECK(view.generation() == 3);
    CHECK(view.memory_kind() == MemoryKind::Host);
    CHECK(view.endian() == Endian::Little);
    CHECK(view.end() == view.begin() + view.size());
}

TEST_CASE("a detached view does not alias the caller's Fields", "[frame]")
{
    Frame frame = make_frame(2, 2);
    const FrameView view = detach(frame);

    // The whole reason detached() copies rather than borrows: a caller that
    // reuses one Fields for every synthetic frame -- which is the obvious way to
    // write a loop -- must not retroactively change the views it already made.
    frame.fields.sequence = 999;
    frame.fields.generation = 100;

    CHECK(view.sequence() == 42);
    CHECK(view.generation() == 3);
}

TEST_CASE("a detached view behaves like a delivered one under copying",
          "[frame]")
{
    const Frame frame = make_frame(2, 2);

    FrameView first = detach(frame);
    REQUIRE(first.use_count() == 1);

    {
        const FrameView second = first;
        CHECK(first.use_count() == 2);
        CHECK(second.use_count() == 2);
        CHECK(second.data() == first.data());
    }

    CHECK(first.use_count() == 1);

    // reset() is how an application returns a credit early, so it has to be the
    // same operation on a synthetic frame as on a real one.
    first.reset();

    CHECK_FALSE(static_cast<bool>(first));
    CHECK(first.size() == 0);
    CHECK(first.data() == nullptr);
    CHECK(first.rank() == 0);
    CHECK(first.element_type() == ElementType::Unknown);
}

TEST_CASE("a detached view keeps its owner alive", "[frame]")
{
    std::weak_ptr<std::vector<std::uint16_t>> observer;
    FrameView view;

    {
        const Frame frame = make_frame(2, 2);
        observer = frame.pixels;
        view = detach(frame);

        // Two references: the caller's and the view's.
        CHECK(observer.use_count() == 2);
    }

    // The Frame is gone and the bytes are not, which is what lets a consumer
    // hold a view past the callback that gave it one.
    REQUIRE_FALSE(observer.expired());
    REQUIRE(static_cast<bool>(view));
    CHECK(reinterpret_cast<const std::uint16_t *>(view.data())[3] == 3);

    view.reset();
    CHECK(observer.expired());
}

TEST_CASE("a detached view accepts a null owner and an unreadable pointer",
          "[frame]")
{
    // A device pointer is the case this supports: detached() must not
    // dereference `data`, so a caller can build a view over GPU memory to
    // exercise a callback without a GPU present.
    FrameView::Fields fields;
    fields.rank = 2;
    fields.shape = {2208, 3216, 0, 0};
    fields.strides = {3216 * 2, 2, 0, 0};
    fields.element_type = ElementType::UInt16;
    fields.element_size = 2;
    fields.payload_bytes = 2208ull * 3216ull * 2ull;
    fields.memory_kind = MemoryKind::Cuda;

    const auto pretend_device_pointer =
        reinterpret_cast<const std::byte *>(std::uintptr_t{0x7f0000000000ull});

    const FrameView view =
        FrameView::detached(nullptr, pretend_device_pointer, fields);

    REQUIRE(static_cast<bool>(view));
    CHECK(view.data() == pretend_device_pointer);
    CHECK(view.size() == 2208ull * 3216ull * 2ull);
    CHECK(view.memory_kind() == MemoryKind::Cuda);
}

TEST_CASE("a default-constructed view is disengaged", "[frame]")
{
    const FrameView view;

    CHECK_FALSE(static_cast<bool>(view));
    CHECK(view.data() == nullptr);
    CHECK(view.size() == 0);
    CHECK(view.use_count() == 0);
    CHECK(view.shape()[0] == 0);
    CHECK(view.strides()[0] == 0);
}
