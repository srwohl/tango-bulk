// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/copy_pool.h>

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <memory>
#include <thread>
#include <vector>

using namespace TangoBulk;

TEST_CASE("the pool issues its own buffers first and the heap after", "[core][copy]")
{
    auto pool = std::make_shared<detail::CopyPool>(3, 1000);
    CHECK(pool->count() == 3);
    CHECK(pool->buffer_bytes() == 1000);
    CHECK(pool->available() == 3);

    std::vector<std::shared_ptr<std::byte>> held;
    for(int i = 0; i < 3; ++i)
    {
        held.push_back(pool->acquire());
        CHECK(pool->owns(held.back().get()));
    }
    CHECK(pool->available() == 0);
    CHECK(pool->exhausted() == 0);

    const std::shared_ptr<std::byte> extra = pool->acquire();
    CHECK_FALSE(pool->owns(extra.get()));
    CHECK(pool->exhausted() == 1);

    // A heap buffer is as writable as a pool one for the buffer size.
    std::memset(extra.get(), 0x5a, pool->buffer_bytes());
    std::memset(held.front().get(), 0x5a, pool->buffer_bytes());
}

TEST_CASE("a buffer released on another thread is reissued", "[core][copy]")
{
    auto pool = std::make_shared<detail::CopyPool>(1, 64);
    std::shared_ptr<std::byte> first = pool->acquire();
    const std::byte *address = first.get();
    CHECK(pool->available() == 0);

    std::thread([buffer = std::move(first)]() mutable { buffer.reset(); }).join();

    CHECK(pool->available() == 1);
    const std::shared_ptr<std::byte> again = pool->acquire();
    CHECK(again.get() == address);
    CHECK(pool->exhausted() == 0);
}

TEST_CASE("an issued buffer keeps the pool alive", "[core][copy]")
{
    std::shared_ptr<std::byte> buffer;
    std::weak_ptr<detail::CopyPool> observer;
    {
        auto pool = std::make_shared<detail::CopyPool>(2, 64);
        observer = pool;
        buffer = pool->acquire();
    }
    CHECK_FALSE(observer.expired());
    std::memset(buffer.get(), 1, 64);

    buffer.reset();
    CHECK(observer.expired());
}

TEST_CASE("an empty pool serves everything from the heap", "[core][copy]")
{
    auto pool = std::make_shared<detail::CopyPool>(0, 128);
    CHECK(pool->available() == 0);
    const std::shared_ptr<std::byte> buffer = pool->acquire();
    CHECK_FALSE(pool->owns(buffer.get()));
    CHECK(pool->exhausted() == 1);
}
