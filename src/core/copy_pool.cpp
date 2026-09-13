// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/copy_pool.h>

#include <cstdlib>
#include <new>

namespace TangoBulk::detail
{
namespace
{

constexpr std::size_t k_page = 4096;

/// Page-aligned, and one page more when the aligned size is a power of two of
/// 256 KiB or above: a copy whose destinations sit exactly a large power of
/// two apart lands in the same cache sets every time, and the receive side
/// measured a threefold copy-bandwidth loss from that alone.
std::size_t buffer_stride(std::size_t bytes) noexcept
{
    std::size_t stride = (bytes + k_page - 1) / k_page * k_page;
    if(stride == 0)
    {
        stride = k_page;
    }
    if(stride >= (256u << 10) && (stride & (stride - 1)) == 0)
    {
        stride += k_page;
    }
    return stride;
}

} // namespace

CopyPool::CopyPool(std::size_t count, std::size_t buffer_bytes) :
    count_(count),
    buffer_bytes_(buffer_bytes),
    stride_(buffer_stride(buffer_bytes)),
    free_(count > 0 ? count : 1)
{
    if(count_ > 0)
    {
        storage_ = static_cast<std::byte *>(std::aligned_alloc(k_page, stride_ * count_));
        if(storage_ == nullptr)
        {
            throw std::bad_alloc();
        }
        for(std::size_t i = 0; i < count_; ++i)
        {
            (void)free_.try_push(storage_ + i * stride_);
        }
    }
}

CopyPool::~CopyPool()
{
    // Every issued buffer holds a reference to this pool, so nothing is out
    // when this runs.
    std::free(storage_);
}

bool CopyPool::owns(const void *p) const noexcept
{
    const auto *bytes = static_cast<const std::byte *>(p);
    return storage_ != nullptr && bytes >= storage_ && bytes < storage_ + stride_ * count_;
}

std::shared_ptr<std::byte> CopyPool::acquire()
{
    std::byte *buffer = nullptr;
    if(!free_.try_pop(buffer))
    {
        buffer = static_cast<std::byte *>(std::aligned_alloc(k_page, stride_));
        if(buffer == nullptr)
        {
            throw std::bad_alloc();
        }
        exhausted_.fetch_add(1, std::memory_order_relaxed);
    }

    try
    {
        return std::shared_ptr<std::byte>(
            buffer, [pool = shared_from_this()](std::byte *p) noexcept { pool->release(p); });
    }
    catch(...)
    {
        release(buffer);
        throw;
    }
}

void CopyPool::release(std::byte *buffer) noexcept
{
    if(owns(buffer))
    {
        // Cannot fail: the free list has room for every pool buffer.
        (void)free_.try_push(std::move(buffer));
        return;
    }
    std::free(buffer);
}

} // namespace TangoBulk::detail
