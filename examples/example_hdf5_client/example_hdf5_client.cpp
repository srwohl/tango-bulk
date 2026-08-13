// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <hdf5.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{

using TangoBulk::Endian;
using TangoBulk::ElementType;
using TangoBulk::FrameView;
using TangoBulk::MemoryKind;

std::atomic<bool> running{true};

void on_signal(int)
{
    running.store(false);
}

struct Options
{
    std::string device;
    std::filesystem::path output;
    std::string stream{"image"};
    std::uint64_t frames{0};
    std::size_t writers{4};
    std::size_t frames_per_block{32};
    std::size_t blocks_per_stripe{1};
    std::size_t queue_depth{4};
    std::size_t ring_depth{32};
    bool overwrite{false};
    bool chunked{true};
};

std::uint64_t parse_positive(const char *text, const char *option)
{
    char *end = nullptr;
    const unsigned long long value = std::strtoull(text, &end, 10);
    if(text[0] == '\0' || end == nullptr || *end != '\0' || value == 0)
        throw std::runtime_error(std::string(option) + " requires a positive integer");
    return static_cast<std::uint64_t>(value);
}

std::size_t parse_size(int &index, int argc, char *argv[], const char *option)
{
    if(index + 1 >= argc)
        throw std::runtime_error(std::string(option) + " requires a value");
    const std::uint64_t value = parse_positive(argv[++index], option);
    if(value > std::numeric_limits<std::size_t>::max())
        throw std::runtime_error(std::string(option) + " is too large");
    return static_cast<std::size_t>(value);
}

Options parse_options(int argc, char *argv[])
{
    if(argc < 3)
        throw std::runtime_error("missing device or output file");

    Options options;
    options.device = argv[1];
    options.output = argv[2];
    for(int i = 3; i < argc; ++i)
    {
        const std::string argument = argv[i];
        if(argument == "--overwrite")
            options.overwrite = true;
        else if(argument == "--contiguous")
            options.chunked = false;
        else if(argument == "--frames")
            options.frames = parse_size(i, argc, argv, "--frames");
        else if(argument == "--writers")
            options.writers = parse_size(i, argc, argv, "--writers");
        else if(argument == "--frames-per-block" || argument == "--batch-frames")
            options.frames_per_block = parse_size(i, argc, argv, argument.c_str());
        else if(argument == "--blocks-per-stripe")
            options.blocks_per_stripe = parse_size(i, argc, argv, "--blocks-per-stripe");
        else if(argument == "--queue-depth")
            options.queue_depth = parse_size(i, argc, argv, "--queue-depth");
        else if(argument == "--ring-depth")
            options.ring_depth = parse_size(i, argc, argv, "--ring-depth");
        else if(argument == "--stream" && i + 1 < argc)
            options.stream = argv[++i];
        else
            throw std::runtime_error("unknown or incomplete option: " + argument);
    }

    if(options.frames == 0)
        throw std::runtime_error("--frames is required");
    if(options.frames % options.frames_per_block != 0)
        throw std::runtime_error("--frames must be a multiple of --frames-per-block");
    const std::uint64_t block_count = options.frames / options.frames_per_block;
    if(options.writers > block_count)
        throw std::runtime_error("--writers cannot exceed the number of blocks");
    if(options.ring_depth > std::numeric_limits<std::uint32_t>::max())
        throw std::runtime_error("--ring-depth is too large");
    return options;
}

void check_hdf5(herr_t status, const char *operation)
{
    if(status < 0)
        throw std::runtime_error(std::string(operation) + " failed");
}

hid_t checked_id(hid_t id, const char *operation)
{
    if(id < 0)
        throw std::runtime_error(std::string(operation) + " failed");
    return id;
}

hid_t hdf5_type(ElementType type, Endian endian)
{
    const bool little = endian == Endian::Little;
    switch(type)
    {
    case ElementType::UInt8:
    case ElementType::Byte:
        return H5T_STD_U8LE;
    case ElementType::Int8:
        return H5T_STD_I8LE;
    case ElementType::UInt16:
        return little ? H5T_STD_U16LE : H5T_STD_U16BE;
    case ElementType::Int16:
        return little ? H5T_STD_I16LE : H5T_STD_I16BE;
    case ElementType::UInt32:
        return little ? H5T_STD_U32LE : H5T_STD_U32BE;
    case ElementType::Int32:
        return little ? H5T_STD_I32LE : H5T_STD_I32BE;
    case ElementType::UInt64:
        return little ? H5T_STD_U64LE : H5T_STD_U64BE;
    case ElementType::Int64:
        return little ? H5T_STD_I64LE : H5T_STD_I64BE;
    case ElementType::Float32:
        return little ? H5T_IEEE_F32LE : H5T_IEEE_F32BE;
    case ElementType::Float64:
        return little ? H5T_IEEE_F64LE : H5T_IEEE_F64BE;
    case ElementType::Unknown:
        break;
    }
    throw std::runtime_error("the stream has an unsupported HDF5 element type");
}

struct Geometry
{
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0};
    std::vector<hsize_t> frame_dimensions;
    std::uint64_t frame_bytes{0};
    std::uint64_t slot_bytes{0};
};

Geometry geometry_from_publisher(const TangoBulk::BulkQueryResult &publisher)
{
    if(publisher.rank == 0 || publisher.rank > TangoBulk::k_max_rank ||
       publisher.element_size == 0 || publisher.element_type == ElementType::Unknown)
        throw std::runtime_error("publisher returned incomplete frame geometry");

    Geometry result;
    result.element_type = publisher.element_type;
    result.element_size = publisher.element_size;
    result.slot_bytes = publisher.max_frame_bytes;
    result.frame_dimensions.reserve(publisher.rank);
    std::uint64_t stride = publisher.element_size;
    for(std::size_t reverse = publisher.rank; reverse > 0; --reverse)
    {
        const std::size_t i = reverse - 1;
        if(publisher.shape[i] == 0 || publisher.strides[i] != stride)
            throw std::runtime_error("the HDF5 client requires non-empty C-contiguous publisher geometry");
        if(stride > std::numeric_limits<std::uint64_t>::max() / publisher.shape[i])
            throw std::runtime_error("publisher frame geometry overflows");
        stride *= publisher.shape[i];
    }
    result.frame_bytes = stride;
    if(result.frame_bytes > publisher.max_frame_bytes)
        throw std::runtime_error("publisher frame shape exceeds max_frame_bytes");
    for(std::size_t i = 0; i < publisher.rank; ++i)
        result.frame_dimensions.push_back(static_cast<hsize_t>(publisher.shape[i]));
    return result;
}

struct Block
{
    std::uint64_t global_first_frame{0};
    std::uint64_t local_first_frame{0};
    std::size_t frame_count{0};
    Endian endian{Endian::Little};
    std::vector<FrameView> frames;
};

std::string shard_suffix(std::size_t index)
{
    std::ostringstream stream;
    stream << ".part_" << std::setfill('0') << std::setw(3) << index << ".h5";
    return stream.str();
}

std::filesystem::path shard_path(const Options &options, std::size_t index)
{
    std::filesystem::path base = options.output;
    if(base.extension() == ".h5" || base.extension() == ".hdf5")
        base.replace_extension();
    return base.string() + shard_suffix(index);
}

void write_scalar_attribute(hid_t object, const char *name, std::uint64_t value)
{
    const hid_t space = checked_id(H5Screate(H5S_SCALAR), "H5Screate(attribute)");
    hid_t attribute = -1;
    try
    {
        attribute = checked_id(H5Acreate2(object, name, H5T_STD_U64LE, space, H5P_DEFAULT,
                                          H5P_DEFAULT),
                               "H5Acreate2");
        check_hdf5(H5Awrite(attribute, H5T_NATIVE_UINT64, &value), "H5Awrite");
        check_hdf5(H5Aclose(attribute), "H5Aclose");
        check_hdf5(H5Sclose(space), "H5Sclose");
    }
    catch(...)
    {
        if(attribute >= 0)
            H5Aclose(attribute);
        H5Sclose(space);
        throw;
    }
}

void write_string_attribute(hid_t object, const char *name, const std::string &value)
{
    const hid_t type = checked_id(H5Tcopy(H5T_C_S1), "H5Tcopy");
    hid_t space = -1;
    hid_t attribute = -1;
    try
    {
        check_hdf5(H5Tset_size(type, value.size() + 1), "H5Tset_size");
        space = checked_id(H5Screate(H5S_SCALAR), "H5Screate(attribute)");
        attribute = checked_id(H5Acreate2(object, name, type, space, H5P_DEFAULT, H5P_DEFAULT),
                               "H5Acreate2");
        check_hdf5(H5Awrite(attribute, type, value.c_str()), "H5Awrite");
        check_hdf5(H5Aclose(attribute), "H5Aclose");
        check_hdf5(H5Sclose(space), "H5Sclose");
        check_hdf5(H5Tclose(type), "H5Tclose");
    }
    catch(...)
    {
        if(attribute >= 0)
            H5Aclose(attribute);
        if(space >= 0)
            H5Sclose(space);
        H5Tclose(type);
        throw;
    }
}

class Hdf5ShardWriter
{
  public:
    Hdf5ShardWriter(std::filesystem::path filename,
                    std::size_t shard_index,
                    std::uint64_t local_frames,
                    const Geometry &geometry,
                    const Options &options) :
        filename_(std::move(filename)), geometry_(geometry)
    {
        file_ = checked_id(H5Fcreate(filename_.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT),
                           "H5Fcreate(shard)");
        std::vector<hsize_t> dimensions{static_cast<hsize_t>(local_frames)};
        dimensions.insert(dimensions.end(), geometry_.frame_dimensions.begin(),
                          geometry_.frame_dimensions.end());
        hid_t space = -1;
        hid_t creation = H5P_DEFAULT;
        try
        {
            space = checked_id(H5Screate_simple(static_cast<int>(dimensions.size()),
                                                dimensions.data(), nullptr),
                               "H5Screate_simple(shard)");
            if(options.chunked)
            {
                creation = checked_id(H5Pcreate(H5P_DATASET_CREATE), "H5Pcreate");
                std::vector<hsize_t> chunk = dimensions;
                chunk.front() = std::min<hsize_t>(options.frames_per_block, local_frames);
                check_hdf5(H5Pset_chunk(creation, static_cast<int>(chunk.size()), chunk.data()),
                           "H5Pset_chunk");
            }
            dataset_ = checked_id(H5Dcreate2(file_, "/images",
                                             hdf5_type(geometry_.element_type, Endian::Little),
                                             space, H5P_DEFAULT, creation, H5P_DEFAULT),
                                  "H5Dcreate2(shard)");
            write_scalar_attribute(file_, "shard_index", shard_index);
            write_scalar_attribute(file_, "local_frame_count", local_frames);
            write_scalar_attribute(file_, "writer_count", options.writers);
            write_scalar_attribute(file_, "frames_per_block", options.frames_per_block);
            write_scalar_attribute(file_, "blocks_per_stripe", options.blocks_per_stripe);
            write_string_attribute(file_, "sharding_mode", "round_robin_blocks");
            if(creation != H5P_DEFAULT)
                check_hdf5(H5Pclose(creation), "H5Pclose");
            check_hdf5(H5Sclose(space), "H5Sclose");
        }
        catch(...)
        {
            if(creation != H5P_DEFAULT)
                H5Pclose(creation);
            if(space >= 0)
                H5Sclose(space);
            close_noexcept();
            throw;
        }
    }

    ~Hdf5ShardWriter() { close_noexcept(); }

    void write(const Block &block)
    {
        // A block can wrap around the receive ring, or contain non-adjacent
        // slots after earlier shard writes return credits out of order.  Write
        // each ascending run as one strided memory selection.  HDF5 reads
        // directly from the registered receive slots; no staging buffer exists.
        std::size_t run_begin = 0;
        while(run_begin < block.frames.size())
        {
            std::size_t run_end = run_begin + 1;
            while(run_end < block.frames.size() &&
                  block.frames[run_end].data() ==
                      block.frames[run_end - 1].data() + geometry_.slot_bytes)
                ++run_end;
            write_run(block, run_begin, run_end);
            run_begin = run_end;
        }
    }

    void close()
    {
        if(dataset_ >= 0)
        {
            check_hdf5(H5Dclose(dataset_), "H5Dclose");
            dataset_ = -1;
        }
        if(file_ >= 0)
        {
            check_hdf5(H5Fclose(file_), "H5Fclose");
            file_ = -1;
        }
    }

  private:
    void write_run(const Block &block, std::size_t begin, std::size_t end)
    {
        const std::size_t run_frames = end - begin;
        std::vector<hsize_t> file_start(geometry_.frame_dimensions.size() + 1, 0);
        std::vector<hsize_t> file_count{static_cast<hsize_t>(run_frames)};
        file_start.front() = static_cast<hsize_t>(block.local_first_frame + begin);
        file_count.insert(file_count.end(), geometry_.frame_dimensions.begin(),
                          geometry_.frame_dimensions.end());

        const hid_t file_space = checked_id(H5Dget_space(dataset_), "H5Dget_space");
        hid_t memory_space = -1;
        try
        {
            check_hdf5(H5Sselect_hyperslab(file_space, H5S_SELECT_SET, file_start.data(), nullptr,
                                           file_count.data(), nullptr),
                       "H5Sselect_hyperslab(file)");
            if(geometry_.slot_bytes == geometry_.frame_bytes)
            {
                // This is the normal image-stream case: adjacent receive slots
                // are also adjacent frame payloads.  Describe them as the
                // dense N-D array HDF5 is about to write.  Expressing the same
                // memory as one enormous 1-D hyperslab makes HDF5 expand the
                // selection element by element during chunk setup; a block of
                // four 14 MB frames can spend minutes in H5S__hyper_iter_next.
                memory_space = checked_id(
                    H5Screate_simple(static_cast<int>(file_count.size()), file_count.data(),
                                     nullptr),
                    "H5Screate_simple(contiguous receive slots)");
            }
            else
            {
                const hsize_t slot_elements = geometry_.slot_bytes / geometry_.element_size;
                const hsize_t frame_elements = geometry_.frame_bytes / geometry_.element_size;
                const hsize_t memory_extent =
                    slot_elements * static_cast<hsize_t>(run_frames - 1) + frame_elements;
                memory_space = checked_id(H5Screate_simple(1, &memory_extent, nullptr),
                                          "H5Screate_simple(receive slots)");
                const hsize_t memory_start = 0;
                const hsize_t memory_count = run_frames;
                check_hdf5(H5Sselect_hyperslab(memory_space, H5S_SELECT_SET, &memory_start,
                                               &slot_elements, &memory_count, &frame_elements),
                           "H5Sselect_hyperslab(receive slots)");
            }
            check_hdf5(H5Dwrite(dataset_, hdf5_type(geometry_.element_type, block.endian),
                                memory_space, file_space, H5P_DEFAULT,
                                block.frames[begin].data()),
                       "H5Dwrite");
            check_hdf5(H5Sclose(memory_space), "H5Sclose");
            check_hdf5(H5Sclose(file_space), "H5Sclose");
        }
        catch(...)
        {
            if(memory_space >= 0)
                H5Sclose(memory_space);
            H5Sclose(file_space);
            throw;
        }
    }

    void close_noexcept() noexcept
    {
        if(dataset_ >= 0)
            H5Dclose(dataset_);
        if(file_ >= 0)
            H5Fclose(file_);
        dataset_ = -1;
        file_ = -1;
    }

    std::filesystem::path filename_;
    Geometry geometry_;
    hid_t file_{-1};
    hid_t dataset_{-1};
};

class ShardWorker
{
  public:
    ShardWorker(std::filesystem::path filename,
                std::size_t index,
                std::uint64_t local_frames,
                const Geometry &geometry,
                const Options &options) :
        filename_(std::move(filename)), index_(index), local_frames_(local_frames),
        geometry_(geometry), options_(options)
    {
        thread_ = std::thread([this] { run(); });
    }

    ~ShardWorker()
    {
        stop_noexcept();
    }

    void submit(Block block)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        space_available_.wait(lock, [this] {
            return queue_.size() < options_.queue_depth || stopping_ || error_;
        });
        rethrow_if_failed();
        if(stopping_)
            throw std::runtime_error("submit to a stopped shard worker");
        queue_.push_back(std::move(block));
        peak_queue_ = std::max(peak_queue_, queue_.size());
        lock.unlock();
        work_available_.notify_one();
    }

    void stop()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        work_available_.notify_all();
        space_available_.notify_all();
        if(thread_.joinable())
            thread_.join();
        std::lock_guard<std::mutex> lock(mutex_);
        rethrow_if_failed();
    }

    std::uint64_t bytes_written() const noexcept { return bytes_written_; }
    std::uint64_t blocks_written() const noexcept { return blocks_written_; }
    std::size_t peak_queue() const noexcept { return peak_queue_; }
    double seconds() const noexcept
    {
        return started_ ? std::chrono::duration<double>(finished_at_ - started_at_).count() : 0.0;
    }

  private:
    void rethrow_if_failed() const
    {
        if(error_)
            std::rethrow_exception(error_);
    }

    void run() noexcept
    {
        try
        {
            Hdf5ShardWriter writer(filename_, index_, local_frames_, geometry_, options_);
            while(true)
            {
                Block block;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    work_available_.wait(lock, [this] { return !queue_.empty() || stopping_; });
                    if(queue_.empty() && stopping_)
                        break;
                    block = std::move(queue_.front());
                    queue_.pop_front();
                }
                space_available_.notify_one();
                if(!started_)
                {
                    started_ = true;
                    started_at_ = std::chrono::steady_clock::now();
                }
                writer.write(block);
                bytes_written_ += geometry_.frame_bytes * block.frame_count;
                ++blocks_written_;
            }
            writer.close();
            finished_at_ = std::chrono::steady_clock::now();
        }
        catch(...)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            error_ = std::current_exception();
            stopping_ = true;
        }
        work_available_.notify_all();
        space_available_.notify_all();
    }

    void stop_noexcept() noexcept
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        work_available_.notify_all();
        space_available_.notify_all();
        if(thread_.joinable())
            thread_.join();
    }

    std::filesystem::path filename_;
    std::size_t index_;
    std::uint64_t local_frames_;
    Geometry geometry_;
    Options options_;
    mutable std::mutex mutex_;
    std::condition_variable work_available_;
    std::condition_variable space_available_;
    std::deque<Block> queue_;
    std::thread thread_;
    bool stopping_{false};
    std::exception_ptr error_;
    std::uint64_t bytes_written_{0};
    std::uint64_t blocks_written_{0};
    std::size_t peak_queue_{0};
    bool started_{false};
    std::chrono::steady_clock::time_point started_at_{};
    std::chrono::steady_clock::time_point finished_at_{};
};

struct BlockLocation
{
    std::size_t writer{0};
    std::uint64_t local_block{0};
};

BlockLocation locate_block(std::uint64_t block, const Options &options)
{
    const std::uint64_t stripe = block / options.blocks_per_stripe;
    const std::size_t writer = static_cast<std::size_t>(stripe % options.writers);
    const std::uint64_t local_stripe = stripe / options.writers;
    const std::uint64_t inside = block % options.blocks_per_stripe;
    return {writer, local_stripe * options.blocks_per_stripe + inside};
}

std::vector<std::uint64_t> local_frame_counts(const Options &options)
{
    std::vector<std::uint64_t> result(options.writers, 0);
    const std::uint64_t blocks = options.frames / options.frames_per_block;
    for(std::uint64_t block = 0; block < blocks; ++block)
    {
        const BlockLocation location = locate_block(block, options);
        result[location.writer] =
            std::max(result[location.writer],
                     (location.local_block + 1) * options.frames_per_block);
    }
    return result;
}

class ParallelShardWriter
{
  public:
    ParallelShardWriter(const Options &options, const Geometry &geometry) :
        options_(options), geometry_(geometry), local_frames_(local_frame_counts(options))
    {
        for(std::size_t i = 0; i < options_.writers; ++i)
            workers_.push_back(std::make_unique<ShardWorker>(shard_path(options_, i), i,
                                                             local_frames_[i], geometry_, options_));
    }

    void submit(std::uint64_t global_block, std::vector<FrameView> frames, Endian endian)
    {
        const BlockLocation location = locate_block(global_block, options_);
        Block block;
        block.global_first_frame = global_block * options_.frames_per_block;
        block.local_first_frame = location.local_block * options_.frames_per_block;
        block.frame_count = options_.frames_per_block;
        block.endian = endian;
        block.frames = std::move(frames);
        if(!started_)
        {
            started_ = true;
            started_at_ = std::chrono::steady_clock::now();
        }
        workers_[location.writer]->submit(std::move(block));
    }

    void close()
    {
        for(auto &worker : workers_)
            worker->stop();
        finished_at_ = std::chrono::steady_clock::now();
    }

    double seconds() const noexcept
    {
        return started_ ? std::chrono::duration<double>(finished_at_ - started_at_).count() : 0.0;
    }

    const std::vector<std::unique_ptr<ShardWorker>> &workers() const noexcept { return workers_; }
    const std::vector<std::uint64_t> &local_frames() const noexcept { return local_frames_; }

  private:
    Options options_;
    Geometry geometry_;
    std::vector<std::uint64_t> local_frames_;
    std::vector<std::unique_ptr<ShardWorker>> workers_;
    bool started_{false};
    std::chrono::steady_clock::time_point started_at_{};
    std::chrono::steady_clock::time_point finished_at_{};
};

void create_master(const Options &options, const Geometry &geometry)
{
    std::vector<hsize_t> dimensions{static_cast<hsize_t>(options.frames)};
    dimensions.insert(dimensions.end(), geometry.frame_dimensions.begin(),
                      geometry.frame_dimensions.end());
    const hid_t virtual_space = checked_id(
        H5Screate_simple(static_cast<int>(dimensions.size()), dimensions.data(), nullptr),
        "H5Screate_simple(VDS)");
    const hid_t creation = checked_id(H5Pcreate(H5P_DATASET_CREATE), "H5Pcreate(VDS)");
    try
    {
        const std::uint64_t blocks = options.frames / options.frames_per_block;
        const std::vector<std::uint64_t> shard_frames = local_frame_counts(options);
        for(std::uint64_t block = 0; block < blocks; ++block)
        {
            const BlockLocation location = locate_block(block, options);
            std::vector<hsize_t> virtual_start(dimensions.size(), 0);
            std::vector<hsize_t> source_dimensions = dimensions;
            virtual_start.front() = block * options.frames_per_block;
            source_dimensions.front() = options.frames_per_block;

            const hid_t selected_virtual = checked_id(H5Scopy(virtual_space), "H5Scopy(VDS)");
            hid_t source_space = -1;
            try
            {
                check_hdf5(H5Sselect_hyperslab(selected_virtual, H5S_SELECT_SET,
                                               virtual_start.data(), nullptr,
                                               source_dimensions.data(), nullptr),
                           "H5Sselect_hyperslab(VDS)");
                std::vector<hsize_t> source_count = source_dimensions;
                source_count.front() = options.frames_per_block;
                const hsize_t local_start = location.local_block * options.frames_per_block;
                std::vector<hsize_t> local_dimensions = dimensions;
                local_dimensions.front() = shard_frames[location.writer];
                source_space = checked_id(
                    H5Screate_simple(static_cast<int>(local_dimensions.size()),
                                     local_dimensions.data(), nullptr),
                    "H5Screate_simple(VDS source)");
                std::vector<hsize_t> local_offset(local_dimensions.size(), 0);
                local_offset.front() = local_start;
                check_hdf5(H5Sselect_hyperslab(source_space, H5S_SELECT_SET, local_offset.data(),
                                               nullptr, source_count.data(), nullptr),
                           "H5Sselect_hyperslab(VDS source)");
                const std::string source = shard_path(options, location.writer).filename().string();
                check_hdf5(H5Pset_virtual(creation, selected_virtual, source.c_str(), "/images",
                                          source_space),
                           "H5Pset_virtual");
                check_hdf5(H5Sclose(source_space), "H5Sclose");
                source_space = -1;
                check_hdf5(H5Sclose(selected_virtual), "H5Sclose");
            }
            catch(...)
            {
                if(source_space >= 0)
                    H5Sclose(source_space);
                H5Sclose(selected_virtual);
                throw;
            }
        }

        const hid_t file = checked_id(H5Fcreate(options.output.c_str(), H5F_ACC_EXCL, H5P_DEFAULT,
                                                H5P_DEFAULT),
                                      "H5Fcreate(master)");
        hid_t dataset = -1;
        try
        {
            dataset = checked_id(H5Dcreate2(file, "/images",
                                            hdf5_type(geometry.element_type, Endian::Little),
                                            virtual_space, H5P_DEFAULT, creation, H5P_DEFAULT),
                                 "H5Dcreate2(VDS)");
            write_scalar_attribute(file, "writer_count", options.writers);
            write_scalar_attribute(file, "frames_per_block", options.frames_per_block);
            write_scalar_attribute(file, "blocks_per_stripe", options.blocks_per_stripe);
            write_string_attribute(file, "sharding_mode", "round_robin_blocks");
            check_hdf5(H5Dclose(dataset), "H5Dclose");
            check_hdf5(H5Fclose(file), "H5Fclose");
        }
        catch(...)
        {
            if(dataset >= 0)
                H5Dclose(dataset);
            H5Fclose(file);
            throw;
        }
        check_hdf5(H5Pclose(creation), "H5Pclose");
        check_hdf5(H5Sclose(virtual_space), "H5Sclose");
    }
    catch(...)
    {
        H5Pclose(creation);
        H5Sclose(virtual_space);
        throw;
    }
}

void prepare_outputs(const Options &options)
{
    std::vector<std::filesystem::path> paths{options.output};
    for(std::size_t i = 0; i < options.writers; ++i)
        paths.push_back(shard_path(options, i));
    for(const auto &path : paths)
    {
        if(std::filesystem::exists(path))
        {
            if(!options.overwrite)
                throw std::runtime_error(path.string() +
                                         " already exists (pass --overwrite to replace outputs)");
            std::filesystem::remove(path);
        }
    }
}

void validate_frame(const FrameView &frame,
                    const TangoBulk::BulkQueryResult &publisher,
                    const Geometry &geometry,
                    std::optional<Endian> &endian)
{
    if(frame.memory_kind() != MemoryKind::Host || frame.element_type() != geometry.element_type ||
       frame.element_size() != geometry.element_size || frame.rank() != publisher.rank ||
       frame.size() != geometry.frame_bytes)
        throw std::runtime_error("stream geometry changed or does not match BulkQuery");
    for(std::size_t i = 0; i < publisher.rank; ++i)
    {
        if(frame.shape()[i] != publisher.shape[i] || frame.strides()[i] != publisher.strides[i])
            throw std::runtime_error("stream geometry changed after BulkQuery");
    }
    if(endian && *endian != frame.endian())
        throw std::runtime_error("stream byte order changed during acquisition");
    endian = frame.endian();
}

void print_usage(const char *program)
{
    std::cerr << "usage: " << program
              << " <device> <master.h5> --frames N [--stream NAME] [--writers N]"
                 " [--frames-per-block N] [--blocks-per-stripe N] [--queue-depth N]"
                 " [--ring-depth N] [--contiguous] [--overwrite]\n";
}

} // namespace

int main(int argc, char *argv[])
{
    Options options;
    try
    {
        options = parse_options(argc, argv);
    }
    catch(const std::exception &error)
    {
        print_usage(argv[0]);
        std::cerr << error.what() << std::endl;
        return 2;
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try
    {
        prepare_outputs(options);
        Tango::DeviceProxy proxy(options.device);
        const TangoBulk::BulkQueryResult publisher = TangoBulk::bulk_query(proxy);
        if(publisher.status != TangoBulk::Status::Ok)
            throw TangoBulk::BulkException(
                {publisher.status, "publisher is not ready", "BulkQuery"});
        const Geometry geometry = geometry_from_publisher(publisher);

        hbool_t hdf5_thread_safe = 0;
        check_hdf5(H5is_library_threadsafe(&hdf5_thread_safe), "H5is_library_threadsafe");
        if(options.writers > 1 && hdf5_thread_safe == 0)
            throw std::runtime_error(
                "multiple writer threads require a thread-safe HDF5 build; use --writers 1");

        const std::uint64_t block_bytes = geometry.frame_bytes * options.frames_per_block;
        if(geometry.frame_bytes != 0 && block_bytes / geometry.frame_bytes != options.frames_per_block)
            throw std::runtime_error("block byte size overflows");
        if(publisher.max_frame_bytes % geometry.element_size != 0)
            throw std::runtime_error("publisher slot size is not an exact number of elements");

        TangoBulk::SubscriberConfig config;
        config.stream_name = options.stream;
        config.max_frame_bytes = publisher.max_frame_bytes;
        config.delivery_mode = TangoBulk::DeliveryMode::Manual;
        config.drop_policy = TangoBulk::DropPolicy::DropNewest;
        config.reconnect_policy = TangoBulk::ReconnectPolicy::BoundedRetry;
        const std::uint64_t slots_in_budget =
            config.pinned_memory_limit_bytes / config.max_frame_bytes;
        const std::uint64_t available_slots =
            std::min<std::uint64_t>(publisher.ring_depth, slots_in_budget);
        if(options.ring_depth > available_slots)
            throw std::runtime_error("requested receive ring exceeds publisher or pinned-memory limit");
        config.ring_depth = static_cast<std::uint32_t>(options.ring_depth);
        config.credit_window = std::min(publisher.credit_window, config.ring_depth);
        config.delivery_queue_depth = config.ring_depth;
        if(options.frames_per_block > config.credit_window)
            throw std::runtime_error("--frames-per-block exceeds the available credit window");
        if(config.max_frame_bytes >
           std::numeric_limits<std::uint64_t>::max() / config.ring_depth)
            throw std::runtime_error("receive-ring byte size overflows");
        const std::uint64_t receive_bytes = config.max_frame_bytes * config.ring_depth;
        auto receive_ring = std::shared_ptr<void>(
            new std::byte[static_cast<std::size_t>(receive_bytes)],
            [](void *memory) { delete[] static_cast<std::byte *>(memory); });
        config.receive_buffer = receive_ring;
        config.receive_buffer_bytes = receive_bytes;
        config.receive_memory_kind = MemoryKind::Host;

        std::cout << "publisher geometry: type=" << TangoBulk::to_string(geometry.element_type)
                  << " rank=" << publisher.rank << " shape=";
        for(std::size_t i = 0; i < publisher.rank; ++i)
            std::cout << (i == 0 ? "[" : ",") << publisher.shape[i];
        std::cout << "] frame_bytes=" << geometry.frame_bytes
                  << " block_bytes=" << block_bytes
                  << " zero_copy_ring_bytes=" << receive_bytes
                  << " retained_frame_limit=" << config.credit_window << std::endl;

        ParallelShardWriter writer(options, geometry);
        TangoBulk::BulkSubscriber subscriber(proxy, config);
        std::uint64_t received = 0;
        std::uint64_t submitted_blocks = 0;
        std::optional<Endian> stream_endian;
        std::vector<FrameView> block;
        block.reserve(options.frames_per_block);

        subscriber.set_frame_callback(
            [&](FrameView frame)
            {
                if(received >= options.frames)
                    return;
                validate_frame(frame, publisher, geometry, stream_endian);
                block.push_back(std::move(frame));
                ++received;
                if(block.size() == options.frames_per_block)
                {
                    writer.submit(submitted_blocks++, std::move(block), *stream_endian);
                    block.clear();
                    block.reserve(options.frames_per_block);
                }
            });
        subscriber.set_state_callback(
            [](TangoBulk::SubscriberState state, const TangoBulk::BulkError &error)
            {
                std::cout << "state: " << TangoBulk::to_string(state);
                if(error.status != TangoBulk::Status::Ok)
                    std::cout << " (" << TangoBulk::to_string(error.status) << ": "
                              << error.message << ")";
                std::cout << std::endl;
            });

        subscriber.start();
        while(running.load() && received < options.frames)
            subscriber.poll(std::chrono::milliseconds{50});
        subscriber.stop();
        writer.close();

        if(received != options.frames)
        {
            std::cerr << "acquisition stopped after " << received
                      << " frames; shard files are retained but no VDS master was created"
                      << std::endl;
            return 130;
        }

        create_master(options, geometry);
        const double seconds = writer.seconds();
        const std::uint64_t bytes = received * geometry.frame_bytes;
        const double gb_per_second =
            seconds > 0.0 ? static_cast<double>(bytes) / seconds / 1'000'000'000.0 : 0.0;
        std::cout << "wrote " << received << " frames, " << bytes << " bytes in " << seconds
                  << " s: " << gb_per_second << " GB/s\n";
        for(std::size_t i = 0; i < writer.workers().size(); ++i)
        {
            const ShardWorker &worker = *writer.workers()[i];
            const double worker_rate = worker.seconds() > 0.0
                                           ? static_cast<double>(worker.bytes_written()) /
                                                 worker.seconds() / 1'000'000'000.0
                                           : 0.0;
            std::cout << "writer=" << i << " blocks=" << worker.blocks_written()
                      << " bytes=" << worker.bytes_written()
                      << " seconds=" << worker.seconds() << " GB/s=" << worker_rate
                      << " peak_queue=" << worker.peak_queue() << " file="
                      << shard_path(options, i) << '\n';
        }
        std::cout << "master=" << options.output << " writers=" << options.writers
                  << " frames_per_block=" << options.frames_per_block
                  << " blocks_per_stripe=" << options.blocks_per_stripe
                  << " queue_depth=" << options.queue_depth << std::endl;
        return 0;
    }
    catch(const TangoBulk::BulkException &error)
    {
        std::cerr << "bulk error (" << TangoBulk::to_string(error.error().status) << ", from "
                  << error.error().origin << "): " << error.error().message << std::endl;
    }
    catch(const Tango::DevFailed &failure)
    {
        Tango::Except::print_exception(failure);
    }
    catch(const std::exception &error)
    {
        std::cerr << "error: " << error.what() << std::endl;
    }
    return 1;
}
