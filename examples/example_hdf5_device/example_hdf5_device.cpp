// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <H5Cpp.h>
#include <tango/tango.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
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

using namespace TangoBulk;

struct Options
{
    std::string file;
    std::string dataset;
    std::uint32_t prefetch_frames{8};
    FanoutMode fanout_mode{FanoutMode::BestEffort};
    std::optional<double> frame_rate;
    bool file_from_command_line{false};
    bool dataset_from_command_line{false};
    bool prefetch_from_command_line{false};
    bool fanout_from_command_line{false};
};

Options options;

std::uint64_t checked_multiply(std::uint64_t left,
                               std::uint64_t right,
                               const char *description)
{
    if(right != 0 && left > std::numeric_limits<std::uint64_t>::max() / right)
    {
        throw std::runtime_error(std::string(description) + " is too large");
    }
    return left * right;
}

std::string option_value(const std::string &argument,
                         const std::string &name,
                         int &index,
                         int argc,
                         char *argv[])
{
    const std::string prefix = name + "=";
    if(argument.compare(0, prefix.size(), prefix) == 0)
    {
        return argument.substr(prefix.size());
    }
    if(argument == name && index + 1 < argc)
    {
        return argv[++index];
    }
    throw std::runtime_error(name + " requires a value");
}

FanoutMode parse_fanout_mode(const std::string &value, const char *origin)
{
    if(value == "best-effort" || value == "BestEffort")
        return FanoutMode::BestEffort;
    if(value == "all-active" || value == "AllActive")
        return FanoutMode::AllActive;
    throw std::runtime_error(std::string(origin) +
                             " must be 'best-effort' or 'all-active'");
}

std::vector<char *> parse_options(int argc, char *argv[])
{
    std::vector<char *> tango_arguments;
    tango_arguments.reserve(static_cast<std::size_t>(argc));
    tango_arguments.push_back(argv[0]);

    for(int i = 1; i < argc; ++i)
    {
        const std::string argument(argv[i]);
        if(argument == "--hdf5-help")
        {
            std::cout
                << "HDF5 replay options:\n"
                << "  --hdf5-file PATH       override Hdf5File (mainly for -nodb)\n"
                << "  --hdf5-dataset PATH    image stack; default follows NeXus default/signal\n"
                << "  --prefetch-frames N    HDF5 frames read ahead into publisher slots (default 8)\n"
                << "  --fanout-mode MODE     best-effort or all-active (default best-effort)\n"
                << "  --frame-rate HZ        replay rate; 0 is unlimited (default: file timing or 100)\n"
                << "Database mode reads Hdf5File, Hdf5Dataset, PrefetchFrames, FanoutMode,\n"
                << "and FrameRate; command-line values take precedence.\n";
            std::exit(0);
        }
        if(argument == "--hdf5-file" || argument.rfind("--hdf5-file=", 0) == 0)
        {
            options.file = option_value(argument, "--hdf5-file", i, argc, argv);
            options.file_from_command_line = true;
        }
        else if(argument == "--hdf5-dataset" || argument.rfind("--hdf5-dataset=", 0) == 0)
        {
            options.dataset = option_value(argument, "--hdf5-dataset", i, argc, argv);
            options.dataset_from_command_line = true;
        }
        else if(argument == "--prefetch-frames" ||
                argument.rfind("--prefetch-frames=", 0) == 0)
        {
            const std::string value = option_value(argument, "--prefetch-frames", i, argc, argv);
            const unsigned long parsed = std::stoul(value);
            if(parsed == 0 || parsed > k_max_ring_depth / 2)
            {
                throw std::runtime_error("--prefetch-frames must be in the range 1.." +
                                         std::to_string(k_max_ring_depth / 2));
            }
            options.prefetch_frames = static_cast<std::uint32_t>(parsed);
            options.prefetch_from_command_line = true;
        }
        else if(argument == "--fanout-mode" || argument.rfind("--fanout-mode=", 0) == 0)
        {
            options.fanout_mode = parse_fanout_mode(
                option_value(argument, "--fanout-mode", i, argc, argv), "--fanout-mode");
            options.fanout_from_command_line = true;
        }
        else if(argument == "--frame-rate" || argument.rfind("--frame-rate=", 0) == 0)
        {
            const std::string value = option_value(argument, "--frame-rate", i, argc, argv);
            const double rate = std::stod(value);
            if(!std::isfinite(rate) || rate < 0.0)
            {
                throw std::runtime_error("--frame-rate must be finite and >= 0");
            }
            options.frame_rate = rate;
        }
        else
        {
            tango_arguments.push_back(argv[i]);
        }
    }

    return tango_arguments;
}

std::string read_string_attribute(const H5::H5Object &object, const char *name)
{
    if(H5Aexists(object.getId(), name) <= 0)
    {
        throw std::runtime_error(std::string("missing HDF5 attribute '") + name + "'");
    }
    H5::Attribute attribute = object.openAttribute(name);
    H5::StrType type = attribute.getStrType();
    H5std_string value;
    attribute.read(type, value);
    return value;
}

std::string absolute_path(const std::string &base, const std::string &child)
{
    if(child.empty())
    {
        throw std::runtime_error("an HDF5 default/signal attribute is empty");
    }
    if(child.front() == '/')
    {
        return child;
    }
    return base == "/" ? "/" + child : base + "/" + child;
}

std::string parent_path(const std::string &path)
{
    const std::size_t slash = path.find_last_of('/');
    return slash == 0 ? "/" : path.substr(0, slash);
}

std::string discover_nexus_dataset(H5::H5File &file)
{
    const H5::Group root = file.openGroup("/");
    const std::string entry_path = absolute_path("/", read_string_attribute(root, "default"));
    const H5::Group entry = file.openGroup(entry_path);
    const std::string plot_path = absolute_path(entry_path, read_string_attribute(entry, "default"));
    const H5::Group plot = file.openGroup(plot_path);
    return absolute_path(plot_path, read_string_attribute(plot, "signal"));
}

struct DataDescription
{
    ElementType element_type{ElementType::Unknown};
    Endian endian{Endian::Little};
    std::uint64_t element_bytes{0};
};

DataDescription describe_type(const H5::DataType &type)
{
    DataDescription result;
    result.element_bytes = type.getSize();
    const H5T_class_t type_class = type.getClass();

    if(type_class == H5T_INTEGER)
    {
        const bool is_unsigned = H5Tget_sign(type.getId()) == H5T_SGN_NONE;
        if(result.element_bytes == 1)
            result.element_type = is_unsigned ? ElementType::UInt8 : ElementType::Int8;
        else if(result.element_bytes == 2)
            result.element_type = is_unsigned ? ElementType::UInt16 : ElementType::Int16;
        else if(result.element_bytes == 4)
            result.element_type = is_unsigned ? ElementType::UInt32 : ElementType::Int32;
        else if(result.element_bytes == 8)
            result.element_type = is_unsigned ? ElementType::UInt64 : ElementType::Int64;
    }
    else if(type_class == H5T_FLOAT)
    {
        if(result.element_bytes == 4)
            result.element_type = ElementType::Float32;
        else if(result.element_bytes == 8)
            result.element_type = ElementType::Float64;
    }

    if(result.element_type == ElementType::Unknown)
    {
        throw std::runtime_error("the image dataset must contain 8/16/32/64-bit integers or 32/64-bit floats");
    }

    const H5T_order_t order = H5Tget_order(type.getId());
    if(result.element_bytes > 1 && order != H5T_ORDER_LE && order != H5T_ORDER_BE)
    {
        throw std::runtime_error("the image dataset uses an unsupported byte order");
    }
    result.endian = order == H5T_ORDER_BE ? Endian::Big : Endian::Little;
    return result;
}

class ReplayReader
{
  public:
    ReplayReader(const Options &configuration) :
        file_(configuration.file, H5F_ACC_RDONLY),
        dataset_path_(configuration.dataset.empty() ? discover_nexus_dataset(file_)
                                                     : configuration.dataset),
        dataset_(file_.openDataSet(dataset_path_)),
        file_type_(dataset_.getDataType()),
        description_(describe_type(file_type_))
    {
        const H5::DataSpace space = dataset_.getSpace();
        const int rank = space.getSimpleExtentNdims();
        if(rank < 2 || rank > static_cast<int>(k_max_rank) + 1)
        {
            throw std::runtime_error("the image dataset rank must be 2..5 ([frame, ...image axes])");
        }

        dimensions_.resize(static_cast<std::size_t>(rank));
        space.getSimpleExtentDims(dimensions_.data());
        frame_count_ = rank == 2 ? 1 : dimensions_.front();
        if(frame_count_ == 0)
        {
            throw std::runtime_error("the image dataset contains no frames");
        }

        frame_shape_.assign(rank == 2 ? dimensions_.begin() : dimensions_.begin() + 1,
                            dimensions_.end());
        std::uint64_t elements = 1;
        for(const hsize_t dimension : frame_shape_)
        {
            if(dimension == 0)
                throw std::runtime_error("the image dataset has a zero-sized image axis");
            elements = checked_multiply(elements, dimension, "frame element count");
        }
        frame_bytes_ = checked_multiply(elements, description_.element_bytes, "frame size");
        if(frame_bytes_ > k_max_frame_bytes_hard_cap)
        {
            throw std::runtime_error("a frame exceeds tango-bulk's 256 MiB hard limit");
        }
    }

    void read_frame(std::uint64_t index, void *destination)
    {
        if(index >= frame_count_)
        {
            throw std::runtime_error("HDF5 frame index is out of range");
        }

        H5::DataSpace file_space = dataset_.getSpace();
        std::vector<hsize_t> start(dimensions_.size(), 0);
        std::vector<hsize_t> count = dimensions_;
        if(dimensions_.size() > 2)
        {
            start[0] = index;
            count[0] = 1;
        }
        file_space.selectHyperslab(H5S_SELECT_SET, count.data(), start.data());
        H5::DataSpace memory_space(static_cast<int>(count.size()), count.data());
        dataset_.read(destination, file_type_, memory_space, file_space);
    }

    double suggested_rate() const
    {
        // A NeXus signal is commonly opened through NXdata/plot/data, while
        // acquisition is a sibling of the link target (Areascan/data). Walk
        // upward so both a direct dataset path and that soft-link layout work.
        std::string group = parent_path(dataset_path_);
        while(group != "/")
        {
            try
            {
                const std::string acquisition = group + "/acquisition/";
                const double exposure = read_scalar_double(acquisition + "exposure_time");
                const double latency = read_scalar_double(acquisition + "latency_time");
                const double period = exposure + latency;
                if(std::isfinite(period) && period > 0.0)
                    return 1.0 / period;
            }
            catch(const H5::Exception &)
            {
                // Keep looking; timing metadata is optional.
            }
            group = parent_path(group);
        }
        return 100.0;
    }

    std::uint64_t frame_count() const noexcept { return frame_count_; }
    std::uint64_t frame_bytes() const noexcept { return frame_bytes_; }
    const std::vector<hsize_t> &frame_shape() const noexcept { return frame_shape_; }
    const std::string &dataset_path() const noexcept { return dataset_path_; }
    const DataDescription &description() const noexcept { return description_; }

  private:
    double read_scalar_double(const std::string &path) const
    {
        const H5::DataSet value = file_.openDataSet(path);
        double result = 0.0;
        value.read(&result, H5::PredType::NATIVE_DOUBLE);
        return result;
    }

    H5::H5File file_;
    std::string dataset_path_;
    H5::DataSet dataset_;
    H5::DataType file_type_;
    DataDescription description_;
    std::vector<hsize_t> dimensions_;
    std::vector<hsize_t> frame_shape_;
    std::uint64_t frame_count_{0};
    std::uint64_t frame_bytes_{0};
};

enum class ReplayAttribute
{
    FrameWidth,
    FrameHeight,
    DatasetFrames,
    PrefetchFrames,
    ReadyFrames,
    FrameRate
};

class Hdf5ReplayDetector : public TANGO_BASE_CLASS
{
  public:
    Hdf5ReplayDetector(Tango::DeviceClass *owner, const std::string &identifier) :
        TANGO_BASE_CLASS(owner, identifier.c_str())
    {
        init_device();
    }

    ~Hdf5ReplayDetector() override { delete_device(); }

    void init_device() override
    {
        H5::Exception::dontPrint();
        configuration_ = read_configuration();
        replay_ = std::make_unique<ReplayReader>(configuration_);
        frame_rate_.store(configuration_.frame_rate.value_or(replay_->suggested_rate()));
        frame_width_read_ = replay_->frame_shape().back();
        frame_height_read_ = replay_->frame_shape().size() > 1
                                 ? replay_->frame_shape()[replay_->frame_shape().size() - 2]
                                 : 1;
        dataset_frames_read_ = replay_->frame_count();
        prefetch_frames_read_ = configuration_.prefetch_frames;

        PublisherConfig config;
        config.stream_name = "image";
        config.frame_metadata.element_type = replay_->description().element_type;
        config.frame_metadata.element_size =
            static_cast<std::uint32_t>(replay_->description().element_bytes);
        config.frame_metadata.rank = static_cast<std::uint32_t>(replay_->frame_shape().size());
        for(std::size_t i = 0; i < replay_->frame_shape().size(); ++i)
            config.frame_metadata.shape[i] = replay_->frame_shape()[i];
        config.frame_metadata.endian = replay_->description().endian;
        // The transport has a 4 KiB minimum slot size; small test images still
        // carry their exact payload_bytes in FrameMetadata.
        config.max_frame_bytes = std::max<std::uint64_t>(k_min_frame_bytes,
                                                         replay_->frame_bytes());
        config.ring_depth = configuration_.prefetch_frames * 2;
        config.credit_window = configuration_.prefetch_frames;
        config.max_sessions = 4;
        config.fanout_mode = configuration_.fanout_mode;
        config.pinned_memory_limit_bytes = std::max<std::uint64_t>(
            1ull << 30, checked_multiply(config.max_frame_bytes, config.ring_depth + 1,
                                        "publisher memory budget"));

        publisher_ = std::make_unique<BulkPublisher>(std::move(config));
        attach_publisher(*this, *publisher_);

        running_ = true;
        metrics_running_ = true;
        set_state(Tango::RUNNING);
        set_status("replaying " + configuration_.file + ":" + replay_->dataset_path() + "; " +
                   std::to_string(replay_->frame_count()) + " frames, " +
                   std::to_string(configuration_.prefetch_frames) + " prefetched, fanout=" +
                   to_string(configuration_.fanout_mode));
        loader_thread_ = std::thread([this] { loader_loop(); });
        replay_thread_ = std::thread([this] { replay_loop(); });
        metrics_thread_ = std::thread([this] { metrics_loop(); });
    }

    void delete_device() override
    {
        running_ = false;
        ready_changed_.notify_all();
        if(loader_thread_.joinable())
            loader_thread_.join();
        if(replay_thread_.joinable())
            replay_thread_.join();
        metrics_running_ = false;
        metrics_changed_.notify_all();
        if(metrics_thread_.joinable())
            metrics_thread_.join();
        {
            std::lock_guard<std::mutex> lock(ready_mutex_);
            ready_.clear();
            ready_frames_.store(0);
        }
        if(publisher_)
            detach_publisher(*this);
        publisher_.reset();
        replay_.reset();
    }

    void read_replay_attribute(Tango::Attribute &attribute, ReplayAttribute which)
    {
        switch(which)
        {
        case ReplayAttribute::FrameWidth:
            attribute.set_value(&frame_width_read_);
            break;
        case ReplayAttribute::FrameHeight:
            attribute.set_value(&frame_height_read_);
            break;
        case ReplayAttribute::DatasetFrames:
            attribute.set_value(&dataset_frames_read_);
            break;
        case ReplayAttribute::PrefetchFrames:
            attribute.set_value(&prefetch_frames_read_);
            break;
        case ReplayAttribute::ReadyFrames:
            ready_frames_read_ = ready_frames_.load();
            attribute.set_value(&ready_frames_read_);
            break;
        case ReplayAttribute::FrameRate:
            double_read_ = frame_rate_.load();
            attribute.set_value(&double_read_);
            break;
        }
    }

    void write_frame_rate(Tango::WAttribute &attribute)
    {
        Tango::DevDouble value;
        attribute.get_write_value(value);
        if(!std::isfinite(value) || value < 0.0)
        {
            Tango::Except::throw_exception("InvalidFrameRate", "frameRate must be finite and >= 0",
                                           "Hdf5ReplayDetector::write_frame_rate");
        }
        frame_rate_.store(value);
    }

  private:
    Options read_configuration()
    {
        Options result = options;
        if(Tango::Util::instance()->use_db())
        {
            Tango::DbData properties;
            properties.emplace_back("Hdf5File");
            properties.emplace_back("Hdf5Dataset");
            properties.emplace_back("PrefetchFrames");
            properties.emplace_back("FanoutMode");
            properties.emplace_back("FrameRate");
            get_db_device()->get_property(properties);

            if(!result.file_from_command_line && !properties[0].is_empty())
                properties[0] >> result.file;
            if(!result.dataset_from_command_line && !properties[1].is_empty())
                properties[1] >> result.dataset;
            if(!result.prefetch_from_command_line && !properties[2].is_empty())
            {
                Tango::DevULong64 value = 0;
                if(!(properties[2] >> value))
                    throw std::runtime_error(
                        "device property PrefetchFrames is not an unsigned integer");
                if(value == 0 || value > k_max_ring_depth / 2)
                {
                    throw std::runtime_error("device property PrefetchFrames must be in the range 1.." +
                                             std::to_string(k_max_ring_depth / 2));
                }
                result.prefetch_frames = static_cast<std::uint32_t>(value);
            }
            if(!result.fanout_from_command_line && !properties[3].is_empty())
            {
                std::string value;
                if(!(properties[3] >> value))
                    throw std::runtime_error("device property FanoutMode is not a string");
                result.fanout_mode = parse_fanout_mode(value, "device property FanoutMode");
            }
            if(!result.frame_rate && !properties[4].is_empty())
            {
                double value = 0.0;
                if(!(properties[4] >> value))
                    throw std::runtime_error("device property FrameRate is not numeric");
                result.frame_rate = value;
            }
        }

        if(result.file.empty())
        {
            throw std::runtime_error(
                "Hdf5File device property is required (or use --hdf5-file in -nodb mode)");
        }
        if(result.prefetch_frames == 0 || result.prefetch_frames > k_max_ring_depth / 2)
            throw std::runtime_error("PrefetchFrames must produce a valid publisher ring depth");
        if(result.frame_rate && (!std::isfinite(*result.frame_rate) || *result.frame_rate < 0.0))
            throw std::runtime_error("device property FrameRate must be finite and >= 0");
        return result;
    }

    struct PreparedFrame
    {
        std::uint64_t dataset_frame{0};
        BulkPublisher::SlotHandle lease;
    };

    void restart_prefetch(std::uint64_t dataset_frame)
    {
        std::lock_guard<std::mutex> lock(ready_mutex_);
        ++load_epoch_;
        next_frame_to_load_ = dataset_frame;
        ready_.clear();
        ready_frames_.store(0);
        ready_changed_.notify_all();
    }

    void loader_loop()
    {
        try
        {
            while(running_)
            {
                std::uint64_t dataset_frame = 0;
                std::uint64_t epoch = 0;
                {
                    std::unique_lock<std::mutex> lock(ready_mutex_);
                    ready_changed_.wait(lock,
                                        [this]
                                        {
                                            return !running_ ||
                                                   ready_.size() < configuration_.prefetch_frames;
                                        });
                    if(!running_)
                        break;
                    dataset_frame = next_frame_to_load_;
                    next_frame_to_load_ = (next_frame_to_load_ + 1) % replay_->frame_count();
                    epoch = load_epoch_;
                }

                const auto lease_started = std::chrono::steady_clock::now();
                BulkPublisher::SlotHandle lease;
                while(running_ && !lease)
                {
                    lease = publisher_->try_acquire();
                    if(!lease)
                        std::this_thread::yield();
                }
                if(!running_)
                    break;
                const auto lease_finished = std::chrono::steady_clock::now();
                const double lease_microseconds =
                    std::chrono::duration<double, std::micro>(lease_finished - lease_started)
                        .count();
                {
                    std::lock_guard<std::mutex> lock(metrics_mutex_);
                    lease_latencies_us_.push_back(lease_microseconds);
                }

                replay_->read_frame(dataset_frame, lease.data());

                std::lock_guard<std::mutex> lock(ready_mutex_);
                if(running_ && epoch == load_epoch_)
                {
                    ready_.push_back({dataset_frame, std::move(lease)});
                    ready_frames_.store(ready_.size());
                    ready_changed_.notify_all();
                }
            }
        }
        catch(...)
        {
            {
                std::lock_guard<std::mutex> lock(ready_mutex_);
                loader_failure_ = std::current_exception();
            }
            running_ = false;
            ready_changed_.notify_all();
        }
    }

    PreparedFrame next_ready_frame()
    {
        std::unique_lock<std::mutex> lock(ready_mutex_);
        ready_changed_.wait(lock,
                            [this]
                            {
                                return !running_ || loader_failure_ || !ready_.empty();
                            });
        if(loader_failure_)
            std::rethrow_exception(loader_failure_);
        if(!running_ || ready_.empty())
            return {};

        PreparedFrame frame = std::move(ready_.front());
        ready_.pop_front();
        ready_frames_.store(ready_.size());
        ready_changed_.notify_all();
        return frame;
    }

    void rethrow_loader_failure()
    {
        std::lock_guard<std::mutex> lock(ready_mutex_);
        if(loader_failure_)
            std::rethrow_exception(loader_failure_);
    }

    void replay_loop()
    {
        std::uint64_t event = 0;
        auto next_frame = std::chrono::steady_clock::now();

        try
        {
            while(running_)
            {
                while(running_ && publisher_->session_count() == 0)
                    std::this_thread::yield();
                if(!running_)
                {
                    rethrow_loader_failure();
                    break;
                }

                PreparedFrame frame = next_ready_frame();
                if(!frame.lease)
                    break;

                FrameMetadata metadata;
                metadata.element_type = replay_->description().element_type;
                metadata.element_size = static_cast<std::uint32_t>(replay_->description().element_bytes);
                metadata.rank = static_cast<std::uint32_t>(replay_->frame_shape().size());
                for(std::size_t i = 0; i < replay_->frame_shape().size(); ++i)
                    metadata.shape[i] = replay_->frame_shape()[i];
                metadata.payload_bytes = replay_->frame_bytes();
                metadata.event_counter = event;
                metadata.endian = replay_->description().endian;

                PublishResult result = PublishResult::QueueFull;
                while(running_ && (result == PublishResult::QueueFull ||
                                   result == PublishResult::WouldBlock))
                {
                    result = publisher_->publish(std::move(frame.lease), metadata);
                    if(result == PublishResult::QueueFull || result == PublishResult::WouldBlock)
                        std::this_thread::yield();
                }
                if(result == PublishResult::Accepted)
                {
                    ++event;
                    published_frames_.fetch_add(1, std::memory_order_relaxed);
                    published_bytes_.fetch_add(replay_->frame_bytes(), std::memory_order_relaxed);
                }
                else if(result == PublishResult::NoSession ||
                        result == PublishResult::CreditStalled)
                {
                    restart_prefetch(frame.dataset_frame);
                    continue;
                }
                else if(result == PublishResult::Shutdown)
                {
                    break;
                }
                else
                {
                    throw std::runtime_error("publish failed: " + std::string(to_string(result)));
                }

                const double rate = frame_rate_.load();
                if(rate > 0.0)
                {
                    const auto period = std::chrono::duration<double>(1.0 / rate);
                    next_frame += std::chrono::duration_cast<std::chrono::steady_clock::duration>(period);
                    const auto now = std::chrono::steady_clock::now();
                    if(next_frame > now)
                        std::this_thread::sleep_until(next_frame);
                    else
                        next_frame = now;
                }
                else
                {
                    next_frame = std::chrono::steady_clock::now();
                }
            }
            rethrow_loader_failure();
        }
        catch(const H5::Exception &failure)
        {
            set_state(Tango::FAULT);
            set_status("HDF5 replay failed: " + failure.getDetailMsg());
        }
        catch(const std::exception &failure)
        {
            set_state(Tango::FAULT);
            set_status("HDF5 replay failed: " + std::string(failure.what()));
        }
    }

    static double percentile(const std::vector<double> &sorted, double fraction)
    {
        const std::size_t rank = static_cast<std::size_t>(
            std::ceil(fraction * static_cast<double>(sorted.size())));
        return sorted[std::max<std::size_t>(rank, 1) - 1];
    }

    void metrics_loop()
    {
        using Clock = std::chrono::steady_clock;
        auto interval_started = Clock::now();

        while(metrics_running_)
        {
            {
                std::unique_lock<std::mutex> lock(metrics_wait_mutex_);
                metrics_changed_.wait_for(lock, std::chrono::seconds(1),
                                          [this] { return !metrics_running_; });
            }

            const auto now = Clock::now();
            const double seconds = std::chrono::duration<double>(now - interval_started).count();
            interval_started = now;
            const std::uint64_t frames =
                published_frames_.exchange(0, std::memory_order_relaxed);
            const std::uint64_t bytes =
                published_bytes_.exchange(0, std::memory_order_relaxed);
            std::vector<double> latencies;
            {
                std::lock_guard<std::mutex> lock(metrics_mutex_);
                latencies.swap(lease_latencies_us_);
            }

            std::ostringstream line;
            line << std::fixed << std::setprecision(3) << "hdf5 replay metrics: fps="
                 << (seconds > 0.0 ? static_cast<double>(frames) / seconds : 0.0)
                 << " GB/s="
                 << (seconds > 0.0 ? static_cast<double>(bytes) / seconds / 1'000'000'000.0
                                    : 0.0)
                 << " frames=" << frames << " lease_acquire_us=";
            if(latencies.empty())
            {
                line << "n/a";
            }
            else
            {
                std::sort(latencies.begin(), latencies.end());
                line << "p50=" << percentile(latencies, 0.50)
                     << ",p95=" << percentile(latencies, 0.95)
                     << ",p99=" << percentile(latencies, 0.99)
                     << ",max=" << latencies.back() << ",samples=" << latencies.size();
            }
            std::clog << line.str() << std::endl;
        }
    }

    Options configuration_;
    std::unique_ptr<ReplayReader> replay_;
    std::unique_ptr<BulkPublisher> publisher_;
    std::thread loader_thread_;
    std::thread replay_thread_;
    std::thread metrics_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> metrics_running_{false};
    std::atomic<double> frame_rate_{100.0};
    std::mutex ready_mutex_;
    std::condition_variable ready_changed_;
    std::deque<PreparedFrame> ready_;
    std::exception_ptr loader_failure_;
    std::uint64_t next_frame_to_load_{0};
    std::uint64_t load_epoch_{0};
    std::atomic<std::uint64_t> ready_frames_{0};
    std::atomic<std::uint64_t> published_frames_{0};
    std::atomic<std::uint64_t> published_bytes_{0};
    std::mutex metrics_mutex_;
    std::vector<double> lease_latencies_us_;
    std::mutex metrics_wait_mutex_;
    std::condition_variable metrics_changed_;
    Tango::DevULong64 frame_width_read_{0};
    Tango::DevULong64 frame_height_read_{0};
    Tango::DevULong64 dataset_frames_read_{0};
    Tango::DevULong64 prefetch_frames_read_{0};
    Tango::DevULong64 ready_frames_read_{0};
    Tango::DevDouble double_read_{0};
};

class ReplayAttr : public Tango::Attr
{
  public:
    ReplayAttr(const char *attribute_name,
               long data_type,
               ReplayAttribute which,
               Tango::AttrWriteType write_type) :
        Tango::Attr(attribute_name, data_type, write_type), which_(which)
    {
    }

    void read(Tango::DeviceImpl *device, Tango::Attribute &attribute) override
    {
        static_cast<Hdf5ReplayDetector *>(device)->read_replay_attribute(attribute, which_);
    }

    void write(Tango::DeviceImpl *device, Tango::WAttribute &attribute) override
    {
        static_cast<Hdf5ReplayDetector *>(device)->write_frame_rate(attribute);
    }

  private:
    ReplayAttribute which_;
};

class Hdf5ReplayDetectorClass : public Tango::DeviceClass
{
  public:
    explicit Hdf5ReplayDetectorClass(const std::string &class_name) :
        Tango::DeviceClass(class_name)
    {
    }

    void command_factory() override { install_bulk_commands(*this); }

    void attribute_factory(std::vector<Tango::Attr *> &attributes) override
    {
        attributes.push_back(new ReplayAttr("frameWidth", Tango::DEV_ULONG64,
                                            ReplayAttribute::FrameWidth, Tango::READ));
        attributes.push_back(new ReplayAttr("frameHeight", Tango::DEV_ULONG64,
                                            ReplayAttribute::FrameHeight, Tango::READ));
        attributes.push_back(new ReplayAttr("datasetFrames", Tango::DEV_ULONG64,
                                            ReplayAttribute::DatasetFrames, Tango::READ));
        attributes.push_back(new ReplayAttr("prefetchFrames", Tango::DEV_ULONG64,
                                            ReplayAttribute::PrefetchFrames, Tango::READ));
        attributes.push_back(new ReplayAttr("readyFrames", Tango::DEV_ULONG64,
                                            ReplayAttribute::ReadyFrames, Tango::READ));
        attributes.push_back(new ReplayAttr("frameRate", Tango::DEV_DOUBLE,
                                            ReplayAttribute::FrameRate, Tango::READ_WRITE));
    }

    void device_factory(const Tango::DevVarStringArray *devices) override
    {
        for(CORBA::ULong i = 0; i < devices->length(); ++i)
            device_list.push_back(new Hdf5ReplayDetector(this, std::string((*devices)[i])));
        for(auto *device : device_list)
            export_device(device, device->get_name().c_str());
    }
};

class ExampleDServer : public Tango::DServer
{
  public:
    using Tango::DServer::DServer;

  private:
    void class_factory() override
    {
        add_class(new Hdf5ReplayDetectorClass("Hdf5ReplayDetector"));
    }
};

} // namespace

int main(int argc, char *argv[])
{
    try
    {
        std::vector<char *> tango_arguments = parse_options(argc, argv);
        argc = static_cast<int>(tango_arguments.size());
        argv = tango_arguments.data();
        Tango::Util *util = Tango::Util::init(argc, argv);
        util->register_dserver_constructor(
            [](Tango::DeviceClass *device_class, const std::string &name,
               const std::string &description, Tango::DevState state,
               const std::string &status) -> Tango::DServer *
            {
                return new ExampleDServer(device_class, name.c_str(), description.c_str(), state,
                                          status.c_str());
            });
        util->server_init();
        util->server_run();
        util->server_cleanup();
    }
    catch(const Tango::DevFailed &failure)
    {
        Tango::Except::print_exception(failure);
        return 1;
    }
    catch(const H5::Exception &failure)
    {
        std::cerr << "example_hdf5_device: " << failure.getDetailMsg() << std::endl;
        return 1;
    }
    catch(const std::exception &failure)
    {
        std::cerr << "example_hdf5_device: " << failure.what() << std::endl;
        return 1;
    }
    return 0;
}
