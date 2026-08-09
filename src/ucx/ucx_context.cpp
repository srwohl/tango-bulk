// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/ucx_context.h>

#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>

#include <cstring>
#include <string>

namespace TangoBulk::detail
{

void throw_ucx_error(const char *what, ucs_status_t status, const char *origin)
{
    std::string message = what;
    message += ": ";
    message += ucs_status_string(status);

    // TransportFailure rather than Internal: these are all "UCX could not do
    // the thing", and a caller distinguishing a bad configuration from a broken
    // fabric wants the message, not a finer enum.
    throw BulkException(BulkError{Status::TransportFailure, std::move(message), origin});
}

UcxContext::UcxContext(const std::string &tls)
{
    ucp_config_t *config = nullptr;
    ucs_status_t status = ucp_config_read(nullptr, nullptr, &config);
    if(status != UCS_OK)
    {
        throw_ucx_error("ucp_config_read", status, "publisher");
    }

    if(!tls.empty())
    {
        status = ucp_config_modify(config, "TLS", tls.c_str());
        if(status != UCS_OK)
        {
            ucp_config_release(config);
            throw_ucx_error("ucp_config_modify(TLS)", status, "publisher");
        }
    }

    ucp_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    // AM only.  RMA is added in M5 behind transport_selected == 2; asking for it
    // now would change transport selection for a path that does not exist yet.
    params.features = UCP_FEATURE_AM;

    status = ucp_init(&params, config, &context_);
    ucp_config_release(config);

    if(status != UCS_OK)
    {
        throw_ucx_error("ucp_init", status, "publisher");
    }
}

UcxContext::~UcxContext()
{
    if(context_ != nullptr)
    {
        ucp_cleanup(context_);
    }
}

UcxWorker::UcxWorker(UcxContext &context)
{
    ucp_worker_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    params.thread_mode = UCS_THREAD_MODE_SINGLE;

    ucs_status_t status = ucp_worker_create(context.get(), &params, &worker_);
    if(status != UCS_OK)
    {
        throw_ucx_error("ucp_worker_create", status, "publisher");
    }

    ucp_worker_attr_t attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS | UCP_WORKER_ATTR_FIELD_MAX_AM_HEADER;

    status = ucp_worker_query(worker_, &attr);
    if(status != UCS_OK)
    {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
        throw_ucx_error("ucp_worker_query", status, "publisher");
    }

    address_.resize(attr.address_length);
    std::memcpy(address_.data(), attr.address, attr.address_length);
    max_am_header_ = attr.max_am_header;
    ucp_worker_release_address(worker_, attr.address);

    if(address_.size() > k_max_ucx_address_bytes)
    {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
        throw BulkException(BulkError{Status::Internal,
                                      "UCX worker address is " + std::to_string(address_.size()) +
                                          " bytes, above the protocol's 4096-byte field",
                                      "publisher"});
    }

    // 3.10: query max_am_header at startup and fail construction if it is below
    // the frame header size.  Discovering that limit at the first frame -- in an
    // AM callback, under load, with a detector running -- is not acceptable.
    if(max_am_header_ < Protocol::k_frame_header_bytes)
    {
        const std::size_t got = max_am_header_;
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
        throw BulkException(
            BulkError{Status::Internal,
                      "transport reports max_am_header " + std::to_string(got) +
                          ", below the " + std::to_string(Protocol::k_frame_header_bytes) +
                          "-byte frame header; this transport cannot carry the protocol",
                      "publisher"});
    }
}

UcxWorker::~UcxWorker()
{
    if(worker_ != nullptr)
    {
        ucp_worker_destroy(worker_);
    }
}

ucp_ep_h UcxWorker::create_endpoint(const std::byte *peer_address, std::size_t size)
{
    if(peer_address == nullptr || size == 0 || size > k_max_ucx_address_bytes)
    {
        throw BulkException(
            BulkError{Status::MalformedMessage, "peer UCX address is empty or oversize", "protocol"});
    }

    ucp_ep_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    params.address = reinterpret_cast<const ucp_address_t *>(peer_address);
    params.err_mode = UCP_ERR_HANDLING_MODE_NONE;

    ucp_ep_h ep = nullptr;
    const ucs_status_t status = ucp_ep_create(worker_, &params, &ep);
    if(status != UCS_OK)
    {
        throw_ucx_error("ucp_ep_create", status, "publisher");
    }

    return ep;
}

} // namespace TangoBulk::detail
