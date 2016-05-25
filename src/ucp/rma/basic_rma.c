/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
* Copyright (c) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include <ucp/core/ucp_mm.h>

#include <ucp/core/ucp_ep.h>
#include <ucp/core/ucp_worker.h>
#include <ucp/core/ucp_context.h>
#include <ucp/core/ucp_request.h>
#include <ucp/dt/dt_contig.h>
#include <ucs/datastruct/mpool.inl>
#include <ucp/core/ucp_ep.inl>


#define UCP_RMA_CHECK_PARAMS(_buffer, _length) \
    if ((_length) == 0) { \
        return UCS_OK; \
    } \
    if (ENABLE_PARAMS_CHECK && ((_buffer) == NULL)) { \
        return UCS_ERR_INVALID_PARAM; \
    }

#define _USE_ZCOPY 1

ucs_status_t ucp_put(ucp_ep_h ep, const void *buffer, size_t length,
                     uint64_t remote_addr, ucp_rkey_h rkey)
{
    ucp_ep_rma_config_t *rma_config;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    size_t frag_length;
    ssize_t packed_len;
    uct_ep_h uct_ep;

    UCP_RMA_CHECK_PARAMS(buffer, length);

    /* Loop until all message has been sent.
     * We re-check the configuration on every iteration, because it can be
     * changed by transport switch.
     */
#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;

    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];
    rma_config = &config->rma[lane];

    if (length <= rma_config->max_put_short) {
        status = uct_ep_put_short(uct_ep, buffer,
                                  length, remote_addr,
                                  uct_rkey);
    } else  if (length <= rma_config->max_put_bcopy) {
        ucp_memcpy_pack_context_t pack_ctx;
        pack_ctx.src    = buffer;
        pack_ctx.length = length;
        packed_len = uct_ep_put_bcopy(uct_ep,
                                      ucp_memcpy_pack, &pack_ctx,
                                      remote_addr, uct_rkey);

        status = (packed_len > 0) ? UCS_OK : (ucs_status_t)packed_len;
        ucp_worker_progress(ep->worker);
    } else {
        uct_completion_t uct_comp;
        uct_mem_h mem;
        uct_pd_h uct_pd = ucp_ep_pd(ep, lane);

        uct_comp.count = 1;

        status = uct_pd_mem_reg(uct_pd, (void*)buffer, length, &mem);
        if (status != UCS_OK) {
            ucs_error("failed to register user buffer: %s",
                      ucs_status_string(status));
        }
        for(;;) {
            frag_length = ucs_min(length, config->max_am_zcopy);
            status = uct_ep_put_zcopy(uct_ep,
                                      buffer, frag_length,
                                      mem, remote_addr,
                                      uct_rkey, &uct_comp);

            length -= frag_length;
            ++uct_comp.count;
            if (length == 0) {
                /* Put is completed - return success */
                ucp_worker_progress(ep->worker);
                status = UCS_OK;
                break;
            }
            ucp_worker_progress(ep->worker);
            if (UCS_ERR_NO_RESOURCE == status) {
                continue;
            }
            buffer += frag_length;
            remote_addr += frag_length;
        }
        while (1 < uct_comp.count) {
            ucp_worker_progress(ep->worker);
        }
    }
#else
    for (;;) {
        UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
        if (length <= rma_config->max_put_short) {
            status = uct_ep_put_short(uct_ep, buffer, length, remote_addr,
                                      uct_rkey);
            if (ucs_likely(status != UCS_ERR_NO_RESOURCE)) {
                break;
            }
        } else {
            if (length <= ucp_ep_config(ep)->bcopy_thresh) {
                frag_length = ucs_min(length, rma_config->max_put_short);
                status = uct_ep_put_short(uct_ep, buffer, frag_length, remote_addr,
                                          uct_rkey);
            } else {
                ucp_memcpy_pack_context_t pack_ctx;
                pack_ctx.src    = buffer;
                pack_ctx.length = frag_length =
                                ucs_min(length, rma_config->max_put_bcopy);
                packed_len = uct_ep_put_bcopy(uct_ep, ucp_memcpy_pack, &pack_ctx,
                                              remote_addr, uct_rkey);
                status = (packed_len > 0) ? UCS_OK : (ucs_status_t)packed_len;
            }
            if (ucs_likely(status == UCS_OK)) {
                length      -= frag_length;
                if (length == 0) {
                    break;
                }

                buffer      += frag_length;
                remote_addr += frag_length;
            } else if (status != UCS_ERR_NO_RESOURCE) {
                break;
            }
        }
        ucp_worker_progress(ep->worker);
    }
#endif

    return status;
}

static ucs_status_t ucp_progress_put_nbi(uct_pending_req_t *self)
{
    ucp_request_t *req = ucs_container_of(self, ucp_request_t, send.uct);
    ucp_rkey_h rkey    = req->send.rma.rkey;
    ucp_ep_t *ep       = req->send.ep;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    uct_ep_h uct_ep;

#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;

    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];
    for (;;) {
        size_t frag_length = ucs_min(req->send.length, config->max_am_zcopy);
        status = uct_ep_put_zcopy(uct_ep,
                                  req->send.buffer, frag_length,
                                  req->send.mem, req->send.rma.remote_addr,
                                  uct_rkey, NULL);

        if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
            req->send.length -= frag_length;
            if (req->send.length == 0) {
                /* Put is completed - return success */
                break;
            }
            req->send.buffer += frag_length;
            req->send.rma.remote_addr += frag_length;
        } else {
            break;
        }
    }
#else
    ucp_ep_rma_config_t *rma_config;
    ssize_t packed_len;

    UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
    for (;;) {
        if (req->send.length <= ep->worker->context->config.ext.bcopy_thresh) {
            /* Should be replaced with bcopy */
            packed_len = ucs_min(req->send.length, rma_config->max_put_short);
            status = uct_ep_put_short(uct_ep,
                                      req->send.buffer,
                                      packed_len,
                                      req->send.rma.remote_addr,
                                      uct_rkey);
        } else {
            /* We don't do it right now, but in future we have to add
             * an option to use zcopy
             */
            ucp_memcpy_pack_context_t pack_ctx;
            pack_ctx.src    = req->send.buffer;
            pack_ctx.length =
                ucs_min(req->send.length, rma_config->max_put_bcopy);
            packed_len = uct_ep_put_bcopy(uct_ep,
                                          ucp_memcpy_pack,
                                          &pack_ctx,
                                          req->send.rma.remote_addr,
                                          uct_rkey);
            status = (packed_len > 0) ? UCS_OK : (ucs_status_t)packed_len;
        }

        if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
            req->send.length -= packed_len;
            if (req->send.length == 0) {
                ucp_request_complete(req, void);
                break;
            }

            req->send.buffer += packed_len;
            req->send.rma.remote_addr += packed_len;
        } else {
            break;
        }
    }
#endif

    return status;
}

static UCS_F_ALWAYS_INLINE
void ucp_add_pending_rma(ucp_request_t *req, ucp_ep_h ep, uct_ep_h uct_ep,
                         const void *buffer, size_t length, uint64_t remote_addr,
                         ucp_rkey_h rkey, uct_pending_callback_t cb)
{
    req->send.ep = ep;
    req->send.buffer = buffer;
    req->send.length = length;
    req->send.rma.remote_addr = remote_addr;
    req->send.rma.rkey = rkey;
    req->send.uct.func = cb;
    req->flags = UCP_REQUEST_FLAG_RELEASED;
    ucp_ep_add_pending(ep, uct_ep, req, 1);
}

ucs_status_t ucp_put_nbi(ucp_ep_h ep, const void *buffer, size_t length,
                         uint64_t remote_addr, ucp_rkey_h rkey)
{
    ucp_ep_rma_config_t *rma_config;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    ssize_t packed_len;
    ucp_request_t *req;
    uct_ep_h uct_ep;

    UCP_RMA_CHECK_PARAMS(buffer, length);
#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;
    uct_mem_h mem = NULL;

    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];
    rma_config = &config->rma[lane];
    UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
    if (length <= rma_config->max_put_short) {
        status = uct_ep_put_short(uct_ep, buffer, length,
                                  remote_addr, uct_rkey);
    } else if (length <= rma_config->max_put_bcopy) {
        ucp_memcpy_pack_context_t pack_ctx;
        pack_ctx.src    = buffer;
        pack_ctx.length = ucs_min(length, rma_config->max_put_bcopy);
        packed_len = uct_ep_put_bcopy(uct_ep,
                                      ucp_memcpy_pack, &pack_ctx,
                                      remote_addr, uct_rkey);
        status = (packed_len > 0) ? UCS_OK : (ucs_status_t)packed_len;
    } else {
        uct_pd_h uct_pd = ucp_ep_pd(ep, lane);

        status = uct_pd_mem_reg(uct_pd, (void*)buffer, length, &mem);
        if (status != UCS_OK) {
            ucs_error("failed to register user buffer: %s",
                      ucs_status_string(status));
        }
        for (;;) {
            size_t frag_length = ucs_min(length, config->max_am_zcopy);
            status = uct_ep_put_zcopy(uct_ep,
                                      buffer, frag_length,
                                      mem, remote_addr,
                                      uct_rkey, NULL);

            if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
                length -= frag_length;
                if (length == 0) {
                    /* Put is completed - return success */
                    break;
                }
                buffer += frag_length;
                remote_addr += frag_length;
            } else {
                break;
            }
        }
    }

    if (ucs_unlikely(UCS_ERR_NO_RESOURCE == status)) {
        req = ucs_mpool_get_inline(&ep->worker->req_mp);
        if (req == NULL) {
            return UCS_ERR_NO_MEMORY;
        }

        req->send.mem = mem;

        ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr, rkey,
                            ucp_progress_put_nbi);

        status = UCS_INPROGRESS;
    }
#else
    for (;;) {
        UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
        if (length <= rma_config->max_put_short) {
            /* Fast path for a single short message */
            status = uct_ep_put_short(uct_ep, buffer, length, remote_addr,
                                      uct_rkey);
            if (ucs_likely(status != UCS_ERR_NO_RESOURCE)) {
                /* Return on error or success */
                break;
            } else {
                /* Out of resources - adding request for later schedule */
                req = ucs_mpool_get_inline(&ep->worker->req_mp);
                if (req == NULL) {
                    status = UCS_ERR_NO_MEMORY;
                    break;
                }
                ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr,
                                    rkey, ucp_progress_put_nbi);
                status = UCS_INPROGRESS;
                break;
            }
        } else {
            /* Fragmented put */
            if (length <= ucp_ep_config(ep)->bcopy_thresh) {
                /* TBD: Should be replaced with bcopy */
                packed_len = ucs_min(length, rma_config->max_put_short);
                status = uct_ep_put_short(uct_ep, buffer, packed_len, remote_addr,
                                          uct_rkey);
            } else {
                /* TBD: Use z-copy */
                ucp_memcpy_pack_context_t pack_ctx;
                pack_ctx.src    = buffer;
                pack_ctx.length =
                    ucs_min(length, rma_config->max_put_bcopy);
                packed_len = uct_ep_put_bcopy(uct_ep, ucp_memcpy_pack, &pack_ctx,
                                              remote_addr, uct_rkey);
                status = (packed_len > 0) ? UCS_OK : (ucs_status_t)packed_len;
            }

            if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
                length -= packed_len;
                if (length == 0) {
                    /* Put is completed - return success */
                    break;
                }

                buffer += packed_len;
                remote_addr += packed_len;
            } else if (status == UCS_ERR_NO_RESOURCE) {
                /* Out of resources - adding request for later schedule */
                req = ucs_mpool_get_inline(&ep->worker->req_mp);
                if (req == NULL) {
                    status = UCS_ERR_NO_MEMORY;
                    break;
                }
                ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr,
                                    rkey, ucp_progress_put_nbi);
                status = UCS_INPROGRESS;
                break;
            } else {
                /* Return - Error occured */
                break;
            }
        }
    }
#endif

    return status;
}

ucs_status_t ucp_get(ucp_ep_h ep, void *buffer, size_t length,
                     uint64_t remote_addr, ucp_rkey_h rkey)
{
    ucp_ep_rma_config_t *rma_config;
    uct_completion_t comp;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    size_t frag_length;
    uct_ep_h uct_ep;

    UCP_RMA_CHECK_PARAMS(buffer, length);

    comp.count = 1;
#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;
retry:
    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];
    rma_config = &config->rma[lane];

    if (length <= rma_config->max_get_bcopy) {
        status = uct_ep_get_bcopy(uct_ep,
                                  (uct_unpack_callback_t)memcpy,
                                  (void *)buffer, length,
                                  remote_addr,
                                  uct_rkey, &comp);
        if (status == UCS_INPROGRESS) {
            ++comp.count;
        } else if (status == UCS_ERR_NO_RESOURCE) {
            ucp_worker_progress(ep->worker);
            goto retry;
        } else if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    } else {
        uct_pd_h uct_pd = ucp_ep_pd(ep, lane);
        uct_mem_h mem;

        status = uct_pd_mem_reg(uct_pd, (void*)buffer, length, &mem);
        if (status != UCS_OK) {
            ucs_error("failed to register user buffer: %s",
                      ucs_status_string(status));
        }
        for(;;) {
            frag_length = ucs_min(length, config->max_am_zcopy);
            status = uct_ep_get_zcopy(uct_ep, buffer,
                                      frag_length,
                                      mem, remote_addr,
                                      uct_rkey, &comp);

            length -= frag_length;
            ++comp.count;
            if (length == 0) {
                   /* Get is completed - return success */
                   break;
            }

            buffer += frag_length;
            remote_addr += frag_length;
        }
    }
#else
    for (;;) {
        UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);

        /* Push out all fragments, and request completion only for the last
         * fragment.
         */
        frag_length = ucs_min(rma_config->max_get_bcopy, length);
        status = uct_ep_get_bcopy(uct_ep, (uct_unpack_callback_t)memcpy,
                                  (void*)buffer, frag_length, remote_addr,
                                  uct_rkey, &comp);
        if (ucs_likely(status == UCS_OK)) {
            goto posted;
        } else if (status == UCS_INPROGRESS) {
            ++comp.count;
            goto posted;
        } else if (status == UCS_ERR_NO_RESOURCE) {
            goto retry;
        } else {
            return status;
        }

posted:
        length      -= frag_length;
        if (length == 0) {
            break;
        }

        buffer      += frag_length;
        remote_addr += frag_length;
retry:
        ucp_worker_progress(ep->worker);
    }
#endif

    /* coverity[loop_condition] */
    while (comp.count > 1) {
        ucp_worker_progress(ep->worker);
    }
    return UCS_OK;
}

static ucs_status_t ucp_progress_get_nbi(uct_pending_req_t *self)
{
    ucp_request_t *req = ucs_container_of(self, ucp_request_t, send.uct);
    ucp_rkey_h rkey    = req->send.rma.rkey;
    ucp_ep_t *ep       = req->send.ep;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    size_t frag_length;
    uct_ep_h uct_ep;

#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;

    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];

    for (;;) {
        frag_length = ucs_min(config->max_am_zcopy, req->send.length);
        status = uct_ep_get_zcopy(uct_ep, (void *)req->send.buffer,
                                  frag_length,
                                  req->send.mem, req->send.rma.remote_addr,
                                  uct_rkey, NULL);
        if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
            /* Get was initiated */
            req->send.length -= frag_length;
            req->send.buffer += frag_length;
            req->send.rma.remote_addr += frag_length;
            if (req->send.length == 0) {
                /* Get was posted */
                ucp_request_complete(req, void);
                status = UCS_OK;
                break;
            }
        } else {
            /* Error - abort */
            break;
        }
    }
#else
    ucp_ep_rma_config_t *rma_config;

    UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
    for (;;) {
        frag_length = ucs_min(rma_config->max_get_bcopy, req->send.length);
        status = uct_ep_get_bcopy(uct_ep,
                                  (uct_unpack_callback_t)memcpy,
                                  (void*)req->send.buffer,
                                  frag_length,
                                  req->send.rma.remote_addr,
                                  uct_rkey,
                                  NULL);
        if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
            /* Get was initiated */
            req->send.length -= frag_length;
            req->send.buffer += frag_length;
            req->send.rma.remote_addr += frag_length;
            if (req->send.length == 0) {
                /* Get was posted */
                ucp_request_complete(req, void);
                status = UCS_OK;
                break;
            }
        } else {
            /* Error - abort */
            break;
        }
    }
#endif

    return status;
}

ucs_status_t ucp_get_nbi(ucp_ep_h ep, void *buffer, size_t length,
                         uint64_t remote_addr, ucp_rkey_h rkey)
{
    ucp_ep_rma_config_t *rma_config;
    ucs_status_t status;
    uct_rkey_t uct_rkey;
    size_t frag_length;
    uct_ep_h uct_ep;

    UCP_RMA_CHECK_PARAMS(buffer, length);
#if _USE_ZCOPY
    ucp_ep_config_t *config;
    ucp_lane_index_t lane;

    UCP_EP_RESOLVE_RKEY(ep, rkey, rma, config, lane, uct_rkey);
    uct_ep = ep->uct_eps[lane];
    rma_config = &config->rma[lane];

    uct_completion_t comp;

    if (length < rma_config->max_get_bcopy) {
        status = uct_ep_get_bcopy(uct_ep,
                                  (uct_unpack_callback_t)memcpy,
                                  (void*)buffer,
                                  length,
                                  remote_addr,
                                  uct_rkey,
                                  NULL);
        if (ucs_unlikely(UCS_ERR_NO_RESOURCE == status)) {
            ucp_request_t *req;
            req = ucs_mpool_get_inline(&ep->worker->req_mp);
            if (req == NULL) {
                /* can't allocate memory for request - abort */
                return UCS_ERR_NO_MEMORY;
            }
            ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr,
                                rkey, ucp_progress_get_nbi);
            status = UCS_INPROGRESS;
        }
    } else {
        uct_pd_h uct_pd = ucp_ep_pd(ep, lane);
        uct_mem_h mem;

        status = uct_pd_mem_reg(uct_pd, (void*)buffer, length, &mem);
        if (status != UCS_OK) {
            ucs_error("failed to register user buffer: %s",
                      ucs_status_string(status));
            return status;
        }
        for (;;) {
            frag_length = ucs_min(config->max_am_zcopy, length);
            status = uct_ep_get_zcopy(uct_ep, buffer,
                                      frag_length,
                                      mem, remote_addr,
                                      uct_rkey, &comp);

            if (ucs_unlikely(UCS_ERR_NO_RESOURCE == status)) {
                ucp_request_t *req;
                req = ucs_mpool_get_inline(&ep->worker->req_mp);
                if (req == NULL) {
                    /* can't allocate memory for request - abort */
                    return UCS_ERR_NO_MEMORY;
                }
                req->send.mem = mem;
                ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr,
                                    rkey, ucp_progress_get_nbi);
                status = UCS_INPROGRESS;
                break;
            }
            length -= frag_length;
            ++comp.count;
            if (length == 0) {
                   /* Get is completed - return success */
                   break;
            }

            buffer += frag_length;
            remote_addr += frag_length;
        }
    }
#else
    for (;;) {
        UCP_EP_RESOLVE_RKEY_RMA(ep, rkey, uct_ep, uct_rkey, rma_config);
        frag_length = ucs_min(rma_config->max_get_bcopy, length);
        status = uct_ep_get_bcopy(uct_ep,
                                  (uct_unpack_callback_t)memcpy,
                                  (void*)buffer,
                                  frag_length,
                                  remote_addr,
                                  uct_rkey,
                                  NULL);
        if (ucs_likely(status == UCS_OK || status == UCS_INPROGRESS)) {
            /* Get was initiated */
            length -= frag_length;
            buffer += frag_length;
            remote_addr += frag_length;
            if (length == 0) {
                break;
            }
        } else if (ucs_unlikely(status == UCS_ERR_NO_RESOURCE)) {
            /* Out of resources - adding request for later schedule */
            ucp_request_t *req;
            req = ucs_mpool_get_inline(&ep->worker->req_mp);
            if (req == NULL) {
                /* can't allocate memory for request - abort */
                status = UCS_ERR_NO_MEMORY;
                break;
            }
            ucp_add_pending_rma(req, ep, uct_ep, buffer, length, remote_addr,
                                rkey, ucp_progress_get_nbi);

            /* Mark it as in progress */
            status = UCS_INPROGRESS;
            break;
        } else {
            /* Error */
            break;
        }
    }
#endif

    return status;
}

ucs_status_t ucp_worker_fence(ucp_worker_h worker)
{
    return UCS_ERR_UNSUPPORTED;
}

ucs_status_t ucp_worker_flush(ucp_worker_h worker)
{
    unsigned rsc_index;

    while (worker->stub_pend_count > 0) {
        ucp_worker_progress(worker);
    }

    /* TODO flush in parallel */
    for (rsc_index = 0; rsc_index < worker->context->num_tls; ++rsc_index) {
        if (worker->ifaces[rsc_index] == NULL) {
            continue;
        }

        while (uct_iface_flush(worker->ifaces[rsc_index]) != UCS_OK) {
            ucp_worker_progress(worker);
        }
    }

    return UCS_OK;
}

ucs_status_t ucp_ep_flush(ucp_ep_h ep)
{
    ucp_lane_index_t lane;
    ucs_status_t status;

    for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
        for (;;) {
            status = uct_ep_flush(ep->uct_eps[lane]);
            if (status == UCS_OK) {
                break;
            } else if ((status != UCS_INPROGRESS) && (status != UCS_ERR_NO_RESOURCE)) {
                return status;
            }
            ucp_worker_progress(ep->worker);
        }
    }
    return UCS_OK;
}

