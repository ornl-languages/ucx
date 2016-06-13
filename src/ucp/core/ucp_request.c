/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "ucp_ep.inl"
#include "ucp_request.h"
#include "ucp_context.h"
#include "ucp_worker.h"
#include "ucp_mm.h"

#include "uct/api/uct.h"
#include <ucp/tag/match.h>
#include <ucs/datastruct/mpool.inl>
#include <ucs/debug/log.h>
#include <ucs/arch/bitops.h>
#include <ucs/type/status.h>


int ucp_request_is_completed(void *request)
{
    ucp_request_t *req = (ucp_request_t*)request - 1;
    return !!(req->flags & UCP_REQUEST_FLAG_COMPLETED);
}

void ucp_request_wait(void *request)
{
    ucp_request_t *req = (ucp_request_t*)request;

    while(!(req->flags & UCP_REQUEST_FLAG_COMPLETED)) {
        ucp_worker_progress(req->send.ep->worker);
        req->flags |= UCP_REQUEST_FLAG_COMPLETED;
    }
    while(1 < req->send.uct_comp.count) {
        ucp_ep_flush(req->send.ep);
    }

    if (NULL != req->send.state.dt.contig.memh) {
        ucp_lane_index_t lane;
        ucp_ep_config_t *config;
        ucp_pd_lane_map_t ep_lane_map, rkey_pd_map;
        uint8_t bit_index;

        config     = ucp_ep_config(req->send.ep);
        ep_lane_map = config->key.rma_lane_map;
        rkey_pd_map = ucp_ep_pd_map_expand((req->send.rma.rkey)->pd_map);
        bit_index    = ucs_ffs64(ep_lane_map & rkey_pd_map);
        lane         = bit_index / UCP_PD_INDEX_BITS;
        uct_pd_h uct_pd = ucp_ep_pd(req->send.ep, lane);
        uct_pd_mem_dereg(uct_pd, req->send.state.dt.contig.memh);
    }

    ucs_mpool_put_inline(req);
}

void ucp_request_release(void *request)
{
    ucp_request_t *req = (ucp_request_t*)request - 1;

    ucs_trace_data("release request %p (%p) flags: 0x%x", req, req + 1, req->flags);

    if ((req->flags |= UCP_REQUEST_FLAG_RELEASED) & UCP_REQUEST_FLAG_COMPLETED) {
        ucs_trace_data("put %p to mpool", req);
        ucs_mpool_put_inline(req);
    }
}

void ucp_request_cancel(ucp_worker_h worker, void *request)
{
    ucp_request_t *req = (ucp_request_t*)request - 1;

    if (req->flags & UCP_REQUEST_FLAG_COMPLETED) {
        return;
    }

    if (req->flags & UCP_REQUEST_FLAG_EXPECTED) {
        ucp_tag_cancel_expected(worker->context, req);
        ucp_request_complete(req, req->cb.tag_recv, UCS_ERR_CANCELED, NULL);
    }
}

static void ucp_worker_request_init_proxy(ucs_mpool_t *mp, void *obj, void *chunk)
{
    ucp_worker_h worker = ucs_container_of(mp, ucp_worker_t, req_mp);
    ucp_context_h context = worker->context;
    ucp_request_t *req = obj;

    if (context->config.request.init != NULL) {
        context->config.request.init(req + 1);
    }
}

static void ucp_worker_request_fini_proxy(ucs_mpool_t *mp, void *obj)
{
    ucp_worker_h worker = ucs_container_of(mp, ucp_worker_t, req_mp);
    ucp_context_h context = worker->context;
    ucp_request_t *req = obj;

    if (context->config.request.cleanup != NULL) {
        context->config.request.cleanup(req + 1);
    }
}

ucs_mpool_ops_t ucp_request_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucp_worker_request_init_proxy,
    .obj_cleanup   = ucp_worker_request_fini_proxy
};

