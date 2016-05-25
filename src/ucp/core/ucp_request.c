/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "ucp_request.h"
#include "ucp_context.h"
#include "ucp_worker.h"

#include <ucp/tag/match.h>
#include <ucs/datastruct/mpool.inl>
#include <ucs/debug/log.h>
#include <ucs/type/status.h>


int ucp_request_is_completed(void *request)
{
    ucp_request_t *req = (ucp_request_t*)request - 1;
    return !!(req->flags & UCP_REQUEST_FLAG_COMPLETED);
}

void ucp_request_wait(void *request)
{
//    ucs_status_t status;
    ucp_request_t *req = (ucp_request_t*)request - 1;

//    printf("ucp_request_wait\n");fflush(NULL);
#if 0
    ucp_worker_progress(req->send.ep->worker);
//    ucp_worker_flush(req->send.ep->worker);
#else
    while(!ucp_request_is_completed(request)) {
//        printf("worker_progress\n");fflush(NULL);
        ucp_worker_progress(req->send.ep->worker);
        req->flags |= UCP_REQUEST_FLAG_COMPLETED;
    }
    while(1 < req->send.uct_comp.count) {
//        int debug = 1;
//        while (debug) {};
//        printf("Waiting\n");fflush(NULL);
        if ( NULL != req->send.ep) {
//            printf("Wait flush\n");fflush(NULL);
//            ucp_worker_progress(req->send.ep->worker);
//            ucp_worker_flush(req->send.ep->worker);

//            uct_ep_flush(req->send.ep->uct_eps[UCP_EP_OP_RMA]);
            ucp_ep_flush(req->send.ep);
            //ucs_status_t status = uct_ep_flush(req->send.ep->uct_eps[UCP_EP_OP_RMA]);
//            printf("%s; comp count = %d\n", ucs_status_string(status),req->send.uct_comp.count);
//            printf("comp count = %d\n", req->send.uct_comp.count);
        } else {
            printf("Ups, no ep\n"); fflush(NULL);
            //break;
        }
    }
#endif
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

