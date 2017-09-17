#include "stdafx.h"
#include "router.h"

void service::dispatch(std::shared_ptr<zmsg> msg) {
    if (msg) { //  Queue message if any
        m_requests.push_back(msg);
    }

    GRouter.purge_workers();
    while (!m_waiting.empty() && !m_requests.empty()) {
        // Choose the most recently seen idle worker; others might be about to expire
        std::set<std::shared_ptr<client>>::iterator wrk = m_waiting.begin();
        std::set<std::shared_ptr<client>>::iterator next = wrk;
        for (++next; next != m_waiting.end(); ++next) {
            if ((*next)->m_expiry > (*wrk)->m_expiry)
                wrk = next;
        }

        std::shared_ptr<zmsg> msg = m_requests.front();
        m_requests.pop_front();
        GRouter.worker_send(*wrk, (char*) MDPW_REQUEST, "", msg);
        m_waiting.erase(wrk);
    }
}

bool rpc_broker::match_target(const std::shared_ptr<client>& cli, const std::string& cs) {
    return cs == cli->cid;
}

void rpc_broker::dispatch(std::shared_ptr<zmsg> msg) {
    if (msg) { //  Queue message if any
        m_requests.push_back(msg);
    }
    msg->pop_front();
    msg->pop_front();
    std::string target = (char*) msg->pop_front().c_str();
    GRouter.purge_workers();
    while (!m_waiting.empty() && !m_requests.empty()) {
        // Choose the most recently seen idle worker; others might be about to expire
        for (auto& cli : m_waiting) {
            if (match_target(cli,target)) {
                std::shared_ptr<zmsg> msg = m_requests.front();
                m_requests.pop_front();
                GRouter.worker_send(cli, (char*) MDPW_REQUEST, "", msg);
            }
        }
    }
}

router::router() {

    m_context = new zmq::context_t(1);
    m_socket = new zmq::socket_t(*m_context, ZMQ_ROUTER);
    m_verbose = 1;
    m_services.insert({ "rpc",std::make_shared<rpc_broker>("rpc") });

}


router::~router() {
    m_services.clear();
    m_clients.clear();
}
