#pragma once
#include "zmsg.hpp"
#include "mdp.h"
#include <set>                       
#include <map>
#include <list>
#include <deque>
#include <memory>
#include <chrono>
using namespace std::chrono_literals;





/*
16.09.17
dedmen [9:37 PM]
We have large traffic on start anyway because of the JIP queue.... Hm....


[9:38]
Just thinking about how to handle publicVars...
How about a `GlobalVariable` or `NetworkedVariable` class... You give it it's name in the constructor. and it automatically registeres a handler for the pubvar event


[9:39]
and if you assign a value to it it automatically get's distributed... or.. you can set the distribution scheme in the constructor..
So you don't have to worry about calling publicVariable and stuff.
you just `var = "test"` and it propagates automatically


[9:39]
and you could also configure which clients specifically to propagate to.. But that's probably overkill

[9:44]
Btw I'm also planning a compressing serializer for game_value.
For example if you send a function Arma sends the full raw string. But we would compress it first.. using zlib for example

[9:46]
Ouh gosh....
CODE variables are compiled when you receive them.. Networking lives in a seperate thread and we will need a invoker_lock to compile them... That will be crusty


[9:49]
And when we deserialize we actually don't know what we are even deserializing... So I need a special flag to tell the receiver if the deserialization needs a invoker_lock
 */

#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  2500ms    //  msecs
#define HEARTBEAT_EXPIRY    HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
class service;
class client {
public:
    std::string m_identity;   //  Address of worker
    std::shared_ptr<service>  m_service;      //  Owning service, if known
    std::chrono::system_clock::time_point m_expiry;         //  Expires at unless heartbeat
    std::string cid;

    client(std::string identity, std::shared_ptr<service>  service = 0, std::chrono::system_clock::time_point expiry = {}) {
        m_identity = identity;
        m_service = service;
        m_expiry = expiry;
    }
};

class service {
public:
    service(std::string name) : m_name(name) {}

    virtual ~service() {
        m_requests.clear();
    }

    std::string m_name;             //  Service name
    std::deque<std::shared_ptr<zmsg>> m_requests;   //  List of client requests
    std::set<std::shared_ptr<client>> m_waiting;  //  List of waiting workers

    virtual void dispatch(std::shared_ptr<zmsg> msg);
};

class rpc_broker : public service {
public:
    rpc_broker(std::string name) : service(name) {}
    static bool match_target(const std::shared_ptr<client>& cli, const std::string& cs);
    void dispatch(std::shared_ptr<zmsg> msg) override;
};


class router {
public:
    router();
    virtual ~router();


    void
        bind(std::string endpoint) {
        m_endpoint = endpoint;
        m_socket->bind(m_endpoint.c_str());
        s_console("I: MDP broker/0.1.1 is active at %s", endpoint.c_str());
    }


public:

    //  ---------------------------------------------------------------------
    //  Delete any idle workers that haven't pinged us in a while.

    void
        purge_workers() {
        std::vector<std::shared_ptr<client>> toCull;
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        for (auto& worker : m_clients) {
            if (worker.second->m_expiry <= now)
                toCull.push_back(worker.second);
        }
        for (auto& worker : toCull) {
            if (m_verbose) {
                s_console("I: deleting expired worker: %s",
                    worker->m_identity.c_str());
            }
            worker_delete(worker, 0);
        }
    }

    //  ---------------------------------------------------------------------
    //  Locate or create new service entry

    std::shared_ptr<service>
        service_require(std::string name) {
        assert(name.size() > 0);
        if (m_services.count(name)) {
            return m_services.at(name);
        } else {
            std::shared_ptr<service> srv = std::make_shared<service>(name);
            m_services.insert(std::make_pair(name, srv));
            if (m_verbose) {
                s_console("I: received message:");
            }
            return srv;
        }
    }



    //  ---------------------------------------------------------------------
    //  Dispatch requests to waiting workers as possible

    void service_dispatch(std::shared_ptr<service> srv, std::shared_ptr<zmsg> msg) {
        assert(srv);
        if (msg) {                    //  Queue message if any
            srv->m_requests.push_back(msg);
        }

        purge_workers();
        while (!srv->m_waiting.empty() && !srv->m_requests.empty()) {
            // Choose the most recently seen idle worker; others might be about to expire
            std::set<std::shared_ptr<client>>::iterator wrk = srv->m_waiting.begin();
            std::set<std::shared_ptr<client>>::iterator next = wrk;
            for (++next; next != srv->m_waiting.end(); ++next) {
                if ((*next)->m_expiry > (*wrk)->m_expiry)
                    wrk = next;
            }

            std::shared_ptr<zmsg> msg = srv->m_requests.front();
            srv->m_requests.pop_front();
            worker_send(*wrk, (char*) MDPW_REQUEST, "", msg);
            srv->m_waiting.erase(wrk);
        }
    }

    //  ---------------------------------------------------------------------
    //  Handle internal service according to 8/MMI specification

    void service_internal(std::string service_name, std::shared_ptr<zmsg> msg) {
        //std::cerr << "service\n";
        //msg->dump();
        if (service_name.compare("srv.service") == 0) {
            std::shared_ptr<service>  srv = m_services.at(msg->body());
            if (srv && srv->m_waiting.empty()) {
                msg->body_set("200");
            } else {
                msg->body_set("404");
            }
        } else if (service_name.compare("srv.echo") == 0) {
        } else {
            msg->body_set("501");
        }

        //  Remove & save client return envelope and insert the
        //  protocol header and service name, then rewrap envelope.
        std::string client = msg->unwrap();
        msg->wrap(MDPW_REQUEST, service_name.c_str());
        msg->push_front("");
        msg->wrap(client.c_str(), "");
        //std::cerr << "serviceend\n";
        //msg->dump();
        msg->send(*m_socket);
    }

    //  ---------------------------------------------------------------------
    //  Creates worker if necessary

    std::shared_ptr<client>  worker_require(std::string identity) {
        assert(identity.length() != 0);

        //  self->workers is keyed off worker identity
        if (m_clients.count(identity)) {
            return m_clients.at(identity);
        } else {
            std::shared_ptr<client> wrk = std::make_shared<client>(identity);
            m_clients.insert(std::make_pair(identity, wrk));
            if (m_verbose) {
                s_console("I: registering new worker: %s", identity.c_str());
            }
            return wrk;
        }
    }

    //  ---------------------------------------------------------------------
    //  Deletes worker from all data structures, and destroys worker

    void worker_delete(std::shared_ptr<client> &wrk, int disconnect) {
        assert(wrk);
        if (disconnect) {
            worker_send(wrk, (char*) MDPW_DISCONNECT, "", NULL);
        }

        if (wrk->m_service) {
            wrk->m_service->m_waiting.erase(wrk);
        }
        //  This implicitly calls the worker destructor
        m_clients.erase(wrk->m_identity);
    }



    //  ---------------------------------------------------------------------
    //  Process message sent to us by a worker

    void worker_process(std::string sender, std::shared_ptr<zmsg> msg) {
        assert(msg && msg->parts() >= 1);     //  At least, command

        std::string command = (char *) msg->pop_front().c_str();
        bool worker_ready = m_clients.count(sender) > 0;
        std::shared_ptr<client> wrk = worker_require(sender);

        if (command.compare(MDPW_READY) == 0) {
            if (worker_ready) {              //  Not first command in session
                worker_delete(wrk, 1);
            } else {
                if (sender.size() >= 4  //  Reserved service name
                    && sender.find_first_of("srv.") == 0) {
                    worker_delete(wrk, 1);
                } else {
                    //  Attach worker to service and mark as idle
                    std::string service_name = (char*) msg->pop_front().c_str();
                    std::string clientID = (char*) msg->pop_front().c_str();
                    wrk->m_service = service_require(service_name);
                    wrk->cid = clientID;
                    worker_waiting(wrk);
                }
            }
        } else {
            if (command.compare(MDPW_REPLY) == 0) {
                if (worker_ready) {
                    //  Remove & save client return envelope and insert the
                    //  protocol header and service name, then rewrap envelope.
                    std::string client = msg->unwrap();
                    msg->wrap(MDPC_CLIENT, wrk->m_service->m_name.c_str());
                    msg->wrap(client.c_str(), "");
                    msg->send(*m_socket);
                    worker_waiting(wrk);
                } else {
                    worker_delete(wrk, 1);
                }
            } else {
                if (command.compare(MDPW_HEARTBEAT) == 0) {
                    std::cerr << "pong\n";
                    if (worker_ready) {
                        wrk->m_expiry = std::chrono::system_clock::now() + HEARTBEAT_EXPIRY;
                    } else {
                        worker_delete(wrk, 1);
                    }
                } else {
                    if (command.compare(MDPW_DISCONNECT) == 0) {
                        worker_delete(wrk, 0);
                    } else {
                        s_console("E: invalid input message (%d)", (int) *command.c_str());
                        msg->dump();
                    }
                }
            }
        }
    }

    //  ---------------------------------------------------------------------
    //  Send message to worker
    //  If pointer to message is provided, sends that message

    void worker_send(std::shared_ptr<client> worker, char *command, std::string option, std::shared_ptr<zmsg> msg) {
        msg = (msg ? std::make_shared<zmsg>(*msg) : std::make_shared<zmsg>());

        //  Stack protocol envelope to start of message
        if (option.size() > 0) {                 //  Optional frame after command
            msg->push_front((char*) option.c_str());
        }
        msg->push_front(command);
        msg->push_front((char*) MDPW_WORKER);
        //  Stack routing envelope to start of message
        msg->wrap(worker->m_identity.c_str(), "");

        if (m_verbose) {
            s_console("I: sending %s to worker",
                mdps_commands[(int) *command]);
            msg->dump();
        }
        msg->send(*m_socket);
    }

    //  ---------------------------------------------------------------------
    //  This worker is now waiting for work

    void worker_waiting(std::shared_ptr<client> worker) {
        assert(worker);
        //  Queue to broker and service waiting lists
        worker->m_service->m_waiting.insert(worker);
        worker->m_expiry = std::chrono::system_clock::now() + HEARTBEAT_EXPIRY;
        // Attempt to process outstanding requests
        service_dispatch(worker->m_service, 0);
    }



    //  ---------------------------------------------------------------------
    //  Process a request coming from a client

    void client_process(std::string sender, std::shared_ptr<zmsg> msg) {
        assert(msg && msg->parts() >= 2);     //  Service name + body

        std::string service_name = (char *) msg->pop_front().c_str();
        std::shared_ptr<service> srv = service_require(service_name);
        //  Set reply return address to client sender
        msg->wrap(sender.c_str(), "");
        if (service_name.length() >= 4
            && service_name.find_first_of("srv.") == 0) {
            service_internal(service_name, msg);
        } else {
            if (srv)
                srv->dispatch(msg);
            service_dispatch(srv, msg);
        }
    }

public:

    //  Get and process messages forever or until interrupted
    void route() {
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::chrono::system_clock::time_point heartbeat_at = now + HEARTBEAT_INTERVAL;
        while (!s_interrupted) {
            zmq::pollitem_t items[] = {
                { *m_socket,  0, ZMQ_POLLIN, 0 } };
            std::chrono::milliseconds timeout = std::chrono::duration_cast<std::chrono::milliseconds>(heartbeat_at - now);
            if (timeout < 0ms)
                timeout = 0ms;
            zmq::poll(items, 1, timeout);

            //  Process next input message, if any
            if (items[0].revents & ZMQ_POLLIN) {
                std::shared_ptr<zmsg> msg = std::make_shared<zmsg>(*m_socket);
                if (m_verbose) {
                    //s_console("I: received message:");
                    //msg->dump();
                }
                std::string sender = std::string((char*) msg->pop_front().c_str());
                msg->pop_front(); //empty message
                std::string header = std::string((char*) msg->pop_front().c_str());

                //              std::cout << "sbrok, sender: "<< sender << std::endl;
                //              std::cout << "sbrok, header: "<< header << std::endl;
                //              std::cout << "msg size: " << msg->parts() << std::endl;
                //              msg->dump();
                if (header.compare(MDPC_CLIENT) == 0) {
                    client_process(sender, msg);
                } else if (header.compare(MDPW_WORKER) == 0) {
                    worker_process(sender, msg);
                } else {
                    s_console("E: invalid message:");
                    msg->dump();
                }
            }
            //  Disconnect and delete any expired workers
            //  Send heartbeats to idle workers if needed
            now = std::chrono::system_clock::now();
            if (now >= heartbeat_at) {
                purge_workers();
                m_verbose = false;
                for (auto& worker : m_clients) {
                    std::cerr << "ping\n";
                    worker_send(worker.second, (char*) MDPW_HEARTBEAT, "", NULL);
                }
                m_verbose = true; 
                heartbeat_at += HEARTBEAT_INTERVAL;
                now = std::chrono::system_clock::now();
            }
        }
    }



    zmq::context_t * m_context;                  //  0MQ context
    zmq::socket_t * m_socket;                    //  Socket for clients & workers
    int m_verbose;                               //  Print activity to stdout
    std::string m_endpoint;                      //  Broker binds to this endpoint
    std::map<std::string, std::shared_ptr<service>> m_services;  //  Hash of known services
    std::map<std::string, std::shared_ptr<client>> m_clients;    //  Hash of known workers
};

static router GRouter;