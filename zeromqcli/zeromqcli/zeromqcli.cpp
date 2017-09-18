// "zeromqcli.cpp": Definiert den Einstiegspunkt für die Konsolenanwendung.
//



#include "mdcliapi.hpp"

int main2(int argc, char *argv[]) {
    int verbose = (argc > 1 && strcmp(argv[1], "-v") == 0);

    mdcli session("tcp://127.0.0.1:5555", 0);

    int count;
    for (count = 0; count < 100000; count++) {
        zmsg * request = new zmsg("Hello world");
        zmsg * reply = session.send("mmi.echo", request);
        if (reply) {
            delete reply;
        } else {
            break;              //  Interrupt or failure
        }
    }
    std::cout << count << " requests/replies processed" << std::endl;
    getchar();
    return 0;
}

#include "mdwrkapi.hpp"
#include "client.h"

int pingperfTest() {
    client session("tcp://127.0.0.1:5555", "123", 1);


    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    uint32_t msgCount = 0;
    std::chrono::microseconds avgPing = 0us;
    while (1) {
        //zmsg * req = new zmsg("Hello world");
        //session.send("srv.echo", req);
        std::shared_ptr<zmsg> req = std::make_shared<zmsg>("123");
        std::chrono::high_resolution_clock::time_point pre = std::chrono::high_resolution_clock::now();
        session.send("srv.echo", req);
        std::shared_ptr<zmsg> request = session.recv();
        std::chrono::high_resolution_clock::time_point post = std::chrono::high_resolution_clock::now();

        std::chrono::microseconds ping = std::chrono::duration_cast<std::chrono::microseconds>(post-pre);

        if (avgPing == 0us) avgPing = ping;
        else avgPing = (avgPing+ping)/2;
        msgCount++;
        if (request == nullptr) {
            break;              //  Worker was interrupted
        }
        //std::cerr << "recv\n";
        //request->dump();

        if (std::chrono::system_clock::now() > start + 1s) {
            std::cerr << std::dec << avgPing.count() << "  " << msgCount << "\n";
            msgCount = 0;
            avgPing = 0us;
            start = std::chrono::system_clock::now();
        }

    }
    return 0;
}

int main(int argc, char *argv[]) {
    pingperfTest();
    int verbose = (argc > 1 && strcmp(argv[1], "-v") == 0);
    client session("tcp://127.0.0.1:5555", "123", 1);
    while (1) {
        //zmsg * req = new zmsg("Hello world");
        //session.send("srv.echo", req);
        std::shared_ptr<zmsg> req = std::make_shared<zmsg>("123");
        req->push_back("Hello World");
        session.send("rpc", req);
        std::shared_ptr<zmsg> request = session.recv();
        if (request == nullptr) {
            break;              //  Worker was interrupted
        }
        std::cerr << "recv\n";
        request->dump();

    }
    return 0;
}