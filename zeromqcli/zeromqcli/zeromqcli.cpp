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

int main(int argc, char *argv[]) {
    int verbose = (argc > 1 && strcmp(argv[1], "-v") == 0);
    mdwrk session("tcp://127.0.0.1:5555", "rpc", 1);

    while (1) {
        //zmsg * req = new zmsg("Hello world");
        //session.send("srv.echo", req);
        zmsg * req = new zmsg("123");
        req->push_back("Hello World");
        session.send("rpc", req);
        zmsg *request = session.recv();
        if (request == 0) {
            break;              //  Worker was interrupted
        }
        std::cerr << "recv\n";
        request->dump();
    }
    return 0;
}