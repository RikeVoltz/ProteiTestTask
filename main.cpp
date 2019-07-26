212#include "QueuesSimplifier.h"

using namespace std;

int main(int argc, char **argv) {
    try {
        QueuesSimplifier *qs;
        if (argc > 2)
            qs = new QueuesSimplifier(argv[1], argv[2]);
        else
            qs = new QueuesSimplifier();
        qs->parse();
        delete qs;
    }
    catch (exception &e) {
        cerr << e.what();
        return 0;
    }
    return 0;
}
