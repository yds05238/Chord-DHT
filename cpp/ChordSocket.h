//
// ChordSocket.h
//
// Socket class for connecting Chord Nodes
//

#ifndef CHORD_SOCKET_H
#define CHORD_SOCKET_H

// windows
#ifdef _WIN32
#pragma comment(lib, "Ws2_32.lib")
#define WIN32_LEAN_AND_MEAN
#undef TEXT
#include <winsock2.h>
#include <ws2tcpip.h>
// unix
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <openssl/sha.h>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

/**
 * This class represents the socket protocol for the chord nodes.
 */
class ChordSocket {
private:
    /** IP of this chord node socket connection in num-period format */
    std::string _socketIp;

    /** Port of this chord node socket connection (1024 ~ 65535) */
    uint16_t _socketPort;

    /** Socket connection */
    int _chordSocket;

    /** Socket connection address */
    struct sockaddr_in _chordSocketAddr;

public:
    /** Creates new ChordSocket with localhost ip and random port */
    ChordSocket() : _socketIp("127.0.0.1") {
        // generate random port number
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<uint16_t> dist(1024, 65535);  // inclusive
        _socketPort = dist(mt);
    }
}
