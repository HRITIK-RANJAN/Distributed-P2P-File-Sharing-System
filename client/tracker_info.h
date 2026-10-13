// Parser for tracker_info.txt.
// One tracker per line:  <ip> <client_port> [<sync_port>]
// If <sync_port> is omitted it defaults to client_port + 1000.
#ifndef TRACKER_INFO_H
#define TRACKER_INFO_H

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

struct TrackerEndpoint {
    std::string ip;
    int port;      // clients connect here
    int syncPort;  // the other tracker connects here to replicate state
};

inline std::vector<TrackerEndpoint> loadTrackerInfo(const std::string &path) {
    std::vector<TrackerEndpoint> out;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ls(line);
        TrackerEndpoint t;
        if (!(ls >> t.ip >> t.port)) continue;
        if (!(ls >> t.syncPort)) t.syncPort = t.port + 1000;
        out.push_back(t);
    }
    return out;
}

#endif  // TRACKER_INFO_H
