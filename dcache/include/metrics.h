#pragma once

#include <ostream>

/// A structure to capture the behavior of a cache
struct CacheMetrics {
    /// made a remote read (todo: might also imply allocation?)
    int remote_reads;
    /// made a remote write (todo: might also imply allocation?)
    int remote_writes;
    /// made a remote cas (todo: might also imply allocation?)
    int remote_cas;
    /// Memory management
    int allocation;
    int deallocation;
    /// something was found in the cache but was invalid
    int coherence_misses;
    /// had to swap something out (or was cold/compulsory miss)
    int conflict_misses;
    /// hit in the cache
    int hits;

    CacheMetrics(){
        remote_reads = 0;
        remote_writes = 0;
        remote_cas = 0;
        allocation = 0;
        deallocation = 0;
        coherence_misses = 0;
        conflict_misses = 0;
        hits = 0;
    }

    friend std::ostream& operator<<(std::ostream &os, const CacheMetrics &p) {
        return os << "<Metrics>" << std::endl
                << "  <Allocations = " << p.allocation << "/>" << std::endl
                << "  <Deallocations = " << p.deallocation << "/>" << std::endl
                << "  <CoherenceMiss = " << p.coherence_misses << "/>" << std::endl
                << "  <ConflictMiss = " << p.conflict_misses << "/>" << std::endl
                << "  <RemoteRead = " << p.remote_reads << "/>" << std::endl
                << "  <RemoteWrite = " << p.remote_writes << "/>" << std::endl
                << "  <RemoteCAS = " << p.remote_cas << "/>" << std::endl
                << "  <CacheHits = " << p.hits << "/>" << std::endl
                << "</Metrics>" << std::endl;
    }
};