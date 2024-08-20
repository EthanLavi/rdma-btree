#pragma once

#include <string>

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
    /// had to swap something out (compulsory miss)
    int conflict_misses;
    /// cold miss. Swap something in and replace nothing
    int cold_misses;
    /// priority miss (the priority of the item to be swapped out was lower/higher than the one to swap in (thus leading to normal execution))
    int priority_misses;
    /// hit in the cache
    int hits;
    /// Number of cold lines in the cache
    int empty_lines;
    /// Invalidations
    int successful_invalidations;

    CacheMetrics(){
        remote_reads = 0;
        remote_writes = 0;
        remote_cas = 0;
        allocation = 0;
        deallocation = 0;
        coherence_misses = 0;
        conflict_misses = 0;
        cold_misses = 0;
        hits = 0;
        empty_lines = 0;
        successful_invalidations = 0;
        priority_misses = 0;
    }

    std::string as_string() {
        std::string ss = "";
        ss += "<Metrics>\n";
        ss += "  <Allocations = " + std::to_string(allocation) + "/>\n";
        ss += "  <Deallocations = " + std::to_string(deallocation) + "/>\n";
        ss += "  <CoherenceMiss = " + std::to_string(coherence_misses) + "/>\n";
        ss += "  <ConflictMiss = " + std::to_string(conflict_misses) + "/>\n";
        ss += "  <ColdMiss = " + std::to_string(cold_misses) + "/>\n";
        ss += "  <PriorityMiss = " + std::to_string(priority_misses) + "/>\n";
        ss += "  <RemoteRead = " + std::to_string(remote_reads) + "/>\n";
        ss += "  <RemoteWrite = " + std::to_string(remote_writes) + "/>\n";
        ss += "  <RemoteCAS = " + std::to_string(remote_cas) + "/>\n";
        ss += "  <CacheHits = " + std::to_string(hits) + "/>\n";
        ss += "  <EmptyLines = " + std::to_string(empty_lines) + "/>\n";
        ss += "  <Invalidations = " + std::to_string(successful_invalidations) + "/>\n";
        ss += "</Metrics>\n";
        return ss;
    }
};