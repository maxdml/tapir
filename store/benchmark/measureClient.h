#ifndef _BENCHMARK_MEASURE_CLIENT_H_
#define _BENCHMARK_MEASURE_CLIENT_H_
#include <iostream>
#include <sstream> 
#include <sys/times.h>
#include <iomanip>

struct OpStat {
    struct timespec tstart;
    struct timespec tend;
    long latency;
};

struct TxnStat {
    std::vector<struct OpStat> getStats;
    std::vector<struct OpStat> putStats;
    std::vector<struct OpStat> prepStats;
    struct OpStat begin;
    struct OpStat commit;
    long commit_retries;
    double total_latency; // latency for the entire transaction
    bool status;
};

#define GETTIME_CLOCK CLOCK_THREAD_CPUTIME_ID


template <class ClientT>
class MeasureClient : public ::Client {
public:
    template <class... Types>
    MeasureClient(int txn_len, Types&& ... args) : txn_len(txn_len) {
        base_client = new ClientT(args...);
        stats.reserve(30000);
    }

    ~MeasureClient() {
        summarize(std::cerr);
        output_stats(std::cerr);
    }

    ClientT *base_client;

    void set_txn_len(int txn_len) {
        this->txn_len = txn_len;
    }

    void Begin() {
        curr_stat = &stats.emplace_back();

        OpStat &op = curr_stat->begin;
        set_timespec(op.tstart);
        curr_stat->getStats.reserve(txn_len);
        curr_stat->putStats.reserve(txn_len);
        base_client->Begin();
        set_timespec(op.tend);
        store_latency(op);
        begin_count++;
        begin_latency += op.latency;
    }

    int Get(const std::string &key, std::string &value) {
        OpStat &op = curr_stat->getStats.emplace_back();
        set_timespec(op.tstart);
        int rtn = base_client->Get(key, value);
        set_timespec(op.tend);
        store_latency(op);
        get_latency += op.latency;
        get_count++;
        return rtn;
    }
    std::string Get(const std::string &key) {
        std::string value;
        Get(key, value);
        return value;
    }

    int Put(const std::string &key, const std::string &value) {
        OpStat &op = curr_stat->putStats.emplace_back();
        set_timespec(op.tstart);
        int rtn = base_client->Put(key, value);
        set_timespec(op.tend);
        store_latency(op);
        put_latency += op.latency;
        put_count++;
        return rtn;
    }

    bool Commit() {
        OpStat &op = curr_stat->commit;
        set_timespec(op.tstart);
        bool status = base_client->Commit();
        set_timespec(op.tend);
        store_latency(op);
        curr_stat->status = status;

        commit_latency += op.latency;
        curr_stat->total_latency = calc_duration(curr_stat->begin.tstart,
                                                curr_stat->commit.tend);
        if (status) {
            n_success++;
            total_latency += curr_stat->total_latency;
        }

        commit_count++;
        return status;
    }
    void Abort() {
        base_client->Abort();
    }

    void summarize(std::ostream &out) {
        out << std::unitbuf;
        out << "# Commit_Ratio: " << (double)n_success / stats.size() \
            << std::endl \
            << "# Average_Latency: " << total_latency / n_success \
            << std::endl  \
            << "# Begin: " << begin_count << ", " << begin_latency / begin_count \
            << std::endl \
            << "# Get: " << get_count << ", " << get_latency / get_count \
            << std::endl \
            << "# Put: " << put_count << ", " << put_latency / put_count \
            << std::endl \
            << "# Commit: " << commit_count << ", " << commit_latency / commit_count \
            << std::endl << std::flush;

        replication::ir::IRClient *irclient =
                ((tapirstore::ShardClient *) ((tapirstore::Client *) base_client)->bclient[0]->txnclient)->client;
        fprintf(stderr, "# Fast path: %d\n", irclient->fast_path_taken);
        fprintf(stderr, "# Slow path: %d\n", irclient->slow_path_taken);
        fprintf(stderr, "# Get timeouts: %d\n", irclient->unlogged_timeouts);
        fprintf(stderr, "# Prepare timeouts: %d\n", irclient->consensus_timeouts);
        fprintf(stderr, "# PrepareFinalize timeouts: %d\n", irclient->finalize_consensus_timeouts);
        fprintf(stderr, "# Commit timeouts: %d\n", irclient->inconsistent_timeouts);
        fprintf(stderr, "# CommitFinalize timeouts: %d\n", irclient->finalize_inconsistent_timeouts);



    }

    void output_stats(std::ostream &out) {
        for (unsigned int i = 0; i < stats.size(); ++i) {
            TxnStat &stat = stats[i];
            output_stat(out, stat.begin, stat.status, "begin", i+1);
            for (unsigned int j = 0; j < stat.putStats.size(); ++j) {
                std::ostringstream ss;
                ss << "put_" << j;
                output_stat(out, stat.putStats[j], stat.status, ss.str(), i+1);
            }
            for (unsigned int j = 0; j < stat.getStats.size(); ++j) {
                std::ostringstream ss;
                ss << "get_" << j;
                output_stat(out, stat.getStats[j], stat.status, ss.str(), i+1);
            }
            output_stat(out, stat.commit, stat.status, "commit", i+1);

            out << i + 1 << " total " \
                << stat.begin.tstart.tv_sec << "." << stat.begin.tstart.tv_nsec << " " \
                << stat.commit.tend.tv_sec << "." << stat.commit.tend.tv_nsec << " " \
                << stat.total_latency << " " << stat.status << std::endl << std::flush;
        }
    }

    std::vector<int> Stats() {
        std::vector<int> x{};
        return x;
    }


private:
    int txn_len;
    int n_success;
    int begin_count;
    int get_count;
    int put_count;
    int commit_count;

    double begin_latency;
    double get_latency;
    double put_latency;
    double commit_latency;
    double total_latency;

    std::vector<TxnStat> stats;
    TxnStat *curr_stat;

    constexpr void store_latency(OpStat &stat) {
        stat.latency = calc_duration(stat.tstart, stat.tend);
    }

    constexpr double calc_duration(struct timespec &start, struct timespec &end) {
        return (long int)(end.tv_sec - start.tv_sec) * 1000000000 + \
               (end.tv_nsec - start.tv_nsec);
    }

    void output_stat(std::ostream &out, OpStat &stat, bool status, 
                     const std::string &label, int idx) {
        out << idx << " " << label << " " \
            << stat.tstart.tv_sec << "." << std::setfill('0') << std::setw(9) << stat.tstart.tv_nsec << " " \
            << stat.tend.tv_sec << "." << std::setfill('0') << std::setw(9) << stat.tend.tv_nsec << " " \
            << stat.latency << " " << status << std::endl << std::flush;
    }
    void set_timespec(struct timespec &spec) {
        clock_gettime(CLOCK_MONOTONIC_RAW, &spec);
    }
}

;

#endif
