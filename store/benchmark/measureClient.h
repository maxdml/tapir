#ifndef _BENCHMARK_MEASURE_CLIENT_H_
#define _BENCHMARK_MEASURE_CLIENT_H_
#include <iostream>
#include <sstream> 

struct OpStat {
    struct timeval tstart;
    struct timeval tend;
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


template <class ClientT>
class MeasureClient : public ::Client {
public:
    template <class... Types>
    MeasureClient(int txn_len, Types&& ... args) : base_client(args...), txn_len(txn_len) {}

    ~MeasureClient() {
        summarize(std::cerr);
        output_stats(std::cerr);
    }

    ClientT base_client;

    void set_txn_len(int txn_len) {
        this->txn_len = txn_len;
    }

    void Begin() {
        curr_stat = &stats.emplace_back();
        curr_stat->getStats.reserve(txn_len);
        curr_stat->putStats.reserve(txn_len);

        OpStat &op = curr_stat->begin;
        gettimeofday(&op.tstart, NULL);
        base_client.Begin();
        gettimeofday(&op.tend, NULL);
        store_latency(op);
        begin_count++;
        begin_latency += op.latency;
    }

    int Get(const std::string &key, std::string &value) {
        OpStat &op = curr_stat->getStats.emplace_back();
        gettimeofday(&op.tstart, NULL);
        int rtn = base_client.Get(key, value);
        gettimeofday(&op.tend, NULL);
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
        gettimeofday(&op.tstart, NULL);
        int rtn = base_client.Put(key, value);
        gettimeofday(&op.tend, NULL);
        store_latency(op);
        put_latency += op.latency;
        put_count++;
        return rtn;
    }

    bool Commit() {
        OpStat &op = curr_stat->commit;
        gettimeofday(&op.tstart, NULL);
        bool status = base_client.Commit();
        gettimeofday(&op.tend, NULL);
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
        base_client.Abort();
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
                << stat.begin.tstart.tv_sec << "." << stat.begin.tstart.tv_usec << " " \
                << stat.commit.tend.tv_sec << "." << stat.commit.tend.tv_usec << " " \
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

    void store_latency(OpStat &stat) {
        stat.latency = calc_duration(stat.tstart, stat.tend);
    }

    double calc_duration(struct timeval &start, struct timeval &end) {
        return (end.tv_sec - start.tv_sec) * 1000000 + \
               (end.tv_usec - start.tv_usec);
    }

    void output_stat(std::ostream &out, OpStat &stat, bool status, 
                     const std::string &label, int idx) {
        out << idx << " " << label << " " \
            << stat.tstart.tv_sec << "." << stat.tstart.tv_usec << " " \
            << stat.tend.tv_sec << "." << stat.tend.tv_usec << " " \
            << stat.latency << " " << status << std::endl << std::flush;
    }
};

#endif
