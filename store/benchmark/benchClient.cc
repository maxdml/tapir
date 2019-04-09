// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/benchClient.cc:
 *   Benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/strongstore/client.h"
#include "store/weakstore/client.h"
#include "store/tapirstore/client.h"

#include <iostream>


using namespace std;

struct txn_stats {
    struct timeval tstart;
    struct timeval tend;
    long latency;
};

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double alpha = -1;
double *zipf;

vector<string> keys;
int nKeys = 100;

int
main(int argc, char **argv)
{
    const char *configPath = NULL;
    const char *keysPath = NULL;
    int duration = 10;
    int nShards = 1;
    int tLen = 10;
    int wPer = 50; // Out of 100
    int closestReplica = -1; // Closest replica id.
    int skew = 0; // difference between real clock and TrueTime
    int error = 0; // error bars

    Client *client;
    enum {
        MODE_UNKNOWN,
        MODE_TAPIR,
        MODE_WEAK,
        MODE_STRONG
    } mode = MODE_UNKNOWN;

    // Mode for strongstore.
    strongstore::Mode strongmode;

    int opt;
    while ((opt = getopt(argc, argv, "c:d:N:l:w:k:f:m:e:s:z:r:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'f': // Generated keys path
        {
            keysPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'd': // Duration in seconds to run.
        {
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (duration <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'l': // Length of each transaction (deterministic!)
        {
            char *strtolPtr;
            tLen = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (tLen <= 0)) {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;
        }

        case 'w': // Percentage of writes (out of 100)
        {
            char *strtolPtr;
            wPer = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (wPer < 0 || wPer > 100)) {
                fprintf(stderr, "option -w requires a arg b/w 0-100\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }

        case 's': // Simulated clock skew.
        {
            char *strtolPtr;
            skew = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (skew < 0))
            {
                fprintf(stderr,
                        "option -s requires a numeric arg\n");
            }
            break;
        }

        case 'e': // Simulated clock error.
        {
            char *strtolPtr;
            error = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (error < 0))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'z': // Zipf coefficient for key selection.
        {
            char *strtolPtr;
            alpha = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
            }
            break;
        }

        case 'r': // Preferred closest replica.
        {
            char *strtolPtr;
            closestReplica = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -r requires a numeric arg\n");
            }
            break;
        }

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "txn-l") == 0) {
                mode = MODE_TAPIR;
            } else if (strcasecmp(optarg, "txn-s") == 0) {
                mode = MODE_TAPIR;
            } else if (strcasecmp(optarg, "qw") == 0) {
                mode = MODE_WEAK;
            } else if (strcasecmp(optarg, "occ") == 0) {
                mode = MODE_STRONG;
                strongmode = strongstore::MODE_OCC;
            } else if (strcasecmp(optarg, "lock") == 0) {
                mode = MODE_STRONG;
                strongmode = strongstore::MODE_LOCK;
            } else if (strcasecmp(optarg, "span-occ") == 0) {
                mode = MODE_STRONG;
                strongmode = strongstore::MODE_SPAN_OCC;
            } else if (strcasecmp(optarg, "span-lock") == 0) {
                mode = MODE_STRONG;
                strongmode = strongstore::MODE_SPAN_LOCK;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                exit(0);
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (mode == MODE_TAPIR) {
        client = new tapirstore::Client(configPath, nShards,
                    closestReplica, TrueTime(skew, error));
    } else if (mode == MODE_WEAK) {
        client = new weakstore::Client(configPath, nShards,
                    closestReplica);
    } else if (mode == MODE_STRONG) {
        client = new strongstore::Client(strongmode, configPath,
                    nShards, closestReplica, TrueTime(skew, error));
    } else {
        fprintf(stderr, "option -m is required\n");
        exit(0);
    }

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    /**TODO:
     * - get a vector of begin and commit latencies
     * - add the put latencies
     * - remove obsolete variables
     */
    cerr << "Running benchClient for " << duration << "s, tLen=" << tLen << ", wPer=" << wPer << endl;

    struct timeval t0, t1;
    struct timeval tbeginstart, tbeginend;
    struct timeval tputstart, tputend;
    struct timeval tcommitstart, tcommitend;

    int nTransactions = 0;
    int tCount = 0;
    double tLatency = 0.0;
    double getLatency = 0.0;
    double putLatency = 0.0;
    int beginCount = 0;
    double beginLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;
    double begin_latency = 0;
    vector<struct txn_stats> get_stats(tLen);
    double commit_latency = 0;
    long latency = 0;
    int getCount, putCount;
    int totalGetCount = 0;
    int totalPutCount = 0;

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    while (1) {
        getCount = 0;
        putCount = 0;
        gettimeofday(&tbeginstart, NULL);
        client->Begin();
        gettimeofday(&tbeginend, NULL);

        beginCount++;
        begin_latency = ((tbeginend.tv_sec - tbeginstart.tv_sec)*1000000 + (tbeginend.tv_usec - tbeginstart.tv_usec));
        beginLatency += begin_latency;

        for (int j = 0; j < tLen; j++) {
            key = keys[rand_key()];

            if (rand() % 100 < wPer) {
                gettimeofday(&tputstart, NULL);
                client->Put(key, key);
                gettimeofday(&tputend, NULL);

                putLatency += ((tputend.tv_sec - tputstart.tv_sec)*1000000 + (tputend.tv_usec - tputstart.tv_usec));
                putCount++;
            } else {
                struct txn_stats get_stat;
                gettimeofday(&get_stat.tstart, NULL);
                client->Get(key, value);
                gettimeofday(&get_stat.tend, NULL);

                get_stat.latency =
                    (get_stat.tend.tv_sec - get_stat.tstart.tv_sec) * 1000000
                    + (get_stat.tend.tv_usec - get_stat.tstart.tv_usec);

                get_stats.push_back(get_stat);
                getCount++;
            }
        }

        gettimeofday(&tcommitstart, NULL);
        bool status = client->Commit();
        gettimeofday(&tcommitend, NULL);

        commitCount++;
        commit_latency = ((tcommitend.tv_sec - tcommitstart.tv_sec)*1000000 + (tcommitend.tv_usec - tcommitstart.tv_usec));
        commitLatency += commit_latency;

        /** Print out transaction begin latency */
        cerr << "[" << std::this_thread::get_id() << "]";
        cerr << nTransactions+1 << " begin ";
        cerr << tbeginstart.tv_sec << "." << tbeginstart.tv_usec << " ";
        cerr << tbeginend.tv_sec << "." << tbeginend.tv_usec << " ";
        cerr << begin_latency << endl;

        /** Print out transaction get latency */
        for (int i = totalGetCount; i < totalGetCount + getCount; ++i) {
            cerr << "[" << std::this_thread::get_id() << "]";
            cerr << nTransactions+1 << " get " << " ";
            cerr << get_stats[i].tstart.tv_sec << "." << get_stats[i].tend.tv_usec << " ";
            cerr << get_stats[i].tend.tv_sec << "." << get_stats[i].tend.tv_usec << " ";
            cerr << get_stats[i].latency << endl;
        }

        /** TODO: Print out transaction put latency */

        /** Print out transaction commit latency */
        cerr << "[" << std::this_thread::get_id() << "]";
        cerr << nTransactions+1 << " commit ";
        cerr << tcommitstart.tv_sec << "." << tcommitstart.tv_usec << " ";
        cerr << tcommitend.tv_sec << "." << tcommitend.tv_usec << " ";
        cerr << commit_latency << endl;

        /** Print out transaction total latency */
        latency = (tcommitend.tv_sec - tbeginend.tv_sec)*1000000 + (tcommitend.tv_usec - tbeginend.tv_usec);
        cerr << "[" << std::this_thread::get_id() << "]";
        cerr << nTransactions+1 << " total " << tbeginend.tv_sec << "." << tbeginend.tv_usec;
        cerr << " " << tcommitend.tv_sec << "." << tcommitend.tv_usec << " " << latency << " " << status;
        cerr << endl;

        if (status) {
            tCount++;
            tLatency += latency;
        }
        totalGetCount += getCount;
        totalPutCount += putCount;
        nTransactions++;

        gettimeofday(&t1, NULL);
        if (((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > duration*1000000) {
            cerr << "Breaking out after " << nTransactions << " txn completed " << endl;
            break;
        }
    }

    fprintf(stderr, "# Commit_Ratio: %lf\n", (double)tCount/nTransactions);
    fprintf(stderr, "# Average_Latency: %lf\n", tLatency/tCount);
    fprintf(stderr, "# Begin: %d, %lf\n", beginCount, beginLatency/beginCount);
    fprintf(stderr, "# Get: %d, %lf\n", totalGetCount, getLatency/totalGetCount);
    fprintf(stderr, "# Put: %d, %lf\n", totalPutCount, putLatency/totalPutCount);
    fprintf(stderr, "# Commit: %d, %lf\n", commitCount, commitLatency/commitCount);

    return 0;
}

int rand_key()
{
    if (alpha < 0) {
        // Uniform selection of keys.
        return (rand() % nKeys);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}
