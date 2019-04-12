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

/**TODO: those two structs should really be in store/common/frontend/client.h */
#define SAMPLE 1

struct opStat {
    struct timeval tstart;
    struct timeval tend;
    long latency;
};

struct txnStats {
    vector<struct opStat> getStats;
    vector<struct opStat> putStats;
    struct opStat begin;
    struct opStat commit;
    double tLatency; // latency for the entire transaction
    bool status;
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

    cerr << "# benchClient_params: " << duration << "s, tLen=" << tLen << ", wPer=" << wPer << endl;
    vector<struct txnStats> tStats;
    int tCount = 0; // successful txns count
    double tLatency = 0.0; //successful latencies
    int beginCount = 0;
    double beginLatency = 0.0;
    int getCount = 0;
    double getLatency = 0.0;
    int putCount = 0;
    double putLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;

    struct timeval t0, t1;
    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    while (1) {
        struct txnStats tStat;
        tStat.getStats.reserve(tLen);
        tStat.putStats.reserve(tLen);

        gettimeofday(&tStat.begin.tstart, NULL);
        client->Begin();
        gettimeofday(&tStat.begin.tend, NULL);

        tStat.begin.latency = (tStat.begin.tend.tv_sec - tStat.begin.tstart.tv_sec) * 1000000
                              + (tStat.begin.tend.tv_usec - tStat.begin.tstart.tv_usec);
        beginLatency = tStat.begin.latency;
        beginCount++;

        for (int j = 0; j < tLen; j++) {
            key = keys[rand_key()];

            if (rand() % 100 < wPer) {
                struct opStat putStat;
                gettimeofday(&putStat.tstart, NULL);
                client->Put(key, key);
                gettimeofday(&putStat.tend, NULL);

                putStat.latency = (putStat.tend.tv_sec - putStat.tstart.tv_sec) * 1000000
                                  + (putStat.tend.tv_usec - putStat.tstart.tv_usec);
                tStat.putStats.push_back(putStat);
                putLatency += putStat.latency;
                putCount++;
            } else {
                struct opStat getStat;
                gettimeofday(&getStat.tstart, NULL);
                client->Get(key, value);
                gettimeofday(&getStat.tend, NULL);

                getStat.latency = (getStat.tend.tv_sec - getStat.tstart.tv_sec) * 1000000
                                  + (getStat.tend.tv_usec - getStat.tstart.tv_usec);
                tStat.getStats.push_back(getStat);
                getLatency += getStat.latency;
                getCount++;
            }
        }

        gettimeofday(&tStat.commit.tstart, NULL);
        tStat.status = client->Commit();
        gettimeofday(&tStat.commit.tend, NULL);

        tStat.commit.latency = (tStat.commit.tend.tv_sec - tStat.commit.tstart.tv_sec) * 1000000
                               + (tStat.commit.tend.tv_usec - tStat.commit.tstart.tv_usec);
        commitLatency += tStat.commit.latency;
        commitCount++;
        tStat.tLatency = (tStat.commit.tend.tv_sec - tStat.begin.tend.tv_sec) * 1000000
                         + (tStat.commit.tend.tv_usec - tStat.begin.tend.tv_usec);

        tStats.push_back(tStat);

        if (tStat.status) {
            tCount++;
            tLatency += tStat.tLatency;
        }

        gettimeofday(&t1, NULL);
        if (((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > duration*1000000) {
            break;
        }
    }

    /** Transaction summary */
    fprintf(stderr, "# Commit_Ratio: %lf\n", (double)tCount/tStats.size());
    fprintf(stderr, "# Average_Latency: %lf\n", tLatency/tCount);
    fprintf(stderr, "# Begin: %d, %lf\n", beginCount, beginLatency/beginCount);
    fprintf(stderr, "# Get: %d, %lf\n", getCount, getLatency/getCount);
    fprintf(stderr, "# Put: %d, %lf\n", putCount, putLatency/putCount);
    fprintf(stderr, "# Commit: %d, %lf\n", commitCount, commitLatency/commitCount);
    fprintf(stderr, "# Fast path: %d\n",
            ((tapirstore::ShardClient *) ((tapirstore::Client *) client)->bclient[0]->txnclient)->client->fast_path_taken);
    fprintf(stderr, "# Slow path: %d\n",
            ((tapirstore::ShardClient *) ((tapirstore::Client *) client)->bclient[0]->txnclient)->client->slow_path_taken);

    /** Log transactions statistics */
    for (unsigned int i = 0; i < tStats.size(); ++i) {
        /** Begin stats */
        cerr << i+1 << " begin ";
        cerr << tStats[i].begin.tstart.tv_sec << "." << tStats[i].begin.tstart.tv_usec << " ";
        cerr << tStats[i].begin.tend.tv_sec << "." << tStats[i].begin.tend.tv_usec << " ";
        cerr << tStats[i].begin.latency << " ";
        cerr << tStats[i].status << endl;
        /** Put stats (if present) */
        for (unsigned int j = 0; j < tStats[i].putStats.size(); ++j) {
            cerr << i+1 << " put ";
            cerr << tStats[i].putStats[j].tstart.tv_sec << "." << tStats[i].putStats[j].tstart.tv_usec << " ";
            cerr << tStats[i].putStats[j].tend.tv_sec << "." << tStats[i].putStats[j].tend.tv_usec << " ";
            cerr << tStats[i].putStats[j].latency << " ";
            cerr << tStats[i].status << endl;
        }
        /** Get stats (if present) */
        for (unsigned int j = 0; j < tStats[i].getStats.size(); ++j) {
            cerr << i+1 << " get ";
            cerr << tStats[i].getStats[j].tstart.tv_sec << "." << tStats[i].getStats[j].tstart.tv_usec << " ";
            cerr << tStats[i].getStats[j].tend.tv_sec << "." << tStats[i].getStats[j].tend.tv_usec << " ";
            cerr << tStats[i].getStats[j].latency << " ";
            cerr << tStats[i].status << endl;
        }
        /** Commit stats */
        cerr << i+1 << " commit ";
        cerr << tStats[i].commit.tstart.tv_sec << "." << tStats[i].commit.tstart.tv_usec << " ";
        cerr << tStats[i].commit.tend.tv_sec << "." << tStats[i].commit.tend.tv_usec << " ";
        cerr << tStats[i].commit.latency << " ";
        cerr << tStats[i].status << endl;
        /** Overall stats */
        cerr << i+1 << " total ";
        cerr << tStats[i].begin.tend.tv_sec << "." << tStats[i].begin.tend.tv_usec << " ";
        cerr << tStats[i].commit.tend.tv_sec << "." << tStats[i].commit.tend.tv_usec << " ";
        cerr << tStats[i].tLatency << " ";
        cerr << tStats[i].status << endl;
    }

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
