#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE
    };

    struct Task {
        int taskType;
        bool is_assigned;
        bool is_completed;
        int index;

        Task(int _taskType, int _index) {
            taskType = _taskType;
            is_assigned = false;
            is_completed = false;
            index = _index;
        }
    };

    struct AskTaskResponse {
        int taskType;
        int index;
        std::string filename;

        MSGPACK_DEFINE(
            taskType,
            index,
            filename
        )

        AskTaskResponse(int _t, int i, std::string f) {
            taskType = _t;
            index = i;
            filename = f;
        }
        AskTaskResponse() {}
    };
    

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        AskTaskResponse askTask(int);
        int submitTask(int taskType, int index);
        bool Done();
        bool is_map_finished();
        bool is_reduce_finished();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        std::vector<Task> map_tasks;
        std::vector<Task> reduce_tasks;

        int mapCount;
        int reduceCount;

        std::string output_filename;
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(int taskType, int index);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
    };
}