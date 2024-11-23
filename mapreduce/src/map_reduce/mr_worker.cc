#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce
{

    Worker::Worker(MR_CoordinatorConfig config)
    {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename)
    {
        // Lab4: Your code goes here.
        // read file
        // std::cout << "start doing map! index=" << index << ", filename=" << filename << std::endl;
        auto res_lookup_file = chfs_client->lookup(1, filename);
        auto file_inode_id = res_lookup_file.unwrap();
        auto res_type = chfs_client->get_type_attr(file_inode_id);
        auto file_length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(file_inode_id, 0, file_length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end()); // construct content
        // std::cout << "read content succeed, now start mapping" << std::endl;
        // do map
        std::vector<KeyVal> mapResult = Map(content);

        // std::cout << "mapping result output ok" << std::endl;
        // for (auto it = mapResult.begin(); it != mapResult.end(); ++it) {
        //     std::cout << "key=" << (*it).key << ", value=" << (*it).val << std::endl;
        // }

        // write intermediate file
        const int REDUCER_COUNT = 4;
        std::string intermediate_file[REDUCER_COUNT];
        for (KeyVal &kv : mapResult)
        {
            unsigned long hash = 0;
            for (char &ch : kv.key)
            {
                hash = hash + ch;
            }
            hash = hash % REDUCER_COUNT;
            intermediate_file[hash] += kv.key + '\n';
        }
        // std::cout << "finish distributed hashing ok" << std::endl;
        // for (int i = 0; i < REDUCER_COUNT; ++i) {
        //     std::cout << "intermediate file ()" << i << ":" <<  intermediate_file[i] << std::endl;
        // }
        for (int i = 0; i < REDUCER_COUNT; ++i) {
            if (intermediate_file[i] == "") continue;
            auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(index) + "-" + std::to_string(i));
            auto intermediate_file_inode_id = res_mknode.unwrap();
            std::vector<uint8_t> content(intermediate_file[i].begin(), intermediate_file[i].end());
            chfs_client->write_file(intermediate_file_inode_id, 0, content);
            // std::cout << "Map: finish writing mr-" + std::to_string(index) + "-" + std::to_string(i) << std::endl;
            // std::cout << intermediate_file[i] << std::endl;
        }
    }

    void Worker::doReduce(int index, int nfiles)
    {
        // std::cout << "start doing reduce" << std::endl;
        // Lab4: Your code goes here.
        std::vector<KeyVal> intermediate;
        // read all needed intermediate file
        for (unsigned int i = 0; i < nfiles; ++i) {
            // std::cout << "try to open intermediate file: mr" + std::to_string(i) + "-" + std::to_string(index) << std::endl;
            auto res_lookup_inter = chfs_client->lookup(1, "mr-" + std::to_string(i) + "-" + std::to_string(index));
            if (res_lookup_inter.is_err()) {
                continue;
            }
            auto intermediate_file_inode_id = res_lookup_inter.unwrap();
            auto res_type = chfs_client->get_type_attr(intermediate_file_inode_id);
            auto file_length = res_type.unwrap().second.size;                                   // get length
            auto res_read = chfs_client->read_file(intermediate_file_inode_id, 0, file_length); // read file
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end()); // construct content
            // std::cout << "read intermediate file: mr" + std::to_string(i) + "-" + std::to_string(index) << std::endl;
            // std::cout << content;
            std::istringstream ss(content);
            KeyVal kv("", "1");
            while (getline(ss, kv.key, '\n'))
            {
                if (kv.key[0] == 0) {
                    continue;
                }
                intermediate.push_back(kv);
            }
        }
        // sort intermediate
        std::sort(intermediate.begin(), intermediate.end(),
                  [](KeyVal const &a, KeyVal const &b)
                  {
                      return a.key < b.key;
                  });
        // std::cout << "Get all inetrmediate and sorted:" << std::endl;
        // for (auto it = intermediate.begin(); it != intermediate.end(); ++it) {
        //     std::cout << (*it).key << ' ' << (*it).val << std::endl;
        // }
        // start reduce
        std::string result = "";
        unsigned int size = intermediate.size();
        for (unsigned int i = 0; i < size;) {
            std::string key = intermediate[i].key;
            std::vector<std::string> values;
            for (; i < size && (key == intermediate[i].key); ++i)
            {
                values.push_back(intermediate[i].val);
            }
            std::string output = Reduce(key, values);
            result.append(key + ' ' + output + '\n');
        }
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto outputFile_inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(outputFile_inode_id);
        auto length = res_type.unwrap().second.size;
        std::vector<uint8_t> content(result.begin(), result.end());
        chfs_client->write_file(outputFile_inode_id, length, content);
        // auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-out-" + std::to_string(index));
        // auto output_inode_id = res_mknode.unwrap();
        // std::vector<uint8_t> content(result.begin(), result.end());
        // chfs_client->write_file(output_inode_id, 0, content);
        // std::cout << "finish writing reducing file: mr-out-" + std::to_string(index) << std::endl;
        // std::cout << result << std::endl;
    }

    void Worker::doSubmit(int taskType, int index)
    {
        // Lab4: Your code goes here.
        auto rpc_response = mr_client->call(SUBMIT_TASK, taskType, index);
        int rpc_result = rpc_response.unwrap()->as<int>();
        if (rpc_result != 114514) {
            std::cerr << "Worker submit failed, worker terminated\n";
            exit(-1);
        }
    }

    void Worker::stop()
    {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork()
    {
        while (!shouldStop)
        {
            // Lab4: Your code goes here.
            // ask for a task
            auto ask_task_res = mr_client->call(ASK_TASK, 0);
            AskTaskResponse task_msg = ask_task_res.unwrap()->as<AskTaskResponse>();
            // std::cout << "Worker get a task: tasktype=" << task_msg.taskType << ", taskindex=" << task_msg.index << ", filename(ifexist)=" << task_msg.filename << std::endl;
            // do the task and submit
            if (task_msg.taskType == MAP) {
                doMap(task_msg.index, task_msg.filename);
                doSubmit(MAP, task_msg.index);
            } else if (task_msg.taskType == REDUCE) {
                doReduce(task_msg.index, 6);
                doSubmit(REDUCE, task_msg.index);
            } else sleep(1);
        }
    }
}