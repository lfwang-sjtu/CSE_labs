#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>
#include "map_reduce/protocol.h"

namespace mapReduce {
    bool Coordinator::is_map_finished() {
        this->mtx.lock();
        if (this->mapCount == this->map_tasks.size()) {
            this->mtx.unlock();
            return true;
        } else {
            this->mtx.unlock();
            return false;
        }
    }

    bool Coordinator::is_reduce_finished() {
        this->mtx.lock();
        if (this->reduceCount == this->reduce_tasks.size()) {
            this->mtx.unlock();
            return true;
        } else {
            this->mtx.unlock();
            return false;
        }
    }

    AskTaskResponse Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        // std::cout << "server receive asktask request" << std::endl;
        AskTaskResponse ask_task_res(NONE, -1, "");
        if (!is_map_finished()) {
            // 分配一个map任务
            for (Task &task : map_tasks) {
                this->mtx.lock();
                if (!task.is_assigned) {
                    task.is_assigned = true;
                    ask_task_res.taskType = MAP;
                    ask_task_res.index = task.index;
                    ask_task_res.filename = files[task.index];
                    mtx.unlock();
                    return ask_task_res;
                }
                this->mtx.unlock();
            }
        } else {
            // 分配一个reduce任务
            for (Task &task : reduce_tasks) {
                mtx.lock();
                if (!task.is_assigned) {
                    task.is_assigned = true;
                    ask_task_res.taskType = REDUCE;
                    ask_task_res.index = task.index;
                    ask_task_res.filename = "";
                    mtx.unlock();
                    return ask_task_res;
                }
                mtx.unlock();
            }
        }
        
        if (is_map_finished() && is_reduce_finished()) {
            this->isFinished = true;
        }
        return ask_task_res;
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        if (taskType == MAP) {
            this->mtx.lock();
            if (!map_tasks[index].is_completed) {
                map_tasks[index].is_completed = true;
                ++mapCount;
                this->mtx.unlock();
                return 114514;
            } else this->mtx.unlock();
        } else if (taskType == REDUCE) {
            this->mtx.lock();
            if (!reduce_tasks[index].is_completed) {
                reduce_tasks[index].is_completed = true;
                ++reduceCount;
                this->mtx.unlock();
                return 114514;
            } else this->mtx.unlock();
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        this->mapCount = 0;
        this->reduceCount = 0;
        int map_size = this->files.size();
        for (int i = 0; i < map_size; ++i) {
            this->map_tasks.push_back(Task(MAP, i));
        }
        for (int i = 0; i < nReduce; ++i) {
            this->reduce_tasks.push_back(Task(REDUCE, i));
        }
        output_filename = config.resultFile;
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}