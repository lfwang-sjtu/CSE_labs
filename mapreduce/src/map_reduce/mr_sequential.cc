#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> intermediate_file;
        // read all files
		// std::cout << "start reading file from chfs" << std::endl;
        for (const std::string &filename : files) {
            auto res_lookup = chfs_client->lookup(1, filename);
            auto file_inode_id = res_lookup.unwrap(); // get inode id
            auto res_type = chfs_client->get_type_attr(file_inode_id);
            auto file_length = res_type.unwrap().second.size; // get length
            auto res_read = chfs_client->read_file(file_inode_id, 0, file_length); // read file
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end()); // construct content
            std::vector<KeyVal> MapResult = Map(content);
            // write intermediate file
            intermediate_file.insert(intermediate_file.end(), MapResult.begin(), MapResult.end());
            // std::cout << "mapping finished, filename:" << filename << std::endl;
        }
        // prepare result
        // std::cout << "all mapping finished" << std::endl;
        std::sort(intermediate_file.begin(), intermediate_file.end(),
            [](KeyVal const & a, KeyVal const & b) {
		        return a.key < b.key;
	        });
        // std::cout << "start reducing" << std::endl;
        std::string result = "";
        for (unsigned int i = 0; i < intermediate_file.size();) {
            unsigned int j = i + 1;
            for (; j < intermediate_file.size() && intermediate_file[j].key == intermediate_file[i].key;)
                j++;
            std::vector<std::string> values;
            for (unsigned int k = i; k < j; k++) {
                values.push_back(intermediate_file[k].val);
            }
            // std::cout << "reduce for key:" << intermediate_file[i].key << " is prepared" << std::endl;
            std::string output = Reduce(intermediate_file[i].key, values);
            // std::cout << "reduce for key:" << intermediate_file[i].key << " is finished" << std::endl;
            result.append(intermediate_file[i].key + ' ' + output + '\n');
            i = j;
        }
        // std::cout << "Reduce key success! intermediate file: " << std::endl;
        std::vector<chfs::u8> resultContent(result.begin(), result.end());
        // std::cout << "file convert success" << std::endl;
        // write output to resultFile
        auto res_lookup_result = chfs_client->lookup(1, outPutFile);
        auto resultFile_inode_id = res_lookup_result.unwrap();
        // std::cout << "get inode id of output file ok" << std::endl;
        chfs_client->write_file(resultFile_inode_id, 0, resultContent);
        // std::cout << "write output file ok" << std::endl;
    }
}