#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  // UNIMPLEMENTED();
  // Check if the source string is empty
  if (src.empty()) {
      // If empty, directly append the new entry
      return filename + ":" + std::to_string(id);
  }

  // If not empty, append the new entry with a separator "/"
  src += "/";
  src += filename + ":" + std::to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::istringstream iss(src);
  std::string entry;

  while (std::getline(iss, entry, '/')) {
    // Split each entry into name and id (assuming id is an integer)
    std::istringstream entryStream(entry);
    std::string namePart;
    std::getline(entryStream, namePart, ':');
    std::string idPart;
    std::getline(entryStream, idPart, ':');
    // Convert idPart to an inode_id_t (you may need additional validation)
    inode_id_t id = std::stoi(idPart);
    // Create a DirectoryEntry and add it to the list
    list.emplace_back(namePart, id);
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();
  // Find the position of the filename in the source string
  size_t pos = src.find(filename + ":");

  // Check if the filename was found
  if (pos != std::string::npos) {
    // Erase the found entry along with the trailing '/'
    src.erase(pos, src.find('/', pos) - pos + 2);
  }

  return src;

  // return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::cout << "### read_directory(), inode_id=" << id << std::endl;
  auto read_inode_res = fs->read_file(id);
  if (read_inode_res.is_err()) {
    std::cout << "Error in read_inode!!!!!!" << std::endl;
  }
  auto content = read_inode_res.unwrap();
  std::cout << "content length=" << content.size() << std::endl;
  for (auto it = content.begin(); it != content.end(); ++it) {
    std::cout << (*it);
  }
  std::cout << std::endl;
  std::string src(content.begin(), content.end());
  std::cout << "filename table raw:" << src << std::endl;
  parse_directory(src, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::cout << "### lookup(), inode_id=" << id << ", name=" << name << std::endl;
  read_directory(this, id, list);
  std::cout << "read directory file ok, list size is : " << list.size() << std::endl;
  std::cout << "print this list!: ";
  for (auto it = list.begin(); it != list.end(); ++it) {
    std::cout << "filename: " << (*it).name << ", inode_id: " << (*it).id << std::endl;
    if (strcmp((*it).name.c_str(), name) == 0) {
      std::cout << "finish look up, shot!" << std::endl; 
      return ChfsResult<inode_id_t>((*it).id);
    }
  }

  std::cout << "finish loop up, miss!" << std::endl;

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  // UNIMPLEMENTED();
  std::cout << "### mk_helper(), parent inode_id_t=" << id << ", create filename=" << name << std::endl;
  auto lookup_res = lookup(id, name);
  if (!lookup_res.is_err()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  std::cout << "lookup didn't find" << std::endl;
  auto read_parent_inode_res = read_file(id);
  auto directory_content = read_parent_inode_res.unwrap();
  std::string src(directory_content.begin(), directory_content.end());
  std::cout << "parent directory before append: " << src << std::endl;
  auto alloc_inode_res = alloc_inode(type);
  src = append_to_directory(src, name, alloc_inode_res.unwrap());
  std::cout << "parent directory after append: " << src << std::endl;
  std::vector<u8> content(src.begin(), src.end());
  // std::cout << "content: " << std::endl;
  // for (auto it = content.begin(); it != content.end(); ++it) {
  //   std::cout << (char)*it;
  // }
  // std::cout << std::endl;
  // std::cout << "content size: " << content.size() << std::endl;
  write_file(id, content);

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  // UNIMPLEMENTED();
  auto lookup_res = lookup(parent, name);
  if (lookup_res.is_err()) {
    return ChfsNullResult(ErrorType::NotExist);
  }
  remove_file(lookup_res.unwrap());
  auto read_parent_inode_res = read_file(parent);
  std::string src(read_parent_inode_res.unwrap().begin(), read_parent_inode_res.unwrap().end());
  rm_from_directory(src, name);
  std::vector<u8> content(src.begin(), src.end());
  write_file(parent, content);
  
  return KNullOk;
}

} // namespace chfs
