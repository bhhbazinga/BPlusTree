#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H
#include <errno.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#define log(fmt, ...)                                               \
  do {                                                              \
    fprintf(stderr, "%s:%d:" fmt, __FILE__, __LINE__, __VA_ARGS__); \
  } while (0)

class BPlusTree {
  struct Meta;
  struct Child;
  struct Record;
  struct Node;
  struct InternalNode;
  struct LeafNode;

 public:
  BPlusTree(const char* path);
  ~BPlusTree();

  bool Get(const std::string& key, std::string& value);
  // std::vector<std::string> GetRange(const std::string& left,
  //                                   const std::string& right);
  void Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);
  bool Empty() const;
  size_t get_size() const;

 private:
  void Exit(const char* msg) {
    perror(msg);
    exit(EXIT_FAILURE);
  }

  template <typename T>
  T* Map(off_t offset);
  template <typename T>
  void UnMap(T* map_obj, off_t offset);
  template <typename T>
  int UpperBound(T arr[], int n, const std::string& target) const;
  template <typename T>
  T* Alloc(off_t& offset);
  template <typename T>
  void Dealloc();

  off_t GetLeafOffset(const std::string& key);

  int InsertInternalNode(InternalNode* internal_node, const std::string& key,
                         off_t of_left, off_t of_right);
  int InsertLeafNode(LeafNode* leaf_node, const std::string& key,
                     const std::string& value);

  LeafNode* SplitLeafNode(LeafNode* leaf_node, off_t of_leaf);
  InternalNode* SplitInternalNode(InternalNode* internal_node, off_t of_internal);

  InternalNode* GetOrCreateParent(Node* node);

  void Dump();

  int fd_;
  Meta* meta_;
};

#endif  // BPLUS_TREE_H