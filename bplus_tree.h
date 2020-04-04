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
  struct Index;
  struct Record;
  struct Node;
  struct IndexNode;
  struct LeafNode;

 public:
  BPlusTree(const char* path);
  ~BPlusTree();

  void Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value);
  std::vector<std::string> GetRange(const std::string& left,
                                    const std::string& right);
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
  void UnMap(T* map_obj);
  template <typename T>
  T* Alloc();
  template <typename T>
  void Dealloc(T* node);

  constexpr size_t GetMinKeys() const;
  constexpr size_t GetMaxKeys() const;

  template <typename T>
  int UpperBound(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target) const;

  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitInternalNode(IndexNode* internal_node);
  size_t InsertKeyIntoInternalNode(IndexNode* internal_node, const char* key,
                                   off_t of_left, off_t of_right);
  size_t InsertKeyIntoLeafNode(LeafNode* leaf_node, const char* key,
                               const char* value);
  off_t GetLeafOffset(const char* key);
  IndexNode* GetOrCreateParent(Node* node);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key);

  size_t DeleteKeyFromLeafNode(LeafNode* leaf_node, const char* key,
                               bool& found);
  size_t DeleteKeyFromInternalNode(LeafNode* internal_node, const char* key,
                                   bool& found);

  bool BorrowKeyFromLeafSibling(LeafNode* node);
  void ReplaceKeyInInternalNode(off_t of_internal, const char* old_key,
                                const char* new_key);
  void MergeLeafNode(LeafNode* leaf_node);
  void Dump();

  int fd_;
  Meta* meta_;
};

#endif  // BPLUS_TREE_H