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
  void UnMap(T* map_obj, off_t offset);
  template <typename T>
  T* Alloc();
  template <typename T>
  void Dealloc();

  constexpr size_t GetMinKeys() const;
  constexpr size_t GetMaxKeys() const;

  template <typename T>
  int UpperBound(T arr[], int n, const std::string& target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const std::string& target) const;

  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  InternalNode* SplitInternalNode(InternalNode* internal_node);
  size_t InsertKeyIntoInternalNode(InternalNode* internal_node,
                                const std::string& key, off_t of_left,
                                off_t of_right);
  size_t InsertKeyIntoLeafNode(LeafNode* leaf_node, const std::string& key,
                            const std::string& value);
  off_t GetLeafOffset(const std::string& key);
  InternalNode* GetOrCreateParent(Node* node);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const std::string& key);

  size_t DeleteKeyFromLeafNode(LeafNode* leaf_node, const std::string& key);
  size_t DeleteKeyFromInternalNode(LeafNode* internal_node,
                                const std::string& key);

  template <typename T>
  bool BorrowKeyFromSibling(T* node);

  void Dump();

  int fd_;
  Meta* meta_;
};

#endif  // BPLUS_TREE_H