#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include <cstdio>
#include <string>

#define DEBUG

#ifdef DEBUG
#define log(fmt, ...)                                               \
  do {                                                              \
    fprintf(stderr, "%s:%d:" fmt, __FILE__, __LINE__, __VA_ARGS__); \
  } while (0)

#define log2(fmt, ...)                 \
  do {                                 \
    fprintf(stderr, fmt, __VA_ARGS__); \
  } while (0)
#endif

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
  bool Get(const std::string& key, std::string& value) const;
  std::vector<std::string> GetRange(const std::string& left,
                                    const std::string& right) const;
  bool Empty() const;
  size_t Size() const;
#ifdef DEBUG
  void Dump();
#endif

 private:
  void Exit(const char* msg) const;
  template <typename T>
  T* Map(off_t offset) const;
  template <typename T>
  void UnMap(T* map_obj) const;
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

  off_t GetLeafOffset(const char* key) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int fd_;
  Meta* meta_;
};

#endif  // BPLUS_TREE_H