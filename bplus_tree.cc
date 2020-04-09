#include "bplus_tree.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <unordered_map>

const off_t kMetaOffset = 0;
const int kOrder = 128;
static_assert(kOrder >= 3,
              "The order of B+Tree should be greater than or equal to 3.");
const int kMaxKeySize = 32;
const int kMaxValueSize = 256;
const int kMaxCacheSize = 1024 * 1024 * 50;
typedef char Key[kMaxKeySize];
typedef char Value[kMaxValueSize];

void Exit(const char* msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

struct BPlusTree::Meta {
  off_t offset;   // ofset of self
  off_t root;     // offset of root
  off_t block;    // offset of next new node
  size_t height;  // height of B+Tree
  size_t size;    // key size
};

struct BPlusTree::Index {
  Index() : offset(0) { std::memset(key, 0, sizeof(key)); }

  off_t offset;
  Key key;

  void UpdateIndex(off_t of, const char* k) {
    offset = of;
    strncpy(key, k, kMaxKeySize);
  }
  void UpdateKey(const char* k) { strncpy(key, k, kMaxKeySize); }
};

struct BPlusTree::Record {
  Key key;
  Value value;

  void UpdateKV(const char* k, const char* v) {
    strncpy(key, k, kMaxKeySize);
    strncpy(value, v, kMaxValueSize);
  }
  void UpdateKey(const char* k) { strncpy(key, k, kMaxKeySize); }
  void UpdateValue(const char* v) { strncpy(value, v, kMaxValueSize); }
};

struct BPlusTree::Node {
  Node() : parent(0), left(0), right(0), count(0) {}
  Node(off_t parent_, off_t leaf_, off_t right_, size_t count_)
      : parent(parent_), left(leaf_), right(right_), count(count_) {}
  ~Node() = default;

  off_t offset;  // offset of self
  off_t parent;  // offset of parent
  off_t left;    // offset of left node(may be sibling)
  off_t right;   // offset of right node(may be sibling)
  size_t count;  // count of keys
};

struct BPlusTree::IndexNode : BPlusTree::Node {
  IndexNode() = default;
  ~IndexNode() = default;

  const char* FirstKey() const {
    assert(count > 0);
    return indexes[0].key;
  }

  const char* LastKey() const {
    assert(count > 0);
    return indexes[count - 1].key;
  }

  const char* Key(int index) const {
    assert(count > 0);
    assert(index >= 0);
    assert(index <= kOrder);
    return indexes[index].key;
  }

  void UpdateKey(int index, const char* k) {
    assert(index >= 0);
    assert(index <= kOrder);
    indexes[index].UpdateKey(k);
  }

  void UpdateOffset(int index, off_t offset) {
    assert(index >= 0);
    assert(index <= kOrder);
    indexes[index].offset = offset;
  }

  void UpdateIndex(int index, const char* k, off_t offset) {
    assert(index >= 0);
    assert(index <= kOrder);
    UpdateKey(index, k);
    UpdateOffset(index, offset);
  }

  void DeleteKeyAtIndex(int index) {
    assert(index >= 0);
    assert(index <= kOrder);
    std::memmove(&indexes[index], &indexes[index + 1],
                 sizeof(indexes[0]) * (count-- - index));
  }

  void InsertKeyAtIndex(int index, const char* k) {
    assert(index >= 0);
    assert(index <= kOrder);
    std::memmove(&indexes[index + 1], &indexes[index],
                 sizeof(indexes[0]) * (++count - index));
    UpdateKey(index, k);
  }

  void InsertIndexAtIndex(int index, const char* k, off_t offset) {
    assert(index >= 0);
    assert(index <= kOrder);
    std::memmove(&indexes[index + 1], &indexes[index],
                 sizeof(indexes[0]) * (++count - index));
    UpdateIndex(index, k, offset);
  }

  void MergeLeftSibling(IndexNode* sibling) {
    std::memmove(&indexes[sibling->count + 1], &indexes[0],
                 sizeof(indexes[0]) * (count + 1));
    std::memcpy(&indexes[0], &sibling->indexes[0],
                sizeof(indexes[0]) * (sibling->count + 1));
    count += (sibling->count + 1);
  }

  void MergeRightSibling(IndexNode* sibling) {
    std::memcpy(&indexes[count], &sibling->indexes[0],
                sizeof(indexes[0]) * (sibling->count + 1));
    count += sibling->count;
  }

  Index indexes[kOrder + 1];
};

struct BPlusTree::LeafNode : BPlusTree::Node {
  LeafNode() = default;
  ~LeafNode() = default;

  const char* FirstKey() const {
    assert(count > 0);
    return records[0].key;
  }

  const char* LastKey() const {
    assert(count > 0);
    return records[count - 1].key;
  }

  const char* Key(int index) const {
    assert(count > 0);
    assert(index >= 0);
    return records[index].key;
  }

  const char* FirstValue() const {
    assert(count > 0);
    return records[0].value;
  }

  const char* LastValue() const {
    assert(count > 0);
    return records[count - 1].value;
  }

  const char* Value(int index) const {
    assert(count > 0);
    return records[index].value;
  }

  void UpdateValue(int index, const char* v) {
    assert(index >= 0);
    records[index].UpdateValue(v);
  }

  void UpdateKey(int index, const char* k) {
    assert(index >= 0);
    records[index].UpdateKey(k);
  }

  void UpdateKV(int index, const char* k, const char* v) {
    assert(index >= 0);
    records[index].UpdateKV(k, v);
  }

  void InsertKVAtIndex(int index, const char* k, const char* v) {
    assert(index >= 0);
    assert(index < kOrder);
    std::memmove(&records[index + 1], &records[index],
                 sizeof(records[0]) * (count++ - index));
    UpdateKV(index, k, v);
  }

  void DeleteKVAtIndex(int index) {
    assert(index >= 0);
    assert(index < kOrder);
    std::memmove(&records[index], &records[index + 1],
                 sizeof(records[0]) * (--count - index));
  }

  void MergeLeftSibling(LeafNode* sibling) {
    std::memmove(&records[sibling->count], &records[0],
                 sizeof(records[0]) * count);
    std::memcpy(&records[0], &sibling->records[0],
                sizeof(records[0]) * sibling->count);
    count += sibling->count;
  }

  void MergeRightSibling(LeafNode* sibling) {
    std::memcpy(&records[count], &sibling->records[0],
                sizeof(records[0]) * sibling->count);
    count += sibling->count;
  }

  BPlusTree::Record records[kOrder];
};

class BPlusTree::BlockCache {
  struct Node;

 public:
  BlockCache() : head_(new Node()), size_(0) {
    head_->next = head_;
    head_->prev = head_;
  }

  ~BlockCache() {}

  void DeleteNode(Node* node) {
    if (node->next == node->prev && nullptr == node->next) return;
    node->prev->next = node->next;
    node->next->prev = node->prev;
    node->next = node->prev = nullptr;
    size_ -= node->size;
  }

  void InsertHead(Node* node) {
    node->next = head_->next;
    node->prev = head_;
    head_->next->prev = node;
    head_->next = node;
    size_ += node->size;
  }

  Node* DeleteTail() {
    Node* tail = head_->prev;
    if (tail == head_) return nullptr;
    DeleteNode(tail);
    return tail;
  }

  template <typename T>
  void Put(T* block) {
    if (size_ > kMaxCacheSize) Kick();
    assert(size_ < kMaxCacheSize);

    if (offset2node_.find(block->offset) == offset2node_.end()) {
      Node* node = new Node(block, block->offset, sizeof(T));
      offset2node_.emplace(block->offset, node);
      InsertHead(node);
    } else {
      Node* node = offset2node_[block->offset];
      if (--node->ref == 0) InsertHead(node);
    }
  }

  template <typename T>
  T* Get(int fd, off_t offset) {
    if (offset2node_.find(offset) == offset2node_.end()) {
      struct stat st;
      if (fstat(fd, &st) != 0) Exit("fstat");
      constexpr int size = sizeof(T);
      if (st.st_size < offset + size && ftruncate(fd, offset + size) != 0) {
        Exit("ftruncate");
      }
      // Align offset to page size.
      // See http://man7.org/linux/man-pages/man2/mmap.2.html
      off_t page_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
      void* addr = mmap(nullptr, size + offset - page_offset,
                        PROT_READ | PROT_WRITE, MAP_SHARED, fd, page_offset);
      if (MAP_FAILED == addr) Exit("mmap");
      char* start = static_cast<char*>(addr);
      return reinterpret_cast<T*>(&start[offset - page_offset]);
    }

    Node* node = offset2node_[offset];
    ++node->ref;
    DeleteNode(node);
    return static_cast<T*>(node->block);
  }

 private:
  void Kick() {
    Node* tail = DeleteTail();
    if (nullptr == tail) return;

    assert(tail != head_);

    off_t page_offset = tail->offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
    char* start = reinterpret_cast<char*>(tail->block);
    void* addr = static_cast<void*>(&start[page_offset - tail->offset]);
    if (munmap(addr, tail->size + tail->offset - page_offset) != 0) {
      Exit("munmap");
    }
    offset2node_.erase(tail->offset);
    delete tail;
  }

  struct Node {
    Node()
        : block(nullptr),
          offset(0),
          size(0),
          ref(0),
          prev(nullptr),
          next(nullptr) {}

    Node(void* block_, off_t offset_, size_t size_)
        : block(block_),
          offset(offset_),
          size(size_),
          ref(1),
          prev(nullptr),
          next(nullptr) {}

    void* block;
    off_t offset;
    size_t size;
    size_t ref;
    Node* prev;
    Node* next;
  };

  Node* head_;
  size_t size_;
  std::unordered_map<off_t, Node*> offset2node_;
};

BPlusTree::BPlusTree(const char* path)
    : fd_(open(path, O_CREAT | O_RDWR, 0600)), block_cache_(new BlockCache()) {
  if (fd_ == -1) Exit("open");
  meta_ = Map<Meta>(kMetaOffset);
  if (meta_->height == 0) {
    // Initialize B+tree;
    constexpr off_t of_root = kMetaOffset + sizeof(Meta);
    LeafNode* root = new (Map<LeafNode>(of_root)) LeafNode();
    root->offset = of_root;
    meta_->height = 1;
    meta_->root = of_root;
    meta_->block = of_root + sizeof(LeafNode);
    UnMap<LeafNode>(root);
  }
}

BPlusTree::~BPlusTree() {
  UnMap(meta_);
  close(fd_);
}

void BPlusTree::Put(const std::string& key, const std::string& value) {
  // 1. Find Leaf node.
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  if (InsertKVIntoLeafNode(leaf_node, key.data(), value.data()) <=
      GetMaxKeys()) {
    // 2.If records of leaf node less than or equals kOrder - 1 then finish.
    UnMap<LeafNode>(leaf_node);
    return;
  }

  // 3. Split leaf node to two leaf nodes.
  LeafNode* split_node = SplitLeafNode(leaf_node);
  const char* mid_key = split_node->FirstKey();
  IndexNode* parent_node = GetOrCreateParent(leaf_node);
  off_t of_parent = leaf_node->parent;
  split_node->parent = of_parent;

  // 4.Insert key to parent of splited leaf nodes and
  // link two splited left nodes to parent.
  if (InsertKeyIntoIndexNode(parent_node, mid_key, leaf_node, split_node) <=
      GetMaxKeys()) {
    UnMap<LeafNode>(leaf_node);
    UnMap<LeafNode>(split_node);
    UnMap<IndexNode>(parent_node);
    return;
  }

  // 5.Split index node from bottom to up repeatedly
  // until count <= kOrder - 1.
  size_t count;
  do {
    IndexNode* child_node = parent_node;
    IndexNode* split_node = SplitIndexNode(child_node);
    const char* mid_key = child_node->Key(child_node->count);
    parent_node = GetOrCreateParent(child_node);
    of_parent = child_node->parent;
    split_node->parent = of_parent;
    count =
        InsertKeyIntoIndexNode(parent_node, mid_key, child_node, split_node);
    UnMap<IndexNode>(child_node);
  } while (count > GetMaxKeys());
  UnMap<IndexNode>(parent_node);
}

bool BPlusTree::Delete(const std::string& key) {
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  // 1. Delete key from leaf node
  int index = GetIndexFromLeafNode(leaf_node, key.data());
  if (index == -1) {
    UnMap(leaf_node);
    return false;
  }

  leaf_node->DeleteKVAtIndex(index);
  --meta_->size;
  // 2. If leaf_node is root then return.
  if (leaf_node->parent == 0) {
    UnMap(leaf_node);
    return true;
  }

  // 3. If count of leaf_node >= GetMinKeys() then return else execute step 3.
  if (leaf_node->count >= GetMinKeys()) {
    UnMap(leaf_node);
    return true;
  }

  // 4. If borrow from siblings successfully then return else execute step 4.
  if (BorrowFromLeafSibling(leaf_node)) {
    UnMap<LeafNode>(leaf_node);
    return true;
  }

  // 5. Merge two leaf nodes.
  leaf_node = MergeLeaf(leaf_node);

  IndexNode* index_node = Map<IndexNode>(leaf_node->parent);
  UnMap<LeafNode>(leaf_node);

  // 6. If count of index_node >= GetMinKeys() then return or execute 6.
  // 7. If count of one of sibling > GetMinKeys() then swap its key and parent's
  // key then return or execute 7.
  while (index_node->parent != 0 && index_node->count < GetMinKeys() &&
         !BorrowFromIndexSibling(index_node)) {
    // 8. Merge index_node and its' parent and sibling.
    IndexNode* old_index_node = MergeIndex(index_node);
    index_node = Map<IndexNode>(old_index_node->parent);
    UnMap(old_index_node);
  }

  if (index_node->parent == 0 && index_node->count == 0) {
    // 9. Root is removed, update new root and height.
    Node* new_root = Map<Node>(index_node->indexes[0].offset);
    assert(new_root->left == 0);
    assert(new_root->right == 0);
    new_root->parent = 0;
    meta_->root = new_root->offset;
    --meta_->height;
    UnMap(new_root);
    Dealloc(index_node);
    return true;
  }

  UnMap<IndexNode>(index_node);
  return true;
}

bool BPlusTree::Get(const std::string& key, std::string& value) const {
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  int index = GetIndexFromLeafNode(leaf_node, key.data());
  if (index == -1) {
    UnMap<LeafNode>(leaf_node);
    return false;
  }
  value = leaf_node->Value(index);
  UnMap<LeafNode>(leaf_node);
  return true;
}

template <typename T>
T* BPlusTree::Map(off_t offset) const {
  return block_cache_->Get<T>(fd_, offset);
}

template <typename T>
void BPlusTree::UnMap(T* map_obj) const {
  block_cache_->Put<T>(map_obj);
}

constexpr size_t BPlusTree::GetMinKeys() const { return (kOrder + 1) / 2 - 1; }

constexpr size_t BPlusTree::GetMaxKeys() const { return kOrder - 1; }

BPlusTree::IndexNode* BPlusTree::GetOrCreateParent(Node* node) {
  if (node->parent == 0) {
    // Split root node.
    IndexNode* parent_node = Alloc<IndexNode>();
    node->parent = parent_node->offset;
    meta_->root = parent_node->offset;
    ++meta_->height;
    return parent_node;
  }
  return Map<IndexNode>(node->parent);
}

template <typename T>
int BPlusTree::UpperBound(T arr[], int n, const char* key) const {
  assert(n <= GetMaxKeys());
  int l = 0, r = n - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize) <= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return l;
}

template <typename T>
int BPlusTree::LowerBound(T arr[], int n, const char* key) const {
  assert(n <= GetMaxKeys());
  int l = 0, r = n - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize) < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return l;
};

template <typename T>
T* BPlusTree::Alloc() {
  T* node = new (Map<T>(meta_->block)) T();
  node->offset = meta_->block;
  meta_->block += sizeof(T);
  return node;
}

template <typename T>
void BPlusTree::Dealloc(T* node) {
  UnMap<T>(node);
}

off_t BPlusTree::GetLeafOffset(const char* key) const {
  size_t height = meta_->height;
  off_t offset = meta_->root;
  if (height <= 1) {
    assert(height == 1);
    return offset;
  }
  // 1. Find bottom index node.
  IndexNode* index_node = Map<IndexNode>(offset);
  while (--height > 1) {
    int index = UpperBound(index_node->indexes, index_node->count, key);
    off_t of_child = index_node->indexes[index].offset;
    UnMap(index_node);
    index_node = Map<IndexNode>(of_child);
    offset = of_child;
  }
  // 2. Get offset of leaf node.
  int index = UpperBound(index_node->indexes, index_node->count, key);
  off_t of_child = index_node->indexes[index].offset;
  UnMap<IndexNode>(index_node);
  return of_child;
}

inline size_t BPlusTree::InsertKeyIntoIndexNode(IndexNode* index_node,
                                                const char* key,
                                                Node* left_node,
                                                Node* right_node) {
  assert(index_node->count <= GetMaxKeys());
  int index = UpperBound(index_node->indexes, index_node->count, key);
  index_node->InsertIndexAtIndex(index, key, left_node->offset);
  index_node->UpdateOffset(index + 1, right_node->offset);
  return index_node->count;
}

size_t BPlusTree::InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                                       const char* value) {
  assert(leaf_node->count <= GetMaxKeys());
  int index = UpperBound(leaf_node->records, leaf_node->count, key);
  if (index > 0 &&
      std::strncmp(leaf_node->Key(index - 1), key, kMaxKeySize) == 0) {
    leaf_node->UpdateValue(index - 1, value);
    return leaf_node->count;
  }

  leaf_node->InsertKVAtIndex(index, key, value);
  ++meta_->size;
  return leaf_node->count;
}

BPlusTree::LeafNode* BPlusTree::SplitLeafNode(LeafNode* leaf_node) {
  assert(leaf_node->count == kOrder);
  constexpr int mid = (kOrder - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder - mid;

  LeafNode* split_node = Alloc<LeafNode>();

  // Change count.
  leaf_node->count = left_count;
  split_node->count = right_count;

  // Copy right part of index_node.
  std::memcpy(&split_node->records[0], &leaf_node->records[mid],
              sizeof(split_node->records[0]) * right_count);

  // Link siblings.
  split_node->left = leaf_node->offset;
  split_node->right = leaf_node->right;
  leaf_node->right = split_node->offset;
  if (split_node->right != 0) {
    LeafNode* new_sibling = Map<LeafNode>(split_node->right);
    new_sibling->left = split_node->offset;
    UnMap(new_sibling);
  }
  return split_node;
}

BPlusTree::IndexNode* BPlusTree::SplitIndexNode(IndexNode* index_node) {
  assert(index_node->count == kOrder);
  constexpr int mid = (kOrder - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder - mid - 1;

  IndexNode* split_node = Alloc<IndexNode>();

  // Change count.
  index_node->count = left_count;
  split_node->count = right_count;

  // Copy right part of index_node.
  std::memcpy(&split_node->indexes[0], &index_node->indexes[mid + 1],
              sizeof(split_node->indexes[0]) * (right_count + 1));

  // Link old childs to new splited parent.
  for (int i = mid + 1; i <= kOrder; ++i) {
    off_t of_child = index_node->indexes[i].offset;
    LeafNode* child_node = Map<LeafNode>(of_child);
    child_node->parent = split_node->offset;
    UnMap(child_node);
  }

  // Link siblings.
  split_node->left = index_node->offset;
  split_node->right = index_node->right;
  index_node->right = split_node->offset;
  if (split_node->right != 0) {
    IndexNode* new_sibling = Map<IndexNode>(split_node->right);
    new_sibling->left = split_node->offset;
    UnMap<IndexNode>(new_sibling);
  }
  return split_node;
}

inline int BPlusTree::GetIndexFromLeafNode(LeafNode* leaf_node,
                                           const char* key) const {
  int index = LowerBound(leaf_node->records, leaf_node->count, key);
  return index < static_cast<int>(leaf_node->count) &&
                 std::strncmp(leaf_node->Key(index), key, kMaxKeySize) == 0
             ? index
             : -1;
}

std::vector<std::pair<std::string, std::string>> BPlusTree::GetRange(
    const std::string& left_key, const std::string& right_key) const {
  std::vector<std::pair<std::string, std::string>> res;
  off_t of_leaf = GetLeafOffset(left_key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  int index = LowerBound(leaf_node->records, leaf_node->count, left_key.data());
  for (int i = index; i < leaf_node->count; ++i) {
    res.emplace_back(leaf_node->Key(i), leaf_node->Value(i));
  }

  of_leaf = leaf_node->right;
  bool finish = false;
  while (of_leaf != 0 && !finish) {
    LeafNode* right_leaf_node = Map<LeafNode>(of_leaf);
    for (int i = 0; i < right_leaf_node->count; ++i) {
      if (strncmp(right_leaf_node->Key(i), right_key.data(), kMaxKeySize) <= 0) {
        res.emplace_back(right_leaf_node->Key(i), right_leaf_node->Value(i));
      } else {
        finish = true;
        break;
      }
    }
    of_leaf = right_leaf_node->right;
    UnMap(right_leaf_node);
  }

  UnMap(leaf_node);
  return res;
}

bool BPlusTree::Empty() const { return meta_->size == 0; }

size_t BPlusTree::Size() const { return meta_->size; }

// Try Borrow key from left sibling.
bool BPlusTree::BorrowFromLeftLeafSibling(LeafNode* leaf_node) {
  if (leaf_node->left == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->left);
  if (sibling->parent != leaf_node->parent || sibling->count <= GetMinKeys()) {
    if (sibling->parent == leaf_node->parent) {
      assert(sibling->count == GetMinKeys());
    }
    UnMap(sibling);
    return false;
  }
  // 1. Borrow last key from left sibling.
  leaf_node->InsertKVAtIndex(0, sibling->LastKey(), sibling->LastValue());
  --sibling->count;

  // 2. Update parent's key.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  parent_node->UpdateKey(index, leaf_node->FirstKey());
  UnMap<IndexNode>(parent_node);
  UnMap<LeafNode>(sibling);
  return true;
}

// Try Borrow key from right sibling.
bool BPlusTree::BorrowFromRightLeafSibling(LeafNode* leaf_node) {
  if (leaf_node->right == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->right);

  if (sibling->parent != leaf_node->parent || sibling->count <= GetMinKeys()) {
    if (sibling->parent == leaf_node->parent) {
      assert(sibling->count == GetMinKeys());
    }
    UnMap(sibling);
    return false;
  }

  // 1. Borrow frist key from right sibling.
  leaf_node->UpdateKV(leaf_node->count++, sibling->FirstKey(),
                      sibling->FirstValue());
  sibling->DeleteKVAtIndex(0);

  // 2. Update parent's key.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  parent_node->UpdateKey(index - 1, sibling->FirstKey());

  UnMap<IndexNode>(parent_node);
  UnMap<LeafNode>(sibling);
  return true;
}

inline bool BPlusTree::BorrowFromLeafSibling(LeafNode* leaf_node) {
  assert(leaf_node->count == GetMinKeys() - 1);
  assert(leaf_node->parent != 0);
  return BorrowFromLeftLeafSibling(leaf_node) ||
         BorrowFromRightLeafSibling(leaf_node);
}

// Try merge left leaf node.
bool BPlusTree::MergeLeftLeaf(LeafNode* leaf_node) {
  if (leaf_node->left == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->left);
  if (sibling->parent != leaf_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Delete key from parent.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  parent_node->DeleteKeyAtIndex(index);

  // 2. Merge left sibling.
  leaf_node->MergeLeftSibling(sibling);

  // 3. Link new sibling.
  leaf_node->left = sibling->left;
  if (sibling->left != 0) {
    LeafNode* new_sibling = Map<LeafNode>(sibling->left);
    new_sibling->right = leaf_node->offset;
    UnMap(new_sibling);
  }

  UnMap(parent_node);
  Dealloc(sibling);
  return true;
}

// Try Merge right node.
bool BPlusTree::MergeRightLeaf(LeafNode* leaf_node) {
  if (leaf_node->right == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->right);
  if (sibling->parent != leaf_node->parent) {
    UnMap(sibling);
    return false;
  }

  // 1. Delete key from parent.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  parent_node->UpdateKey(index - 1, parent_node->Key(index));
  parent_node->DeleteKeyAtIndex(index);
  UnMap(parent_node);

  // 2. Merge right sibling.
  leaf_node->MergeRightSibling(sibling);

  // 3. Link new sibling.
  leaf_node->right = sibling->right;
  if (sibling->right != 0) {
    LeafNode* new_sibling = Map<LeafNode>(sibling->right);
    new_sibling->left = leaf_node->offset;
    UnMap(new_sibling);
  }

  Dealloc(sibling);
  return true;
}

inline BPlusTree::LeafNode* BPlusTree::MergeLeaf(LeafNode* leaf_node) {
  // Merge left node to leaf_node or right node to leaf_node.
  assert(leaf_node->count == GetMinKeys() - 1);
  assert(leaf_node->parent != 0);
  assert(meta_->root != leaf_node->offset);
  assert(MergeLeftLeaf(leaf_node) || MergeRightLeaf(leaf_node));
  return leaf_node;
}

// Try Swap key between index_node's left sibling and index_node's parent.
bool BPlusTree::BorrowFromLeftIndexSibling(IndexNode* index_node) {
  if (index_node->left == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->left);
  if (sibling->parent != index_node->parent || sibling->count <= GetMinKeys()) {
    if (sibling->parent == index_node->parent) {
      assert(sibling->count == GetMinKeys());
    }
    UnMap(sibling);
    return false;
  }

  // 1.Insert parent'key to the first of index_node's keys.
  IndexNode* parent_node = Map<IndexNode>(index_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  index_node->InsertKeyAtIndex(0, parent_node->Key(index));

  // 2. Change parent's key.
  parent_node->UpdateKey(index, sibling->LastKey());

  // 3. Link sibling's last child to index_node,
  // and delete sibling's last child.
  Node* last_sibling_child =
      Map<Node>(sibling->indexes[sibling->count--].offset);
  index_node->indexes[0].offset = last_sibling_child->offset;
  last_sibling_child->parent = index_node->offset;

  UnMap(last_sibling_child);
  UnMap(parent_node);
  UnMap(sibling);
  return true;
}

bool BPlusTree::BorrowFromRightIndexSibling(IndexNode* index_node) {
  if (index_node->right == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->right);
  if (sibling->parent != index_node->parent || sibling->count <= GetMinKeys()) {
    if (sibling->parent == index_node->parent) {
      assert(sibling->count == GetMinKeys());
    }
    UnMap(sibling);
    return false;
  }

  // 1.Insert parent‘key to the last of index_node's keys.
  IndexNode* parent = Map<IndexNode>(index_node->parent);
  int index = UpperBound(parent->indexes, parent->count, sibling->LastKey());
  index_node->UpdateKey(index_node->count++, parent->Key(index - 1));

  // 2. Change parent's key.
  parent->UpdateKey(index - 1, sibling->FirstKey());

  // 3. Link index_node's last child to sibling's first child,
  // and delete sibling's first child.
  Node* first_sibling_child = Map<Node>(sibling->indexes[0].offset);
  index_node->indexes[index_node->count].offset = first_sibling_child->offset;
  first_sibling_child->parent = index_node->offset;
  sibling->DeleteKeyAtIndex(0);

  UnMap(first_sibling_child);
  UnMap(parent);
  UnMap(sibling);
  return true;
}

inline bool BPlusTree::BorrowFromIndexSibling(IndexNode* index_node) {
  assert(index_node->count == GetMinKeys() - 1);
  return BorrowFromLeftIndexSibling(index_node) ||
         BorrowFromRightIndexSibling(index_node);
}

// Try merge left index node.
bool BPlusTree::MergeLeftIndex(IndexNode* index_node) {
  if (index_node->left == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->left);
  if (sibling->parent != index_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Merge left sibling to index_node.
  index_node->MergeLeftSibling(sibling);

  // 2. Link sibling's childs to index_node.
  for (size_t i = 0; i < sibling->count + 1; ++i) {
    Node* child_node = Map<Node>(sibling->indexes[i].offset);
    child_node->parent = index_node->offset;
    UnMap(child_node);
  }

  // 3. Link new sibling.
  index_node->left = sibling->left;
  if (sibling->left != 0) {
    IndexNode* new_sibling = Map<IndexNode>(sibling->left);
    new_sibling->right = index_node->offset;
    UnMap(new_sibling);
  }

  // 4. Update index_node's mid key.
  IndexNode* parent_node = Map<IndexNode>(index_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  index_node->UpdateKey(sibling->count, parent_node->Key(index));

  // 5. Delete parent's key.
  parent_node->DeleteKeyAtIndex(index);

  Dealloc(sibling);
  return true;
}

// Try merge right index node.
bool BPlusTree::MergeRightIndex(IndexNode* index_node) {
  if (index_node->right == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->right);
  if (sibling->parent != index_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Update index_node's last key.
  IndexNode* parent = Map<IndexNode>(index_node->parent);
  int index = UpperBound(parent->indexes, parent->count, sibling->LastKey());
  index_node->UpdateKey(index_node->count++, parent->Key(index - 1));

  // 2. Merge right sibling to index_node.
  index_node->MergeRightSibling(sibling);

  // 3. Link sibling's childs to index_node.
  for (size_t i = 0; i < sibling->count + 1; ++i) {
    Node* child_node = Map<Node>(sibling->indexes[i].offset);
    child_node->parent = index_node->offset;
    UnMap(child_node);
  }

  // 4. Link new sibling.
  index_node->right = sibling->right;
  if (sibling->right != 0) {
    IndexNode* new_sibling = Map<IndexNode>(sibling->right);
    new_sibling->left = index_node->offset;
    UnMap(new_sibling);
  }

  // 5. Delete parent's key.
  parent->UpdateKey(index - 1, parent->Key(index));
  parent->DeleteKeyAtIndex(index);

  Dealloc(sibling);
  return true;
}

inline BPlusTree::IndexNode* BPlusTree::MergeIndex(IndexNode* index_node) {
  assert(index_node->count == GetMinKeys() - 1);
  assert(index_node->parent != 0);
  assert(meta_->root != index_node->offset);
  assert(MergeLeftIndex(index_node) || MergeRightIndex(index_node));
  return index_node;
}

#ifdef DEBUG
#include <queue>
void BPlusTree::Dump() {
  std::vector<std::vector<std::vector<std::string>>> res(
      5, std::vector<std::vector<std::string>>());
  std::queue<std::pair<off_t, int>> q;
  q.emplace(meta_->root, 1);
  while (!q.empty()) {
    auto cur = q.front();
    q.pop();
    if (cur.second < meta_->height) {
      IndexNode* index_node = Map<IndexNode>(cur.first);
      std::vector<std::string> v;
      for (int i = 0; i < index_node->count + 1; ++i) {
        if (i == index_node->count) {
          v.push_back("");
        } else {
          v.push_back(index_node->indexes[i].key);
        }
        if (index_node->indexes[i].offset != 0) {
          q.emplace(index_node->indexes[i].offset, cur.second + 1);
        }
      }
      res[cur.second].push_back(v);
      UnMap(index_node);
    } else {
      LeafNode* leaf_node = Map<LeafNode>(cur.first);
      std::vector<std::string> v;
      for (int i = 0; i < leaf_node->count; ++i) {
        v.push_back(leaf_node->records[i].key);
      }
      res[cur.second].push_back(v);
      UnMap(leaf_node);
    }
  }

  for (int i = 1; i <= meta_->height; ++i) {
    for (int j = 0; j < meta_->height - i; ++j) {
      LOG2("%s", "\t");
    }
    for (auto& v : res[i]) {
      for (auto& k : v) {
        LOG2("%s,", k.data());
      }
      LOG2("%s", "  ");
    }
    LOG2("%s", "\n");
  }
}
#endif