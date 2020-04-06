#include "bplus_tree.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>

const off_t kMetaOffset = 0;
const int kOrder = 3;
static_assert(kOrder >= 3,
              "The order of B+Tree should be greater than or equal to 3.");
const int kMaxKeySize = 32;
const int kMaxValueSize = 256;
typedef char Key[kMaxKeySize];
typedef char Value[kMaxValueSize];

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

  void Assign(off_t of, const char* k) {
    offset = of;
    strncpy(key, k, kMaxKeySize);
  }
  void UpdateKey(const char* k) { strncpy(key, k, kMaxKeySize); }
};

struct BPlusTree::Record {
  Key key;
  Value value;

  void Assign(const char* k, const char* v) {
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

  size_t AssertCount() {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
      if (std::strcmp(indexes[i].key, "") != 0) {
        ++cnt;
      }
    }
    return cnt;
  }

  const char* FirstKey() const { return count == 0 ? "" : indexes[0].key; }
  const char* LastKey() const {
    return count == 0 ? "" : indexes[count - 1].key;
  }
  const char* Key(int index) const {
    return count == 0 ? "" : indexes[index].key;
  }

  Index indexes[kOrder + 1];
};

struct BPlusTree::LeafNode : BPlusTree::Node {
  LeafNode() = default;
  ~LeafNode() = default;

  size_t AssertCount() {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
      if (strcmp(records[i].key, "") != 0) {
        ++cnt;
      }
    }
    return cnt;
  }

  const char* FirstKey() const { return count == 0 ? "" : records[0].key; }

  const char* LastKey() const {
    return count == 0 ? "" : records[count - 1].key;
  }

  const char* Key(int index) const {
    return count == 0 ? "" : records[index].key;
  }

  const char* FirstValue() const { return count == 0 ? "" : records[0].value; }

  const char* LastValue() const {
    return count == 0 ? "" : records[count - 1].value;
  }

  const char* Value(int index) const {
    return count == 0 ? "" : records[index].value;
  }

  BPlusTree::Record records[kOrder];
};

BPlusTree::BPlusTree(const char* path)
    : fd_(open(path, O_CREAT | O_RDWR, 0600)) {
  if (fd_ == -1) {
    Exit("open");
  }
  meta_ = Map<Meta>(kMetaOffset);
  if (meta_->height == 0) {
    // Initialize B+tree;
    constexpr off_t of_root = kMetaOffset + sizeof(Meta);
    LeafNode* root = new (Map<LeafNode>(of_root)) LeafNode();
    root->offset = of_root;
    meta_->height = 1;
    meta_->root = of_root;
    meta_->block = of_root + sizeof(LeafNode);
    // log("meta_->next_block=%d,meta_->root=%d,\n", meta_->next_block,
    //     meta_->root);
    UnMap<LeafNode>(root);
  }
}

BPlusTree::~BPlusTree() {
  UnMap(meta_);
  close(fd_);
}

void BPlusTree::Put(const std::string& key, const std::string& value) {
  // log("Put:key=%s,value=%s\n", key.data(), value.data());
  // 1. Find Leaf node.
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  if (InsertKeyIntoLeafNode(leaf_node, key.data(), value.data()) <=
      GetMaxKeys()) {
    // 2.If records of leaf node less than or equals kOrder - 1 then finish.
    UnMap<LeafNode>(leaf_node);
    return;
  }

  // 3. Split leaf node to two leaf nodes.
  LeafNode* split_node = SplitLeafNode(leaf_node);
  const char* mid_key = split_node->records[0].key;
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
    const char* mid_key = child_node->indexes[child_node->count].key;
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
  // log("-------------------------------------Delete:key=%s\n", key.data());
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  // log("leaf_node->count=%d,leaf_node->left=%d,leaf_node->right=%d,leaf_node->"
  //     "parent=%d,leaf_node->offset=%d\n",
  //     leaf_node->count, leaf_node->left, leaf_node->right, leaf_node->parent,
  //     leaf_node->offset);

  // LeafNode* test = Map<LeafNode>(leaf_node->right);
  // log("test->count=%d,test->left=%d,test->right=%d,test->parent=%d,test->"
  //     "offset=%d\n",
  //     test->count, test->left, test->right, test->parent, test->offset);

  // 1. Delete key from leaf node
  int index = GetIndexFromLeafNode(leaf_node, key.data());
  if (index == -1) {
    UnMap(leaf_node);
    return false;
  }
  memmove(&leaf_node->records[index], &leaf_node->records[index + 1],
          sizeof(leaf_node->records[0]) * (--leaf_node->count - index));

  // 2. If count of leaf node >= GetMinKeys() then return else execute step 3.
  // 3. If count of one of sibling > GetMinKeys() then borrow from it else
  // execute step 4.
  if (leaf_node->parent == 0 || leaf_node->count >= GetMinKeys() ||
      BorrowKeyFromSibling(leaf_node)) {
    UnMap<LeafNode>(leaf_node);
    return true;
  }

  // 4. Merge two leaf nodes.
  leaf_node = MergeLeafNode(leaf_node);

  // 5. If count of index_node >= GetMinKeys() then return or execute 6.
  // 6. If count of one of sibling > GetMinKeys() then swap its key and parent's
  // key then return or execute 7.
  IndexNode* index_node = Map<IndexNode>(leaf_node->parent);
  UnMap<LeafNode>(leaf_node);

  while (index_node->parent != 0 && index_node->count < GetMinKeys() &&
         !SwapKeyBetweenParentAndSibling(index_node)) {
    // 7. Merge index_node and its' parent and sibling.
    IndexNode* old_index_node = MergeIndexNode(index_node);
    index_node = Map<IndexNode>(old_index_node->parent);
    UnMap(old_index_node);
  }

  if (index_node->parent == 0 && index_node->count < 1) {
    log("%s", ">>>>>>>>>>>>>\n");
    // 8. Root is removed, update new root and height.
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
  value = leaf_node->records[index].value;
  UnMap<LeafNode>(leaf_node);
  return true;
}

template <typename T>
T* BPlusTree::Map(off_t offset) const {
  struct stat st;
  if (fstat(fd_, &st) != 0) Exit("fstat");
  constexpr int size = sizeof(T);
  if (st.st_size < offset + size && ftruncate(fd_, offset + size) != 0) {
    Exit("ftruncate");
  }
  // Align offset to page size.
  // See http://man7.org/linux/man-pages/man2/mmap.2.html
  off_t page_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
  void* addr = mmap(nullptr, size + offset - page_offset,
                    PROT_READ | PROT_WRITE, MAP_SHARED, fd_, page_offset);
  // log("mmap:fd=%d,addr=%p,map_size=%d,page_offset=%d,offset=%d\n", fd_, addr,
  //     size + offset - page_offset, page_offset, offset);
  if (MAP_FAILED == addr) Exit("mmap");
  char* start = static_cast<char*>(addr);
  return reinterpret_cast<T*>(&start[offset - page_offset]);
}

template <typename T>
void BPlusTree::UnMap(T* map_obj) const {
  off_t page_offset = map_obj->offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
  char* start = reinterpret_cast<char*>(map_obj);
  void* addr = static_cast<void*>(&start[page_offset - map_obj->offset]);
  // log("munmap:addr=%p\n", addr);
  if (munmap(addr, sizeof(T)) != 0) Exit("munmap");
}

void BPlusTree::Exit(const char* msg) const {
  perror(msg);
  exit(EXIT_FAILURE);
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
    // log("meta_->root=%d,meta_->height=%d\n", meta_->root, meta_->height);
    return parent_node;
  }
  return Map<IndexNode>(node->parent);
}

template <typename T>
int BPlusTree::UpperBound(T arr[], int n, const char* key) const {
  // assert(n <= GetMaxKeys());
  int l = 0, r = n - 1;
  // log("target=%s,mid=%d\n", target.data(), mid);
  // log("l=%d,r=%d\n", l, r);
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
  // assert(n <= GetMaxKeys());
  int l = 0, r = n - 1;
  // log("target=%s,mid=%d\n", target.data(), mid);
  // log("l=%d,r=%d\n", l, r);
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
  // log("meta_->next_block=%d\n", meta_->next_block);
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
  IndexNode* inter_node = Map<IndexNode>(offset);
  while (--height > 1) {
    int index = UpperBound(inter_node->indexes, inter_node->count, key);
    off_t of_child = inter_node->indexes[index].offset;
    UnMap(inter_node);
    inter_node = Map<IndexNode>(of_child);
    offset = of_child;
  }
  // 2. Get offset of leaf node.
  int index = UpperBound(inter_node->indexes, inter_node->count, key);
  off_t of_child = inter_node->indexes[index].offset;
  UnMap<IndexNode>(inter_node);
  return of_child;
}

inline size_t BPlusTree::InsertKeyIntoIndexNode(IndexNode* index_node,
                                                const char* key,
                                                Node* left_node,
                                                Node* right_node) {
  // assert(index_node->AssertCount() == count);
  // assert(count <= GetMaxKeys());
  int index = UpperBound(index_node->indexes, index_node->count, key);
  std::memmove(
      &index_node->indexes[index + 1], &index_node->indexes[index],
      sizeof(index_node->indexes[0]) * (index_node->count - index + 1));
  // log("index=%d,of_left=%d, key=%s\n", index, of_left, key.data());
  index_node->indexes[index].Assign(left_node->offset, key);
  index_node->indexes[index + 1].offset = right_node->offset;
  return ++index_node->count;
}

size_t BPlusTree::InsertKeyIntoLeafNode(LeafNode* leaf_node, const char* key,
                                        const char* value) {
  // assert(count <= GetMaxKeys());
  // assert(leaf_node->AssertCount() == count);
  int index = UpperBound(leaf_node->records, leaf_node->count, key);
  if (index > 0 &&
      std::strncmp(leaf_node->records[index - 1].key, key, kMaxKeySize) == 0) {
    // log("index=%d,value=%s\n", index, value.data());
    leaf_node->records[index - 1].UpdateValue(value);
    return leaf_node->count;
  }
  std::memmove(&leaf_node->records[index + 1], &leaf_node->records[index],
               sizeof(leaf_node->records[0]) * (leaf_node->count - index));
  leaf_node->records[index].Assign(key, value);
  // assert(leaf_node->AssertCount() == count);
  return ++leaf_node->count;
}

BPlusTree::IndexNode* BPlusTree::SplitIndexNode(IndexNode* index_node) {
  // assert(index_node->count == kOrder);
  // assert(index_node->AssertCount() == index_node->count);
  constexpr int mid = (kOrder - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder - mid - 1;
  IndexNode* split_node = Alloc<IndexNode>();
  split_node->count = right_count;
  split_node->left = index_node->offset;
  split_node->right = 0;
  std::memcpy(&split_node->indexes[0], &index_node->indexes[mid + 1],
              sizeof(split_node->indexes[0]) * (right_count + 1));
  for (int i = mid + 1; i <= kOrder; ++i) {
    // Link old childs to new splited parent.
    off_t of_child = index_node->indexes[i].offset;
    LeafNode* child_node = Map<LeafNode>(of_child);
    child_node->parent = split_node->offset;
    UnMap<LeafNode>(child_node);
  }
  index_node->count = left_count;
  index_node->right = split_node->offset;
  // assert(index_node->AssertCount() == index_node->count);
  // assert(split_node->AssertCount() == split_node->count);
  return split_node;
}

BPlusTree::LeafNode* BPlusTree::SplitLeafNode(LeafNode* leaf_node) {
  // assert(leaf_node->count == kOrder);
  // assert(leaf_node->AssertCount() == leaf_node->count);
  constexpr int mid = (kOrder - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder - mid;
  // Splited right node contains the original record of elements
  // m through kOrder - 1.
  LeafNode* split_node = Alloc<LeafNode>();
  split_node->count = right_count;
  split_node->left = leaf_node->offset;
  split_node->right = 0;
  std::memcpy(&split_node->records[0], &leaf_node->records[mid],
              sizeof(split_node->records[0]) * right_count);
  // Left node maintains the original record of
  // elements 0 through m - 1.
  leaf_node->count = left_count;
  leaf_node->right = split_node->offset;
  // assert(leaf_node->AssertCount() == leaf_node->count);
  // assert(split_node->AssertCount() == split_node->count);
  return split_node;
}

inline int BPlusTree::GetIndexFromLeafNode(LeafNode* leaf_node,
                                           const char* key) const {
  int index = LowerBound(leaf_node->records, leaf_node->count, key);
  return index < static_cast<int>(leaf_node->count) &&
                 std::strncmp(leaf_node->records[index].key, key,
                              kMaxKeySize) == 0
             ? index
             : -1;
}

// std::vector<std::string> BPlusTree::GetRange(const std::string& left_key,
//                                              const std::string& right_key) {
//   off_t of_leaf = GetLeafOffset(left_key);
//   off_t of_leaf = GetLeafOffset(left_key);
//   std::vector<std::string> res;
//   res.push_back("aaa");
//   return res;
// }

inline bool BPlusTree::Empty() const { return Size() == 0; }

inline size_t BPlusTree::Size() const { return meta_->size; }

// Try Borrow key from left sibling.
bool BPlusTree::BorrowKeyFromLeftSibling(LeafNode* leaf_node) {
  if (leaf_node->left == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->left);
  if (sibling->parent != leaf_node->parent || sibling->count <= GetMinKeys()) {
    assert(sibling->count == GetMinKeys());
    UnMap(sibling);
    return false;
  }
  // Borrow last key from left sibling.
  const char* borrow_key = sibling->LastKey();
  std::memmove(&leaf_node->records[1], &leaf_node->records[0],
               sizeof(leaf_node->records[0]) * leaf_node->count++);
  leaf_node->records[0].Assign(borrow_key, sibling->LastValue());

  // Replace parent's key with borrow key.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index = UpperBound(parent_node->indexes, parent_node->count, borrow_key);
  parent_node->indexes[index].UpdateKey(borrow_key);
  UnMap<IndexNode>(parent_node);
  UnMap<LeafNode>(sibling);
  return true;
}

// Try Borrow key from right sibling.
bool BPlusTree::BorrowKeyFromRightSibling(LeafNode* leaf_node) {
  if (leaf_node->right == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->right);
  if (sibling->parent != leaf_node->parent || sibling->count <= GetMinKeys()) {
    assert(sibling->count == GetMinKeys());
    UnMap(sibling);
    return false;
  }

  // Borrow frist key from right sibling.
  const char* borrow_key = sibling->FirstKey();
  leaf_node->records[leaf_node->count++].Assign(borrow_key,
                                                sibling->FirstKey());
  std::memmove(&sibling->records[0], &sibling->records[1],
               sizeof(sibling->records[0]) * (--sibling->count));
  // Replace parent's key with sibling's first key.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index = UpperBound(parent_node->indexes, parent_node->count,
                         leaf_node->records[leaf_node->count - 2].key);
  parent_node->indexes[index].UpdateKey(sibling->FirstKey());
  UnMap<IndexNode>(parent_node);
  UnMap<LeafNode>(sibling);
  return true;
}

inline bool BPlusTree::BorrowKeyFromSibling(LeafNode* leaf_node) {
  assert(leaf_node->count == GetMinKeys() - 1);
  assert(leaf_node->parent != 0);
  return BorrowKeyFromLeftSibling(leaf_node) ||
         BorrowKeyFromRightSibling(leaf_node);
}

// Try merge left leaf node.
bool BPlusTree::MergeLeftLeafNode(LeafNode* leaf_node) {
  if (leaf_node->left == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->left);
  if (sibling->parent != leaf_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Delete key from parent node and link parent to leaf_node.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  std::memmove(
      &parent_node->indexes[index], &parent_node->indexes[index + 1],
      sizeof(&parent_node->indexes[0]) * (parent_node->count-- - index));
  UnMap(parent_node);

  // 2. Merge sibling to leaf_node.
  std::memmove(&leaf_node->records[sibling->count], &leaf_node->records[0],
               sizeof(leaf_node->records[0]) * leaf_node->count);
  std::memcpy(&leaf_node->records[0], &sibling->records[0],
              sizeof(leaf_node->records[0]) * sibling->count);
  leaf_node->count += sibling->count;

  // 3. Link new sibling.
  leaf_node->left = sibling->left;
  if (sibling->left != 0) {
    LeafNode* new_sibling = Map<LeafNode>(sibling->left);
    new_sibling->right = leaf_node->offset;
    UnMap(new_sibling);
  }

  Dealloc(sibling);
  return true;
}

// Try Merge right node.
bool BPlusTree::MergeRightLeafNode(LeafNode* leaf_node) {
  if (leaf_node->right == 0) return false;
  LeafNode* sibling = Map<LeafNode>(leaf_node->right);
  if (sibling->parent != leaf_node->parent) {
    UnMap(sibling);
    return false;
  }

  // 1. Delete key from parent node and link parent to leaf_node.
  IndexNode* parent_node = Map<IndexNode>(leaf_node->parent);
  int index = UpperBound(parent_node->indexes, parent_node->count,
                         leaf_node->LastKey());
  std::memmove(
      &parent_node->indexes[index], &parent_node->indexes[index + 1],
      sizeof(&parent_node->indexes[0]) * (parent_node->count-- - index));
  parent_node->indexes[index].offset = leaf_node->offset;
  UnMap(parent_node);

  // 2. Merge sibling to leaf_node.
  std::memcpy(&leaf_node->records[leaf_node->count], &sibling->records[0],
              sizeof(leaf_node->records[0]) * sibling->count);
  leaf_node->count += sibling->count;

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

inline BPlusTree::LeafNode* BPlusTree::MergeLeafNode(LeafNode* leaf_node) {
  // Merge left node to leaf_node or right node to leaf_node.
  assert(leaf_node->count == GetMinKeys() - 1);
  assert(leaf_node->parent != 0);
  assert(MergeLeftLeafNode(leaf_node) || MergeRightLeafNode(leaf_node));
  return leaf_node;
}

// Try Swap key between index_node's left sibling and index_node's parent.
bool BPlusTree::SwapKeyBetweenParentAndLeftSibling(IndexNode* index_node) {
  if (index_node->left == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->left);
  if (sibling->parent != index_node->parent || sibling->count <= GetMinKeys()) {
    assert(sibling->count == GetMinKeys());
    UnMap(sibling);
    return false;
  }

  // log("%s", "111111111111111111111111\n");
  // 1.Insert parent key to first index of indexes of index_node.
  IndexNode* parent_node = Map<IndexNode>(index_node->parent);
  int index =
      UpperBound(parent_node->indexes, parent_node->count, sibling->LastKey());
  std::memmove(&index_node->indexes[1], &index_node->indexes[0],
               sizeof(index_node->indexes[0]) * (++index_node->count));
  index_node->indexes[0].UpdateKey(parent_node->Key(index));

  // 2. Change parent's key.
  parent_node->indexes[index].UpdateKey(sibling->LastKey());

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

bool BPlusTree::SwapKeyBetweenParentAndRightSibling(IndexNode* index_node) {
  if (index_node->right == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->right);
  if (sibling->parent != index_node->parent || sibling->count <= GetMinKeys()) {
    assert(sibling->count == GetMinKeys());
    UnMap(sibling);
    return false;
  }

  log("%s", "22222222222222222222222222\n");
  // 1.Insert parent key to last index of indexes of index_node.
  IndexNode* parent = Map<IndexNode>(index_node->parent);
  int index = UpperBound(parent->indexes, parent->count, index_node->LastKey());
  index_node->indexes[index_node->count++].UpdateKey(parent->Key(index));

  // 2. Change parent's key.
  parent->indexes[index].UpdateKey(sibling->FirstKey());

  // 3. Link index_node's last child to sibling's first child,
  // and delete sibling's first child.
  Node* first_sibling_child = Map<Node>(sibling->indexes[0].offset);
  index_node->indexes[index_node->count].offset = first_sibling_child->offset;
  first_sibling_child->parent = index_node->offset;
  std::memmove(&sibling->indexes[0], &sibling->indexes[1],
               sizeof(sibling->indexes[0]) * sibling->count--);

  UnMap(first_sibling_child);
  UnMap(parent);
  UnMap(sibling);
  return true;
}

inline bool BPlusTree::SwapKeyBetweenParentAndSibling(IndexNode* index_node) {
  assert(index_node->count == GetMinKeys() - 1);
  return SwapKeyBetweenParentAndLeftSibling(index_node) ||
         SwapKeyBetweenParentAndRightSibling(index_node);
}

// Try merge left index node.
bool BPlusTree::MergeLeftIndexNode(IndexNode* index_node) {
  if (index_node->left == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->left);
  if (sibling->parent != index_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Insert parent's key to index_node.
  IndexNode* parent = Map<IndexNode>(index_node->parent);
  int index = UpperBound(parent->indexes, parent->count, sibling->LastKey());
  std::memmove(&index_node->indexes[sibling->count + 1],
               &index_node->indexes[0],
               sizeof(index_node->indexes[0]) * (index_node->count + 1));
  index_node->indexes[sibling->count].UpdateKey(parent->Key(index));

  // 2. Delete parent's key and link parent to index_node.
  std::memmove(&parent->indexes[index], &parent->indexes[index + 1],
               sizeof(parent->indexes[0]) * (parent->count-- - index));
  parent->indexes[index].offset = index_node->offset;

  // 3. Link new sibling.
  index_node->left = sibling->left;
  if (sibling->left != 0) {
    IndexNode* new_sibling = Map<IndexNode>(sibling->left);
    new_sibling->right = index_node->offset;
    UnMap(new_sibling);
  }

  // 4. Merge sibling's indexes to index_node.
  std::memcpy(&index_node->indexes[0], &sibling->indexes[0],
              sizeof(index_node->indexes[0]) * sibling->count);
  index_node->count += (sibling->count + 1);
  index_node->indexes[sibling->count].offset =
      sibling->indexes[sibling->count].offset;

  // 5. Link sibling's childs to index_node.
  for (size_t i = 0; i < sibling->count + 1; ++i) {
    Node* child_node = Map<Node>(sibling->indexes[i].offset);
    child_node->parent = index_node->offset;
    UnMap(child_node);
  }
  Dealloc(sibling);
  return true;
}

// Try merge right index node.
bool BPlusTree::MergeRightIndexNode(IndexNode* index_node) {
  if (index_node->right == 0) return false;
  IndexNode* sibling = Map<IndexNode>(index_node->right);
  if (sibling->parent != index_node->parent) {
    UnMap(sibling);
    return false;
  }

  assert(sibling->count == GetMinKeys());
  // 1. Insert parent's key to index_node.
  IndexNode* parent = Map<IndexNode>(index_node->parent);
  int index = UpperBound(parent->indexes, parent->count, index_node->LastKey());
  index_node->indexes[index_node->count].UpdateKey(parent->Key(index));

  // 2. Delete parent's key and link parent to index_node.
  std::memmove(&parent->indexes[index], &parent->indexes[index + 1],
               sizeof(parent->indexes[0]) * (parent->count-- - index));
  parent->indexes[index].offset = index_node->offset;

  // 3. Link new sibling.
  index_node->right = sibling->right;
  if (sibling->right != 0) {
    IndexNode* new_sibling = Map<IndexNode>(sibling->right);
    new_sibling->left = index_node->offset;
    UnMap(new_sibling);
  }

  // 4. Merge sibling's indexes to index_node.
  std::memcpy(&index_node->indexes[index_node->count + 1], &sibling->indexes[0],
              sizeof(index_node->indexes[0]) * (sibling->count + 1));
  index_node->count += (sibling->count + 1);

  // 5. Link sibling's childs to index_node.
  for (size_t i = 0; i < sibling->count + 1; ++i) {
    Node* child_node = Map<Node>(sibling->indexes[i].offset);
    child_node->parent = index_node->offset;
    UnMap(child_node);
  }
  Dealloc(sibling);
  return true;
}

inline BPlusTree::IndexNode* BPlusTree::MergeIndexNode(IndexNode* index_node) {
  assert(index_node->count == GetMinKeys() - 1);
  assert(index_node->parent != 0);
  assert(meta_->root != index_node->offset);
  assert(MergeLeftIndexNode(index_node) || MergeRightIndexNode(index_node));
  return index_node;
}
