#include "bplus_tree.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>

const off_t kMetaOffset = 0;
const int kOrder = 128;
static_assert(kOrder >= 3,
              "The order of B+Tree should be greater than or equal to 3.");
const int kMaxKeySize = 32;
const int kMaxValueSize = 256;
typedef char Key[kMaxKeySize];
typedef char Value[kMaxValueSize];

struct BPlusTree::Meta {
  size_t self;    // ofset of self
  size_t height;  // height of B+Tree
  size_t size;    // key size
  off_t root;     // offset of root
  off_t block;    // offset of next new node
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

  off_t self;    // offset of self
  off_t parent;  // offset of parent
  off_t left;    // offset of left node(may be sibling)
  off_t right;   // offset of right node(may be sibling)
  size_t count;  // child count or record count
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
    root->self = of_root;
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
  off_t of_split = leaf_node->right;
  split_node->parent = of_parent;

  // 4.Insert key to parent of splited leaf nodes and
  // link two splited left nodes to parent.
  if (InsertKeyIntoInternalNode(parent_node, mid_key, of_leaf, of_split) <=
      GetMaxKeys()) {
    UnMap<LeafNode>(leaf_node);
    UnMap<LeafNode>(split_node);
    UnMap<IndexNode>(parent_node);
    return;
  }

  // 5.Split internal node from bottom to up repeatedly
  // until count <= kOrder - 1.
  size_t count;
  do {
    IndexNode* old_parent_node = parent_node;
    off_t old_of_parent = of_parent;
    IndexNode* split_node = SplitInternalNode(old_parent_node);
    const char* mid_key = old_parent_node->indexes[old_parent_node->count].key;
    parent_node = GetOrCreateParent(old_parent_node);
    of_parent = old_parent_node->parent;
    of_split = old_parent_node->right;
    split_node->parent = of_parent;
    count = InsertKeyIntoInternalNode(parent_node, mid_key, old_of_parent,
                                      of_split);
    UnMap<IndexNode>(old_parent_node);
  } while (count > GetMaxKeys());
  UnMap<IndexNode>(parent_node);
}

bool BPlusTree::Delete(const std::string& key) {
  off_t of_leaf = GetLeafOffset(key.data());
  LeafNode* leaf_node = Map<LeafNode>(of_leaf);
  bool found;
  size_t count = DeleteKeyFromLeafNode(leaf_node, key.data(), found);
  if (!found) {
    UnMap<LeafNode>(leaf_node);
    return false;
  }

  // do {
  //   if (count >= GetMinKeys()) break;
  //   if (BorrowKeyFromLeafSibling(leaf_node)) break;
  // } while (0);
  // UnMap<LeafNode>(leaf_node);
  // return true;

  if (count >= GetMinKeys()) {
    UnMap<LeafNode>(leaf_node);
    return true;
  }
  if (BorrowKeyFromLeafSibling(leaf_node)) {
    UnMap<LeafNode>(leaf_node);
    return true;
  }
  MergeLeafNode(leaf_node);

  UnMap<LeafNode>(leaf_node);
  return true;
}

bool BPlusTree::Get(const std::string& key, std::string& value) {
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
T* BPlusTree::Map(off_t offset) {
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
void BPlusTree::UnMap(T* map_obj) {
  off_t page_offset = map_obj->self & ~(sysconf(_SC_PAGE_SIZE) - 1);
  char* start = reinterpret_cast<char*>(map_obj);
  void* addr = static_cast<void*>(&start[page_offset - map_obj->self]);
  // log("munmap:addr=%p\n", addr);
  if (munmap(addr, sizeof(T)) != 0) Exit("munmap");
}

constexpr size_t BPlusTree::GetMinKeys() const { return (kOrder + 1) / 2 - 1; }

constexpr size_t BPlusTree::GetMaxKeys() const { return kOrder - 1; }

BPlusTree::IndexNode* BPlusTree::GetOrCreateParent(Node* node) {
  if (node->parent == 0) {
    // Split root node.
    IndexNode* parent_node = Alloc<IndexNode>();
    node->parent = parent_node->self;
    meta_->root = parent_node->self;
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
  node->self = meta_->block;
  meta_->block += sizeof(T);
  return node;
}

template <typename T>
void BPlusTree::Dealloc(T* node) {
  UnMap<T>(node);
}

off_t BPlusTree::GetLeafOffset(const char* key) {
  size_t height = meta_->height;
  off_t offset = meta_->root;
  if (height == 1) return offset;

  // 1. Find bottom internal node.
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

size_t BPlusTree::InsertKeyIntoInternalNode(IndexNode* internal_node,
                                            const char* key, off_t of_left,
                                            off_t of_right) {
  // assert(internal_node->AssertCount() == count);
  // assert(count <= GetMaxKeys());
  int index = UpperBound(internal_node->indexes, internal_node->count, key);
  std::memmove(
      &internal_node->indexes[index + 1], &internal_node->indexes[index],
      sizeof(internal_node->indexes[0]) * (internal_node->count - index + 1));
  // log("index=%d,of_left=%d, key=%s\n", index, of_left, key.data());
  internal_node->indexes[index].Assign(of_left, key);
  internal_node->indexes[index + 1].offset = of_right;
  // assert(internal_node->AssertCount() == count);
  // if (internal_node->AssertCount() != count) {
  //   for (int i = 0; i < count; ++i) {
  //     log("%s  ", childs[i].key);
  //   }
  //   log("%s", "\n");
  // }
  return ++internal_node->count;
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

BPlusTree::IndexNode* BPlusTree::SplitInternalNode(
    IndexNode* internal_node) {
  // assert(internal_node->count == kOrder);
  // assert(internal_node->AssertCount() == internal_node->count);
  constexpr int mid = (kOrder - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder - mid - 1;
  IndexNode* split_node = Alloc<IndexNode>();
  split_node->count = right_count;
  split_node->left = internal_node->self;
  split_node->right = 0;
  std::memcpy(&split_node->indexes[0], &internal_node->indexes[mid + 1],
              (right_count + 1) * sizeof(split_node->indexes[0]));
  for (int i = mid + 1; i <= kOrder; ++i) {
    // Link old childs to new splited parent.
    off_t of_child = internal_node->indexes[i].offset;
    LeafNode* child_node = Map<LeafNode>(of_child);
    child_node->parent = split_node->self;
    UnMap<LeafNode>(child_node);
  }
  internal_node->count = left_count;
  internal_node->right = split_node->self;
  // assert(internal_node->AssertCount() == internal_node->count);
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
  split_node->left = leaf_node->self;
  split_node->right = 0;
  std::memcpy(&split_node->records[0], &leaf_node->records[mid],
              right_count * sizeof(split_node->records[0]));
  // Left node maintains the original record of
  // elements 0 through m - 1.
  leaf_node->count = left_count;
  leaf_node->right = split_node->self;
  // assert(leaf_node->AssertCount() == leaf_node->count);
  // assert(split_node->AssertCount() == split_node->count);
  return split_node;
}

int BPlusTree::GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) {
  int index = LowerBound(leaf_node->records, leaf_node->count, key);
  return index < leaf_node->count && std::strncmp(leaf_node->records[index].key,
                                                  key, kMaxKeySize) == 0
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

inline bool BPlusTree::Empty() const { return get_size() == 0; }

size_t BPlusTree::get_size() const { return meta_->size; }

#include <queue>
void BPlusTree::Dump() {
  // size_t height = meta_->height;
  // InternalNode* inter_node = Map<InternalNode>(meta_->root);
  // std::queue<InternalNode*> q;
  // q.push(inter_node);
  // while (!q.empty()) {
  //   InternalNode* cur = q.front();
  //   q.pop();
  //   for (int i = 0; i < cur->count; ++i) {
  //   }
  // }
}

size_t BPlusTree::DeleteKeyFromLeafNode(LeafNode* leaf_node, const char* key,
                                        bool& found) {
  int index = GetIndexFromLeafNode(leaf_node, key);
  if (index == -1) {
    found = false;
    return 0;
  }

  found = true;
  memmove(&leaf_node->records[index], &leaf_node->records[index + 1],
          sizeof(leaf_node->records[0]) * (leaf_node->count - index - 1));
  return --leaf_node->count;
}

size_t BPlusTree::DeleteKeyFromInternalNode(LeafNode* internal_node,
                                            const char* key, bool& found) {
  return 0;
}

bool BPlusTree::BorrowKeyFromLeafSibling(LeafNode* leaf_node) {
  // Borrow key from left sibling.
  if (leaf_node->left != 0) {
    LeafNode* sibling = Map<LeafNode>(leaf_node->left);
    if (sibling->parent == leaf_node->parent && sibling->count > GetMinKeys()) {
      // Borrow rightmost key from left sibling.
      const char* borrow_key = sibling->records[--sibling->count].key;
      std::memmove(&leaf_node->records[1], &leaf_node->records[0],
                   sizeof(leaf_node->records[0]) * leaf_node->count++);
      leaf_node->records[0].UpdateKey(borrow_key);
      // Replace parent's key with borrow key.
      ReplaceKeyInInternalNode(leaf_node->parent, leaf_node->records[1].key,
                               borrow_key);
      UnMap<LeafNode>(sibling);
      return true;
    }
  } else {
    assert(leaf_node->right != 0);
    // Borrow key from right sibling.
    LeafNode* sibling = Map<LeafNode>(leaf_node->right);
    if (sibling->parent == leaf_node->parent && sibling->count > GetMinKeys()) {
      // Borrow leftmost key from right sibling.
      const char* borrow_key = sibling->records[0].key;
      std::memmove(&sibling->records[0], &sibling->records[1],
                   sizeof(sibling->records[0]) * (--sibling->count));
      leaf_node->records[leaf_node->count++].UpdateKey(borrow_key);
      // Replace parent's key with borrow key.
      ReplaceKeyInInternalNode(leaf_node->parent, borrow_key,
                               sibling->records[0].key);
      UnMap<LeafNode>(sibling);
      return true;
    }
  }
  return false;
}

void BPlusTree::ReplaceKeyInInternalNode(off_t of_internal, const char* old_key,
                                         const char* new_key) {
  IndexNode* internal_node = Map<IndexNode>(of_internal);
  int index = LowerBound(internal_node->indexes, internal_node->count, old_key);
  assert(index < internal_node->count &&
         std::strncmp(internal_node->indexes[index].key, old_key, kMaxKeySize) ==
             0);
  internal_node->indexes[index].UpdateKey(new_key);
  UnMap<IndexNode>(internal_node);
}

void BPlusTree::MergeLeafNode(LeafNode* leaf_node) {
  LeafNode* merge_node = Alloc<LeafNode>();
  LeafNode* sibling;
  bool merge_left = leaf_node->left != 0;
  sibling = merge_left ? Map<LeafNode>(leaf_node->left)
                       : Map<LeafNode>(leaf_node->right);
  assert(sibling->count <= GetMinKeys() && leaf_node->count <= GetMinKeys());
  std::memcpy(&leaf_node->records[leaf_node->count], &sibling->records[0],
              sibling->count);
  leaf_node->count += sibling->count;
  if (merge_left) {
    leaf_node->left = sibling->left;
  } else {
    leaf_node->right = sibling->right;
  }
  Dealloc(sibling);
}
