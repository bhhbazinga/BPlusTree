# BPlusTree
A B+ tree is an m-ary tree with a variable but often large number of children per node. A B+ tree consists of a root, internal nodes and leaves. The root may be either a leaf or a node with two or more children.\
The primary value of a B+ tree is in storing data for efficient retrieval in a block-oriented storage context — in particular, filesystems(In my repo, I use mmap.). This is primarily because unlike binary search trees, B+ trees have very high fanout (number of pointers to child nodes in a node, typically on the order of 100 or more), which reduces the number of I/O operations required to find an element in the tree.\
In theory, if the size of the index node in B+ tree is close to the size of the disk block(eg.4k bytes page size in linux), a query operation needs to access the disk logb(N) times.
## Feature
  * Use mmap to read and write to disk.
  * Use LRU to cache mapped blocks.
## Benchmark
  Magnitude     | Put         | Get        | Delete     |
  :-----------  | :-----------| :----------|:-----------|
  10K           | 72ms        | 28ms       | 52ms       |
  100K          | 900ms       | 507ms      | 777ms      |
  1000K         | 10333ms     | 4726ms     | 8154ms     |
  
The above data was tested on my 2013 macbook-pro with Intel Core i7 4 cores 2.3 GHz.\
Each record has a value length of 100 bytes and I set cache size to 50MB.
See also [test](test.cc).
## Build
```
make && ./test
```
## API
```C++
BPlusTree(const char* path);
void Put(const std::string& key, const std::string& value);
bool Delete(const std::string& key);
bool Get(const std::string& key, std::string& value) const;
std::vector<std::string> GetRange(const std::string& left, const std::string& right) const;
bool Empty() const;
size_t Size() const;
```
## TODO List
- [ ] Support for variable key-value length.
- [ ] When Deallc is executed,  put block into reuse-pool.
- [ ] Defragment db file.
## Reference
[1] https://en.wikipedia.org/wiki/B%2B_tree \
[2] https://www.cnblogs.com/nullzx/p/8729425.html \
[3] http://man7.org/linux/man-pages/man2/mmap.2.html
