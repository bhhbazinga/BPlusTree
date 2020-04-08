# BPlusTree
A B+ tree is an m-ary tree with a variable but often large number of children per node. A B+ tree consists of a root, internal nodes and leaves. The root may be either a leaf or a node with two or more children.\
The primary value of a B+ tree is in storing data for efficient retrieval in a block-oriented storage context — in particular, filesystems(In our repo, we use mmap.). This is primarily because unlike binary search trees, B+ trees have very high fanout (number of pointers to child nodes in a node, typically on the order of 100 or more), which reduces the number of I/O operations required to find an element in the tree.\
In theory, if the size of the index node in the B+ tree is close to the size of the disk block, a query operation needs to access the disk logb(N) times.
## Feature
  * Use mmap to read and write to disk.
  * Use LRU to cache mapped blocks.
## Benchmark
  Magnitude     | Put         | Get        | Delete     |
  :-----------  | :-----------| :----------|:-----------|
  10K           | 7.8ms       | 1.6ms      | 2.1ms      |
  100K          | 74.9ms      | 13.4ms     | 18.4ms     |
  1000K         | 961ms       | 117.8ms    | 216.1ms    |
  
The above data was tested on my 2013 macbook-pro with Intel Core i7 4 cores 2.3 GHz.
See also [test](test.cc).
## Build
```
make && ./test
```
## API
```C++
```
## TODO List
- [ ] Use LRU to cache mapped blocks.
## Reference
[1] [B+ tree-Wikipedia](https://en.wikipedia.org/wiki/B%2B_tree)
