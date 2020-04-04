#include <iostream>

#include "bplus_tree.h"

void Test(const std::string& s1, const std::string& s2) {
  log("%s,%s\n", s1.data(), s2.data());
}

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;

  BPlusTree bpt("test.db");
  bpt.Put("k1", "v1");

  std::string test;
  assert(bpt.Get("k1", test));

  for (int i = 0; i < 10000; ++i) {
    char k[32];
    char v[32];
    snprintf(k, 32, "k%d", i);
    snprintf(v, 32, "v%d", i);
    bpt.Put(k, v);

    std::string test;
    assert(bpt.Get(k, test));
    assert(test == v);
  }

  // std::string test;
  // assert(!bpt.Get("asd2", test));

  return 0;
}
