#include <iostream>

#include "bplus_tree.h"

void Test(const std::string& s1, const std::string& s2) {
  log("%s,%s\n", s1.data(), s2.data());
}

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;

  BPlusTree bpt("test.db");

  for (int i = 0; i < 10000; ++i) {
    char k[32];
    char v[32];
    snprintf(k, 32, "k%d", rand() % 100000);
    snprintf(v, 32, "v%d", rand() % 100000);
    bpt.Put(k, v);
  }
  return 0;
}
