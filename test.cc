#include <iostream>

#include "bplus_tree.h"

void Test(const std::string& s1, const std::string& s2) {
  log("%s,%s\n", s1.data(), s2.data());
}

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;

  BPlusTree bpt("test.db");
  bpt.Put("1", "1");
  bpt.Put("2", "2");
  bpt.Put("3", "3");
  bpt.Put("4", "4");
  bpt.Put("5", "5");
  bpt.Put("6", "6");
  bpt.Put("7", "7");
  bpt.Put("8", "8");

  // bpt.Delete("1");
  // bpt.Delete("2");
  // bpt.Delete("3");
  // bpt.Delete("4");
  // bpt.Delete("5");
  // bpt.Delete("6");
  // bpt.Delete("7");
  // bpt.Delete("8");

  bpt.Delete("8");
  bpt.Delete("7");
  bpt.Delete("6");
  bpt.Delete("5");
  bpt.Delete("4");
  bpt.Delete("3");
  bpt.Delete("2");
  bpt.Delete("1");

  // bpt.Put("1", "1");

  // for (int i = 0; i < 1000; ++i) {
  //   char k[32];
  //   char v[32];
  //   snprintf(k, 32, "k%d", i);
  //   snprintf(v, 32, "v%d", i);
  //   bpt.Put(k, v);

  //   std::string test;
  //   assert(bpt.Get(k, test));
  //   assert(test == v);
  // }

  // for (int i = 0; i < 1000; ++i) {
  //   char k[32];
  //   snprintf(k, 32, "k%d", i);
  //   bpt.Delete(k);
  // }

  return 0;
}
