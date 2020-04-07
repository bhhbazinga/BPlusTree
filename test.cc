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
  bpt.Put("8", "8");
  bpt.Put("6", "6");
  bpt.Put("7", "7");
  bpt.Put("2", "2");

  bpt.Delete("1");
  bpt.Delete("7");
  bpt.Delete("2");
  bpt.Delete("8");

  // for (;;) {
  //   log("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~%s", "\n\n");
  //   BPlusTree bpt("test.db");
  //   srand(time(0));
  //   int n = 8;
  //   for (int i = 1; i <= n; ++i) {
  //     char k[32];
  //     snprintf(k, 32, "%d", rand() % n + 1);
  //     bpt.Put(k, k);
  //   }
  //   for (int i = 1; i <= n; ++i) {
  //     char k[32];
  //     snprintf(k, 32, "%d", rand() % n + 1);
  //     bpt.Delete(k);
  //   }
  //   remove("test.db");
  // }

  // for (int i = 0; i < 100; ++i) {
  //   char k[32];
  //   char v[32];
  //   snprintf(k, 32, "k%d", i);
  //   snprintf(v, 32, "v%d", i);
  //   bpt.Put(k, v);

  //   std::string test;
  //   assert(bpt.Get(k, test));
  //   assert(test == v);
  // }

  // for (int i = 0; i < 100; ++i) {
  //   char k[32];
  //   snprintf(k, 32, "k%d", i);
  //   bpt.Delete(k);
  // }

  return 0;
}
