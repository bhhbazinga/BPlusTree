#include <chrono>
#include <iostream>

#include "bplus_tree.h"

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;

  srand(time(0));
  BPlusTree bpt("test.db");
  char k[33];
  char v[101];
  for (int n = 1000; n <= 1000000; n *= 10) {
    std::cout << "----------------------------------------------------" <<
    "\n"; auto t1 = std::chrono::steady_clock::now();
    // Random Insert
    for (int i = 0; i < n; ++i) {
      int r = rand() % n;
      snprintf(k, 33, "k%d", r);
      snprintf(v, 101, "v%d", r);
      bpt.Put(k, v);
    }
    auto t2 = std::chrono::steady_clock::now();
    std::cout << "Random Insert " << n << " items: time span="
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 -
              t1)
                     .count() << "ms"
              << "\n";

    // Random Find
    for (int i = 0; i < n; ++i) {
      int r = rand() % n;
      snprintf(k, 33, "k%d", r);
      std::string value;
      bpt.Get(k, value);
    }
    auto t3 = std::chrono::steady_clock::now();
    std::cout << "Random Get " << n << " items: time span="
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 -
              t2)
                     .count() << "ms"
              << "\n";

    // Random Delete
    for (int i = 0; i < n; ++i) {
      int r = rand() % n;
      snprintf(k, 33, "k%d", r);
      bpt.Delete(k);
    }
    auto t4 = std::chrono::steady_clock::now();
    std::cout << "Random Delete " << n << " items: time span="
              << std::chrono::duration_cast<std::chrono::milliseconds>(t4 -
              t3)
                     .count() << "ms"
              << "\n";
  }
  return 0;
}
