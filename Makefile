CXX = g++
CXXFLAGS = -Wall -Wextra -Werror=return-type -pedantic -std=c++2a -g -o3 -fsanitize=address
EXEC = test
all: $(EXEC)

$(EXEC): test.cc bplus_tree.cc bplus_tree.h
	$(CXX) $(CXXFLAGS) test.cc bplus_tree.cc -o $(EXEC) 
	rm test.db


clean:
	rm -rf $(EXE) *.o test.db