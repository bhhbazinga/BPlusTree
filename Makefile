CXX = g++
CXXFLAGS = -Wall -Wextra -Werror=return-type -pedantic -std=c++2a -g -o2 -fsanitize=leak
EXEC = test
all: $(EXEC)

$(EXEC): test.cc bplus_tree.cc bplus_tree.h
	$(CXX) $(CXXFLAGS) test.cc bplus_tree.cc -o $(EXEC) 
	rm -f test.db

clean:
	rm -rf $(EXEC) *.o test.db
