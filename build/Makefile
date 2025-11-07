# Makefile
# Compiles each src/* directory into its own executable

# Compiler settings
CXX = g++
CFLAGS = -std=c++17 -Wall -O2
LIBS = -lmysqlcppconn

# Directories
INC = ../inc
LIB = ../lib
SRC = ../src
DATABASE = ../src/database
# Include paths
INCLUDE = -I$(INC) -I$(LIB) -I$(DATABASE)

# Default: build everything
all: server cache client
	@echo "Done!"

# Build server
server:
	@echo "Building server..."
	$(CXX) $(CFLAGS) $(INCLUDE) $(SRC)/server/*.cpp -o server $(LIBS)

# Build cache
cache:
	@echo "Building cache..."
	$(CXX) $(CFLAGS) $(INCLUDE) $(SRC)/cache/*.cpp -o cache $(LIBS)

client:
	@echo "Building client..."
	$(CXX) $(CFLAGS) $(INCLUDE) $(SRC)/client/*.cpp -o client $(LIBS)

# Clean up
clean:
	rm -f server cache client

# Show help
help:
	@echo "Usage:"
	@echo "  make          - Build all"
	@echo "  make server     - Build server only"
	@echo "  make cache     - Build cache only"
	@echo "  make client     - Build client only"
	@echo "  make clean    - Remove executables"

.PHONY: all clean help server cache client