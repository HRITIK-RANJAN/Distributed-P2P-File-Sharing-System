CXX      ?= g++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -O2 -pthread

# Locate OpenSSL and readline (Homebrew on macOS; system packages on Linux need nothing). Kept in
# INCFLAGS / LDFLAGS so that overriding CXXFLAGS on the command line does not lose them.
OPENSSL_PREFIX  := $(shell brew --prefix openssl@3 2>/dev/null || brew --prefix openssl 2>/dev/null)
READLINE_PREFIX := $(shell brew --prefix readline 2>/dev/null)
ifneq ($(OPENSSL_PREFIX),)
  INCFLAGS += -I$(OPENSSL_PREFIX)/include
  LDFLAGS  += -L$(OPENSSL_PREFIX)/lib
endif
ifneq ($(READLINE_PREFIX),)
  INCFLAGS += -I$(READLINE_PREFIX)/include
  LDFLAGS  += -L$(READLINE_PREFIX)/lib
endif
LIBS = -lreadline -lssl -lcrypto

TARGET = client
SRCS   = client.cpp peer_server.cpp downloader.cpp
OBJS   = $(SRCS:.cpp=.o)
DEPS   = client.h net_util.h tracker_info.h

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -c $< -o $@

# ThreadSanitizer build:  make debug && ./client-debug 127.0.0.1:8003 ../tracker_info.txt
debug:
	$(CXX) -std=c++17 -Wall -Wextra -O1 -g -fsanitize=thread -pthread $(INCFLAGS) -o client-debug $(SRCS) $(LDFLAGS) $(LIBS)

# AddressSanitizer + UBSan build. On Linux this also enables LeakSanitizer; on macOS use `leaks <pid>`.
asan:
	$(CXX) -std=c++17 -Wall -Wextra -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread $(INCFLAGS) -o client-asan $(SRCS) $(LDFLAGS) $(LIBS)

clean:
	rm -f $(TARGET) client-debug client-asan $(OBJS)
	rm -rf client-debug.dSYM client-asan.dSYM

.PHONY: all debug asan clean
