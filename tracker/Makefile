CXX      ?= g++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -O2 -pthread

# Locate OpenSSL (Homebrew on macOS; system packages on Linux need nothing). Kept in INCFLAGS /
# LDFLAGS so that overriding CXXFLAGS on the command line does not lose them.
OPENSSL_PREFIX := $(shell brew --prefix openssl@3 2>/dev/null || brew --prefix openssl 2>/dev/null)
ifneq ($(OPENSSL_PREFIX),)
  INCFLAGS += -I$(OPENSSL_PREFIX)/include
  LDFLAGS  += -L$(OPENSSL_PREFIX)/lib
endif
LIBS = -lssl -lcrypto

TARGET = tracker
SRCS   = tracker.cpp user_manager.cpp file_manager.cpp store.cpp sync_trackers.cpp
OBJS   = $(SRCS:.cpp=.o)
DEPS   = header.h net_util.h tracker_info.h

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $(INCFLAGS) -c $< -o $@

# ThreadSanitizer build:  make debug && ./tracker-debug ../tracker_info.txt 1
debug:
	$(CXX) -std=c++17 -Wall -Wextra -O1 -g -fsanitize=thread -pthread $(INCFLAGS) -o tracker-debug $(SRCS) $(LDFLAGS) $(LIBS)

# AddressSanitizer + UBSan build (memory errors, undefined behaviour). On Linux this also enables
# LeakSanitizer; on macOS use `leaks <pid>` for leaks.
asan:
	$(CXX) -std=c++17 -Wall -Wextra -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread $(INCFLAGS) -o tracker-asan $(SRCS) $(LDFLAGS) $(LIBS)

clean:
	rm -f $(TARGET) tracker-debug tracker-asan $(OBJS)
	rm -rf tracker-debug.dSYM tracker-asan.dSYM

.PHONY: all debug asan clean
