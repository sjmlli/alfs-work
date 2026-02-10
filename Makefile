# File: Makefile
CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Wshadow -Wconversion -Wno-sign-conversion
LDFLAGS ?=

BIN = alfs

SRC = src/alfs.c

all: $(BIN)

$(BIN): $(SRC) third_party/jsmn.h
	$(CC) $(CFLAGS) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(BIN)

.PHONY: all clean
