CC=gcc
LD=gcc
AR=ar

prefix := /usr/local

CFLAGS=-std=gnu99 -Wall -Werror -O2
ARFLAGS=rcs

# note order
TEST_LINKFLAGS=-L. -lmeds -lcfitsio -lm
MAKE_INPUT_LINKFLAGS=-lcfitsio -lm

LIB_SOURCES = meds.c

TEST_SOURCES = test.c
TEST_SPEED_SOURCES = test-speed.c
MAKE_INPUT_SOURCES = make-meds-input.c

ALL_SOURCES = $(LIB_SOURCES) \
			  $(TEST_SOURCES) \
			  $(TEST_SPEED_SOURCES) \
			  $(MAKE_INPUT_SOURCES)
LIB_OBJECTS=$(patsubst %.c,%.o,$(LIB_SOURCES)) 
TEST_OBJECTS=$(patsubst %.c,%.o,$(TEST_SOURCES)) 
TEST_SPEED_OBJECTS=$(patsubst %.c,%.o,$(TEST_SPEED_SOURCES)) 
MAKE_INPUT_OBJECTS=$(patsubst %.c,%.o,$(MAKE_INPUT_SOURCES)) 

LIB = libmeds.a

TEST_PROG = test
TEST_SPEED_PROG = test-speed
MAKE_INPUT_PROG = make-meds-input

PROGS=$(TEST_PROG) $(TEST_SPEED_PROG) $(MAKE_INPUT_PROG)

default: all

depend: .depend

.depend: $(ALL_SOURCES)
	$(CC) $(CFLAGS) -MM $^ > ./.depend;

-include .depend



install: $(LIB) $(MAKE_INPUT_PROG)
	mkdir -p $(prefix)/lib
	mkdir -p $(prefix)/bin
	cp $(LIB) $(prefix)/lib/
	cp $(MAKE_INPUT_PROG) $(prefix)/bin/
	chmod a+x $(prefix)/bin/$(MAKE_INPUT_PROG)


all: $(TEST_PROG) $(TEST_SPEED_PROG) $(MAKE_INPUT_PROG)

lib: $(LIB)
	
$(LIB): $(LIB_OBJECTS)
	$(AR) $(ARFLAGS) $(LIB) $(LIB_OBJECTS)

$(TEST_PROG): $(LIB) $(TEST_OBJECTS)
	$(LD) -o $@  $(TEST_OBJECTS) $(TEST_LINKFLAGS)

$(TEST_SPEED_PROG): $(LIB) $(TEST_SPEED_OBJECTS)
	$(LD) -o $@  $(TEST_SPEED_OBJECTS) $(TEST_LINKFLAGS)

$(MAKE_INPUT_PROG): $(MAKE_INPUT_OBJECTS)
	$(LD) -o $@ $(MAKE_INPUT_OBJECTS) $(MAKE_INPUT_LINKFLAGS) 

clean:
	rm -f *.o $(LIB) $(PROGS) .depend
