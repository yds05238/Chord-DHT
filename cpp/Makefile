# 
# 'make' 		build executable files 
# 'make clean'	removes all .o and executable files 
#

CXX = g++ 

CXXFLAGS = -Wall 
CXXFLAGS += -std=c++17 
INCS = -lcrypto -lpthread


all: chord 

chord: chord.o 





# typing 'make' will invoke the first target entry in the file 
# (in this case the default target entry)
# you can name this target entry anything, but "default" or "all"
# are the most commonly used names by convention
#
default: count

# To create the executable file count we need the object files
# countwords.o, counter.o, and scanner.o:
#
count:  countwords.o counter.o scanner.o 
	$(CC) $(CFLAGS) -o count countwords.o counter.o scanner.o

# To create the object file countwords.o, we need the source
# files countwords.c, scanner.h, and counter.h:
#
countwords.o:  countwords.c scanner.h counter.h 
	$(CC) $(CFLAGS) -c countwords.c

# To create the object file counter.o, we need the source files
# counter.c and counter.h:
#
counter.o:  counter.c counter.h 
	$(CC) $(CFLAGS) -c counter.c

# To create the object file scanner.o, we need the source files
# scanner.c and scanner.h:
#
scanner.o:  scanner.c scanner.h 
	$(CC) $(CFLAGS) -c scanner.c

# To start over from scratch, type 'make clean'.  This
# removes the executable file, as well as old .o object
# files and *~ backup files:
#
clean: 
	$(RM) count *.o *~
