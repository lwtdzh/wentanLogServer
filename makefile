INCLUDE = ./include

all: htlib bpsearch pushlog genlog service bptest
	
htlib:
	g++ -c -O3 -D _FILE_OFFSET_BITS=64 ./src/handlethread.cpp -I$(INCLUDE)
	mkdir ./lib
	ar rcs ./lib/libhandlethread.a ./handlethread.o
	rm -f ./handlethread.o

bpsearch:
	g++ -O3 -D _FILE_OFFSET_BITS=64 -o ./search ./tools/bpsearch.cpp ./src/handlethread.cpp -lpthread -I$(INCLUDE)

pushlog:
	g++ -O3 -D _FILE_OFFSET_BITS=64 -o ./pushlog ./tools/pushlog.cpp ./src/handlethread.cpp -lpthread -I$(INCLUDE)
    
genlog:
	g++ -O3 -D _FILE_OFFSET_BITS=64 -o ./genlog ./tools/generatelog.cpp ./src/handlethread.cpp -lpthread -I$(INCLUDE)

service:
	g++ -O3 -D _FILE_OFFSET_BITS=64 -o ./service ./tools/httpservice.cpp ./src/handlethread.cpp -lpthread -I$(INCLUDE)

bptest:
	g++ -O3 -D _FILE_OFFSET_BITS=64 -o ./bptest ./tools/testbpoverval.cpp ./src/handlethread.cpp -lpthread -I$(INCLUDE)

clean:
	rm -rf ./lib
	rm -f ./search
	rm -f ./pushlog
	rm -f ./genlog
	rm -f ./service
	rm -f ./bptest
