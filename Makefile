#CC = g++ -lpthread -rdynamic -pthread -O0 -Wno-deprecated -I.
CC = g++ -lpthread -Dddebug -rdynamic -pthread -O0 -Wno-deprecated -I.

CC_OPTIONS= -g

COMPILEER= $(CC) $(CC_OPTIONS)

#BINARIES= main myMain a1test.out a2.1test.out a2.2test.out a3test.out a4.1.out
BINARIES= main 

tag = -i

ifdef linux
tag = -n
endif

all: $(BINARIES)

OBJECT_FILES= Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o Function.o RelOp.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Statistics.o DBEngine.o

main: $(OBJECT_FILES) main.o
	$(CC) -o main $(OBJECT_FILES) main.o -lfl

DIR:
	mkdir -p temp

a4.1.out: $(OBJECT_FILES) test.o DIR
	$(CC) -o a4.1.out $(OBJECT_FILES) test.o  -lfl

a3test.out: $(OBJECT_FILES) a3test.o
	$(CC) -o a3test.out $(OBJECT_FILES) a3test.o  -lfl

a1test.out: $(OBJECT_FILES) A1test.o
	$(CC) -o a1test.out $(OBJECT_FILES) A1test.o  -lfl

a2.1test.out: $(OBJECT_FILES) A2.1test.o
	$(CC) -o a2.1test.out $(OBJECT_FILES) A2.1test.o  -lfl

a2.2test.out: $(OBJECT_FILES) A2.2test.o
	$(CC) -o a2.2test.out $(OBJECT_FILES) A2.2test.o  -lfl

myMain: $(OBJECT_FILES) myMain.o
	$(CC) -o myMain $(OBJECT_FILES) myMain.o -lfl

readLineitem: $(OBJECT_FILES) readLineitem.o
	$(CC) -o readLineitem $(OBJECT_FILES) readLineitem.o -lfl

readLineitemPage: $(OBJECT_FILES) readLineitemPage.o
	$(CC) -o readLineitemPage $(OBJECT_FILES) readLineitemPage.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc

a3test.o: a3test.cc
	$(CC) -g -c a3test.cc

A1test.o: A1test.cc
	$(CC) -g -c A1test.cc

A2.1test.o: A2.1test.cc
	$(CC) -g -c A2.1test.cc

A2.2test.o: A2.2test.cc
	$(CC) -g -c A2.2test.cc

main.o: main.cc
	$(CC) -g -c main.cc

DBEngine.o: DBEngine.cc
	$(CC) -g -c DBEngine.cc

myMain.o: myMain.cc
	$(CC) -g -c myMain.cc

readLineitem.o: readLineitem.cc
	$(CC) -g -c readLineitem.cc

readLineitemPage.o: readLineitemPage.cc
	$(CC) -g -c readLineitemPage.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc
Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f *.bin
	rm -f *.tmp
	#rm -f *.txt
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f temp/*
	rm -rf temp
	rm -f lex.yyfunc.c
	rm -f lex.yy.c
	rm -f yyfunc.tab.c
	rm -f yyfunc.tab.h
	rm -f $(BINARIES)
	rm -f readLineitem
	rm -f readLineitemPage
cleandb:
	rm ./dbfile_dir/*.bin
	rm ./dbfile_dir/*.meta
