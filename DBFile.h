// Copyright Abhijeet Nayak @2013

#ifndef DBFILE_H
#define DBFILE_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <string>
#include <sstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Errors.h"
#include "Pipe.h"
#include "BigQ.h"

class GenericDBFile {
public:

    GenericDBFile() {
    };

    virtual ~GenericDBFile() {
    };

    virtual void MoveFirst() {
    };

    virtual int GetNext(Record &fetchme) {
    };

    virtual int Create(char *fpath, fType f_type, void *startup) {
    };

    virtual int Open(char *f_path) {
    };

    virtual void Load(Schema &myschema, char *loadpath) {
    };

    virtual void Add(Record &addme) {
    };

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    };

    virtual int Close() {
    };

};

class HeapDBFile : public GenericDBFile {
private:

    //File data structure to hold pages
    File file;
    //Page data structure to hold the group of records
    Page page;
    //Record data structure to hold the single instance of schema data
    Record record;
    //Total no of pages in the file
    off_t NoOfPages;
    //Keep the page count in GetNext
    off_t CurrentPage;
    //Path of the binary file database
    char *f_path;
    //Comparison engine to compare two records
    ComparisonEngine comp;


public:
    HeapDBFile();
    virtual ~HeapDBFile();

    //Create a new binary database to save file instances
    int Create(char *fpath, fType file_type, void *startup);

    //Open the binary database to read the file instances from it
    int Open(char *fpath);

    //Close the binary
    int Close();

    //Copy from loadpath and save it binary DB file 
    //create by Create() method. 
    void Load(Schema &myschema, char *loadpath);

    //Move to first record in the file
    void MoveFirst();

    //Add a new record to the file
    void Add(Record &addme);

    //Get next record from the file
    int GetNext(Record &fetchme);

    //Get next record from the file where it satisfies the cnf rule
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

class SortedDBFile : public GenericDBFile {
    
private:

    //File data structure to hold pages
    File file;
    //Page data structure to hold the group of records
    Page page;
    //Record data structure to hold the single instance of schema data
    Record record;
    //Total no of pages in the file
    off_t NoOfPages;
    //Keep the page count in GetNext
    off_t CurrentPage;
    //Path of the binary file database
    char *f_path;
    //Comparison engine to compare two records
    ComparisonEngine CompEngine;
    //Ordermaker to use a comparison
    OrderMaker ordermaker;
    //Pipes to write data to BigQ
    Pipe *outputPipe;
    //Pipe to read data from BigQ
    Pipe *inputPipe;
    //Mode variable to hold the current modes: writing, reading
    Modes mode;
    //runlen of the sorted dbfile
    int runlen;
    //BigQ to insert data in sorted manner
    BigQ *bigQ;
    //Order maker to compare elements
    OrderMaker comparator;
    
    //Conditional variables to drive the GetNext function
    bool comparatorExists;
    bool buildOrderMakerHistory;
    bool binarySearchHistory;

    //Get single records from file
    int GetRecord(Record &record);

    //Do a sequential search on db and return the found record
    int SequentialGetNext(Record &fetchme, CNF &cnf, Record &literal);

    //Create the comparator 
    int BuildOrderMaker(CNF &cnf, OrderMaker &fileOrderMaker, OrderMaker &CNFOrderMaker);

    //int BinarySearchDriver(CNF &cnf, Record &literal);
    int BinarySearch(CNF &cnf, Record &literal, off_t &result);

public:

    SortedDBFile();
    ~SortedDBFile();

    //Create a new binary database to save file instances
    int Create(char *fpath, fType file_type, void *startup);

    //Open the binary database to read the file instances from it
    int Open(char *fpath);

    //Close the binary dbfile
    int Close();

    //Move to first record in the file
    void MoveFirst();

    //Add a new record to the file
    void Add(Record &addme);

    //Merge data from outputPipe and sorted file and write to new file with same name
    void Merge();

    //Get next record from the file
    int GetNext(Record &fetchme);

    //Get next record from the file where it satisfies the cnf rule
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    
    //dump the text file data to sorted binary file
    void Load(Schema &myschema, char *loadpath);
};

class DBFile {
private:
    GenericDBFile *dbfile;

public:
    DBFile();
    ~DBFile();

    //Create a new binary database to save file instances
    int Create(char *fpath, fType file_type, void *startup);

    //Open the binary database to read the file instances from it
    int Open(char *fpath);

    //Close the binary
    int Close();

    //Copy from loadpath and save it binary DB file 
    //create by Create() method. 
    void Load(Schema &myschema, char *loadpath);

    //Move to first record in the file
    void MoveFirst();

    //Add a new record to the file
    void Add(Record &addme);

    //Get next record from the file
    int GetNext(Record &fetchme);

    //Get next record from the file where it satisfies the cnf rule
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};
#endif
