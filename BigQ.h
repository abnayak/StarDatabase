#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "Errors.h"
#include <sys/time.h>

using namespace std;

class myTime {
public:

    static string getusec() {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        stringstream ss;
        ss << tv.tv_sec;
        ss << ".";
        ss << tv.tv_usec;
        return ss.str();
    }
};


//Structure use to pass arguments from the
//BigQ contructor to pthread thread

typedef struct {
    Pipe *in;
    Pipe *out;
    OrderMaker *sortorder;
    int runlen;
} BigQAttrs;

class BigQ {
public:

    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    //BigQ();
    ~BigQ();
};

//Call to keep track of each head of runlen runs
//There can be maximum of (file size/runlen page size) no of heads

class RunRecordHead {
private:
    off_t startPage;
    off_t endPage;
    File *tempFile;
    Page page;
public:
    Record *record;

    RunRecordHead(off_t startPage, off_t endPage, File *tempFile);
    ~RunRecordHead();

    //Return the smallest element of the runlen pages
    void GetHead(Record *record);

    //This function is use to set new head when top is removed
    int SetNewHead();

};

//This overloaded class is used to compare two RunRecordHead objects

class CompareRunRecordHead {
    OrderMaker *orderMaker;
public:

    CompareRunRecordHead(OrderMaker *orderMaker);
    bool operator()(RunRecordHead *first, RunRecordHead *second);

};

#endif
