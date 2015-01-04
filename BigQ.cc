#include <algorithm>
#include <queue>
#include "BigQ.h"
#include "DBFile.h"

BigQAttrs attributes;

//Constructor for RunRecordHead class

RunRecordHead::RunRecordHead(off_t startPage, off_t endPage, File *tempFile) {
    this->startPage = startPage;
    this->endPage = endPage;
    this->tempFile = tempFile;

    //Get the first record of the run
    this->tempFile->GetPage(&page, startPage);
    record = new Record();
    page.GetFirst(record);
}

int RunRecordHead::SetNewHead() {
    if (page.GetFirst(record) == 0) {
        //Come here if there is no record in the page
        if (startPage < endPage) {
            startPage++;
            tempFile->GetPage(&page, startPage);
            if (page.GetFirst(record) == 1)
                return 1;
            else
                SYS_FATAL("Failed to fetch record from page");
        } else {
            return 0;
        }
    }
    return 1;
}

void RunRecordHead::GetHead(Record *recordBuf) {
    recordBuf->Consume(record);
}

RunRecordHead::~RunRecordHead() {
    delete record;
}

CompareRunRecordHead::CompareRunRecordHead(OrderMaker *orderMaker) {
    this->orderMaker = orderMaker;
}

bool CompareRunRecordHead::operator()(RunRecordHead *first,
        RunRecordHead *second) {
    ComparisonEngine compEngine;
    if (compEngine.Compare(first->record, second->record, orderMaker) >= 0)
        return true;
    else
        return false;
}

//This is the thread executed by the new thread created by BigQ
//contructor to sort the pages and push the records to out pipe

void * BigQThread(void *arg) {

    //Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen

    Pipe *in = ((BigQAttrs*) arg)->in;
    Pipe *out = ((BigQAttrs*) arg)->out;
    OrderMaker *sortorder = ((BigQAttrs*) arg)->sortorder;
    int runlen = ((BigQAttrs*) arg)->runlen;

    //Page page;
    Record *record, *recordBuf;

    //DBFile dbfile;
    File SortedRecFile;

    //Open a temp file which will hold the sorted runlen pages

    static int tmpCount;
    char tempFilename[100];
    sprintf(tempFilename, "buffer%d.tmp", tmpCount++);
    SortedRecFile.Open(0, tempFilename);

    //This vector is used to keep count of pages which are part of
    //runlen sorting. This will be used in second stage of algo.
    vector<off_t> pageOffsets;

    RecordComparison recCompare(sortorder);

    off_t totalSize = PAGE_SIZE * runlen;

    int curSizeInBytes = 0;
    int recSize = 0;

    //Maximum size of each run in bytes
    int max_size = runlen * PAGE_SIZE;

    //Record vector to hold the records in the first stage of TMPP algorithm.
    vector<Record*> recVector;

    Record *rec;
    File tmp_file;

    Page curPage;
    curPage.EmptyItOut();

    Record temp;
    while (in->Remove(&temp) == 1) {

        recSize = (&temp)->GetSize(); //((int *) b)[0];

        Record *newrec = new Record;
        newrec->Consume(&temp);

        //If total size has not exceeded max size of run,
        //Increase current size of current run and
        //push record into Priority Queue
        if (curSizeInBytes + recSize <= max_size) {
            recVector.push_back(newrec);
            curSizeInBytes += recSize;
        }            //If run has reached max size
        else {
            //Save the staring no of the page
            pageOffsets.push_back(SortedRecFile.GetActualLength());

            //Pop all records from PQ and insert into file
            sort(recVector.begin(), recVector.end(), recCompare);

            for (int i = 0; i < recVector.size(); i++) {
                rec = recVector.at(i);
                // append record to the page.if page is full return 0
                if (curPage.Append(rec) == 0) {

                    // if page is full , add page to full
                    SortedRecFile.AddPage(&curPage,
                            SortedRecFile.GetActualLength());

                    curPage.EmptyItOut();

                    // append the record to the new page
                    curPage.Append(rec);

                }
                delete rec;

            }

            recVector.clear();

            //this is absolutely desirable as these are sorted records whihc have not been written
            //at the end of the run
            if (!curPage.IsEmpty()) {
                SortedRecFile.AddPage(&curPage,
                        SortedRecFile.GetActualLength());
                curPage.EmptyItOut();
            }

            pageOffsets.push_back(SortedRecFile.GetActualLength() - 1);

            recVector.push_back(newrec);
            curSizeInBytes = recSize;
        }
    }

    if (curSizeInBytes > 0) {
        pageOffsets.push_back(SortedRecFile.GetActualLength());

        //sort the records of the last run when the run has not reached max size
        sort(recVector.begin(), recVector.end(), recCompare);

        for (int i = 0; i < recVector.size(); i++) {
            rec = recVector.at(i);
            // append record to the page.if page is full return 0
            if (curPage.Append(rec) == 0) {
                SortedRecFile.AddPage(&curPage,
                        SortedRecFile.GetActualLength());
                curPage.EmptyItOut();
                curPage.Append(rec);
            }
            delete rec;
        }
        recVector.clear();

        if (!curPage.IsEmpty()) {
            SortedRecFile.AddPage(&curPage, SortedRecFile.GetActualLength());
            curPage.EmptyItOut();
        }
        pageOffsets.push_back(SortedRecFile.GetActualLength() - 1);

    } //End of the fist stage of TPPM algo

    //Start of the second phase of the TPPM algorithm

    //Priority_queue to hold the

    priority_queue<RunRecordHead *, vector<RunRecordHead*>, CompareRunRecordHead> RunRecordHeadPQ(
            sortorder);

    //Compute total no of runs required to merge
    int numOfRuns = pageOffsets.size() / 2;

    //RunRecordHead points to each run. There are total of numOfRuns heads
    RunRecordHead *runRecordHead;

    //This loop will create numOfRuns no of RunRecordHead and save it in priority_queue
    for (int i = 0; i < pageOffsets.size(); i += 2) {
        runRecordHead = new RunRecordHead(pageOffsets.at(i),
                pageOffsets.at(i + 1), &SortedRecFile);

        //Push the indivisual record heads to be sorted in the priority_queue
        RunRecordHeadPQ.push(runRecordHead);
    }

    /*
     * Logic of second stage of TMPP algorithm. Pop out RunRecordHead which will
     * have the smallest record among all the RunRecordHeads. Remove the Record 
     * from the RunRecordHead and push it into output pipe. If there are more 
     * Records in the priority_queue set the head of RunRecordHead otherwise 
     * delete the RunRecordHead object.
     */
    RunRecordHead *top;
    Record recordBuffer;

    while (RunRecordHeadPQ.size() > 0) {

        top = RunRecordHeadPQ.top();
        RunRecordHeadPQ.pop();

        top->GetHead(&recordBuffer);

        if (top->SetNewHead()) {
            RunRecordHeadPQ.push(top);
        } else {
            delete (top);
        }

        out->Insert(&recordBuffer);
    }

    //Cleanup routines
    SortedRecFile.Close();
    remove(tempFilename);
    out->ShutDown();

}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

    if (runlen <= 0)
        SYS_FATAL("Runlen size can't be less than zero");

    //Prepair attribure object to send arguments to the pthread
    attributes = {&in, &out, &sortorder, runlen};

    pthread_t worker;
    pthread_create(&worker, NULL, BigQThread, &attributes);



}

BigQ::~BigQ() {
}
