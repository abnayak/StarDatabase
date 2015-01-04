/* 
 * File:   GenericDBFile.h
 * Author: abhijeet
 * Created on 2 March, 2013, 6:39 PM
 */


#include <algorithm>

#include "DBFile.h"

SortedDBFile::SortedDBFile() {
    bigQ = NULL;
    f_path = NULL;

    outputPipe = new Pipe(100);
    inputPipe = new Pipe(100);

    //Need to set following variables for proper execution of GetNext()
    comparatorExists = true;
    buildOrderMakerHistory = false;
    binarySearchHistory = false;
}

SortedDBFile::~SortedDBFile() {

    //if (f_path != NULL)
        //free(f_path);

    if (bigQ != NULL)
        delete(bigQ);

    if (outputPipe != NULL)
        delete(outputPipe);

    if (inputPipe != NULL)
        delete(inputPipe);
}

/*
 * NOTE:
 * Metadata file format
 * Each line corresponds to the following
 * 
 * Line 1: type
 * Line 2: runlen
 * Line 3: no of attributes in Ordermaker object
 * Line 4 to subsquent lines: 
 * <attribute column><single white space><type of attribute>
 * 
 */

int SortedDBFile::Create(char *fpath, fType file_type, void *Startup) {

    typedef struct {
        OrderMaker *o;
        int l;
    } StartUp;

    //Fetch the ordermaker and runlen from Starup
    StartUp * startup = (StartUp*) Startup;
    OrderMaker *ordermaker = startup->o;
    int runlen = startup->l;

    //Save the file path
    f_path = strdup(fpath);
    file.Open(0, f_path);
    file.Close();

    //Save the metadata
    string metafilename;
    ofstream metafile;
    metafilename += f_path;
    metafilename += ".meta";
    metafile.open(metafilename.c_str());

    //Open the metafile and write the metadata
    if (metafile.good() && metafile.is_open()) {
        metafile << "sorted\n";
        metafile << runlen << "\n";
        ordermaker->SaveToFile(metafile);
        metafile.close();
    } else {
        SYS_FATAL("Failed to open the metafile");
    }
    mode = ready;

    return 1;

}

int SortedDBFile::Open(char *fpath) {

    //Save the binary sorted db file path
    f_path = strdup(fpath);

    //string metafilename(fpath);
    string metafilename = fpath;
    metafilename.append(".meta");
    ifstream metafile;
    string whichAttr, whichType;
    int *whichAttrs;
    Type *whichTypes;

    //Hold the no of element in sortorder
    int len;

    metafile.open(metafilename.c_str());
    string buffer;

    if (metafile.is_open() && metafile.good()) {
        //read sort order type of file
        metafile >> buffer;
        //Read the runlen and save 
        metafile >> buffer;
        runlen = atoi(buffer.c_str());

        //read the attrlen in ordermaker class
        metafile >> buffer;
        len = atoi(buffer.c_str());

        whichAttrs = new int[len];
        whichTypes = new Type[len];

        for (int i = 0; i < len; i++) {
            metafile >> whichAttr >> whichType;
            //cout << atoi(whichAttr.c_str()) << " + " << atoi(whichType.c_str());
            whichAttrs[i] = atoi(whichAttr.c_str());
            whichTypes[i] = (Type) atoi(whichType.c_str());
        }
        metafile.close();
    } else {
        SYS_FATAL("Failed to open the metadata file");
    }

    //Create the ordermaker object with the info
    ordermaker.Initialize(len, whichAttrs, whichTypes);

    //Open the sorted dbfile
    file.Open(1, fpath);

    //Set the mode of the file as reading
    mode = reading;

    //delete all the memory allocated
    delete(whichAttrs);
    delete(whichTypes);

    return 1;
}

int SortedDBFile::Close() {

    /*Check the current mode of file, if its in writing mode change the mode
     * of file to reading and open the file to read records.
     * Then close the input pipe, read all the data from 
     * output pipe and write it to the file. Finally close the output pipe
     * 
     * If it's in reading mode then just close the file. As we do not have any 
     * sorted records in BigQ
     */

    if (mode == writing) {
        mode = reading;
        //Open the file for reading
        file.Open(1, f_path);
        MoveFirst();

        inputPipe->ShutDown();

        Merge();
        
        file.Close();

    } else {
        file.Close();
    }
    return 1;
}

void SortedDBFile::Add(Record& addme) {
    if (bigQ == NULL) {
        bigQ = new BigQ(*inputPipe, *outputPipe, ordermaker, runlen);
    }

    if (mode == reading) {
        file.Close();
        mode = writing;
    }

    inputPipe->Insert(&addme);
}

void SortedDBFile::Load(Schema &f_schema, char *loadpath) {
    //Create a page to hold the page data
    Page page;

    //Create a record to hold the records
    Record record;

    //Count the no of pages
    off_t pageCount = 0;

    //Check if Create is called and DB file is created before calling Load
    if (f_path != NULL) {

        //open the sorted file
        //sortedBinaryfile.Open(loadpath);
        MoveFirst();

        //Open the text database from which data will be fetched
        FILE *TextFile = fopen(loadpath, "r");

        //Clean the page before further processing
        //page.EmptyItOut();

        //If text file opened successfully, start processing it
        if (TextFile != NULL) {

            //Read all the records from the text file
            while (record.SuckNextRecord(&f_schema, TextFile))
                Add(record);

            //Close the sorted binary file
            Close();

        } else { //file failed to open
            string message = "Failed to open the file";
            message.append(loadpath);
            SYS_FATAL(message.c_str());
        }

        //Close the text file
        fclose(TextFile);

    } else { //else of error for calling load without create
        SYS_FATAL("First call create and then call Load function");
    }
}

void SortedDBFile::MoveFirst() {
    //Cleanup the page 
    page.EmptyItOut();

    //Set page counter
    CurrentPage = 0;

    //Load the page with first page data from file
    if (file.GetActualLength() >= 1)
        file.GetPage(&page, CurrentPage);

    //Need to reset following variables for GetNext() to work properly
    comparatorExists = true;
    buildOrderMakerHistory = false;
    binarySearchHistory = false;
}

//This will merge elements from the BigQ thread and records from the binary database

void SortedDBFile::Merge() {

    //Create temp file name with .meta extension to store metadata
    char *tempFileName = strdup(f_path);
    tempFileName = (char*) realloc(tempFileName, strlen(tempFileName) + 10);
    strcat(tempFileName, ".tmp");

    Record pipeRecord, fileRecord, tempRecord;
    ComparisonEngine CompEngg;
    File tempFile;
    Page page;

    tempFile.Open(0, tempFileName);

    //Total no of pages in the db
    NoOfPages = file.GetActualLength();
    int pipeReadResult;
    int fileReadResult;

    if (NoOfPages >= 1) {
        pipeReadResult = outputPipe->Remove(&pipeRecord);
        fileReadResult = GetRecord(fileRecord);

        //Loop here till we get records from both the file and pipe
        while (fileReadResult == 1 && pipeReadResult == 1) {

            if (CompEngg.Compare(&fileRecord, &pipeRecord, &ordermaker) <= 0) {
                //Left record is smaller
                tempRecord.Consume(&fileRecord);
                fileReadResult = GetRecord(fileRecord);
            } else {
                //Right record is smaller
                tempRecord.Consume(&pipeRecord);
                //fetch new pipe record
                pipeReadResult = outputPipe->Remove(&pipeRecord);
            }

            if (!page.Append(&tempRecord)) {
                tempFile.AddPage(&page, tempFile.GetActualLength());
                page.EmptyItOut();
                page.Append(&tempRecord);
            }

        } //end of while loop

        //If there is no record in file and record present in pipe
        if (fileReadResult == 0 && pipeReadResult == 1) {
            while (pipeReadResult) {
                if (!page.Append(&pipeRecord)) {
                    tempFile.AddPage(&page, tempFile.GetActualLength());
                    page.EmptyItOut();
                    page.Append(&pipeRecord);
                }
                pipeReadResult = outputPipe->Remove(&pipeRecord);
            }
        }
        //If there is no record in pipe and record present in file
        if (fileReadResult == 1 && pipeReadResult == 0) {
            while (fileReadResult) {
                if (!page.Append(&fileRecord)) {
                    tempFile.AddPage(&page, tempFile.GetActualLength());
                    page.EmptyItOut();
                    page.Append(&fileRecord);
                }
                fileReadResult = GetRecord(fileRecord);
            }
        }
        //Check if any record left in the page
        if (!page.IsEmpty()) {
            tempFile.AddPage(&page, tempFile.GetActualLength());
            page.EmptyItOut();
        }

    } else {//Come here if the database file is empty
        while (outputPipe->Remove(&pipeRecord)) {
            if (!page.Append(&pipeRecord)) {
                tempFile.AddPage(&page, tempFile.GetActualLength());
                page.EmptyItOut();
                page.Append(&pipeRecord);
            }
        }
        if (!page.IsEmpty()) {
            tempFile.AddPage(&page, tempFile.GetActualLength());
            page.EmptyItOut();
        }
    }

    //Close all the file and reopen after we swap them
    tempFile.Close();
    file.Close();

    //Remove the old database file and move the temp file to f_path
    remove(f_path);
    rename(tempFileName, f_path);

    //Open the new file and move to first record
    file.Open(1, f_path);
    MoveFirst();
    mode = reading;
}

int SortedDBFile::GetNext(Record & fetchme) {

    if (mode == writing) {
        //Calling close will merge the any sorted records in BigQ to file
        //and we can read from the file, this will change mode to reading
        mode = reading;
        //Open the file for reading
        file.Open(1, f_path);
        MoveFirst();

        //Close the inputPipe, will cause the BigQ to sort and send data to outputPipe
        inputPipe->ShutDown();

        //Calling this function will merge pipe and filedb
        Merge();

        //Delete the space allocated to bigQ
        if (bigQ != NULL)
            delete(bigQ);
    }

    if (mode == reading) {
        //Total no of pages in the db
        NoOfPages = file.GetActualLength();

        //Get record from page or go to else
        if (page.GetFirst(&fetchme)) {
            return 1;
        } else {
            //Check if there are more pages left in the file
            if (CurrentPage + 1 < NoOfPages) {
                CurrentPage++;
                file.GetPage(&page, CurrentPage);
                page.GetFirst(&fetchme);
                return 1;
            } else {
                return 0;
            }
        }
    }
}

int SortedDBFile::SequentialGetNext(Record &fetchme, CNF &cnf, Record &literal) {
    Record temp;

    //Total no of pages in the db
    NoOfPages = file.GetActualLength();

    //Continue processing till there are more pages in the file
    while (CurrentPage < NoOfPages) {
        //Fetch the record from class
        if (page.GetFirst(&temp)) {
            if (CompEngine.Compare(&temp, &literal, &cnf)) {
                fetchme.Consume(&temp);
                return 1;
            }
        } else {
            //Check if there are enough pages in file to fetch
            //If pages available fetch it else return 0
            if (CurrentPage + 1 < NoOfPages) {
                CurrentPage++;
                file.GetPage(&page, CurrentPage);
            } else {
                return 0;
            }
        }
    } //End of while loop
    return 0;
}

int SortedDBFile::BuildOrderMaker(CNF &cnf, OrderMaker &fileOrderMaker, OrderMaker &CNFOrderMaker) {

    int numAtts = fileOrderMaker.numAtts;
    bool IsthisAdded;

    int WhichAtts[MAX_ANDS];
    Type WhichTypes[MAX_ANDS];
    int NumAtts = 0;

    //set buildOrderMakerHistory, so that in future we will not run it again
    buildOrderMakerHistory = true;

    for (int k = 0; k < numAtts; k++) {
        IsthisAdded = false;

        int numAnds = cnf.numAnds;
        for (int i = 0; i < numAnds; i++) {//num of and separated clauses in cnf - usually 2 or 3
            for (int j = 0; j < cnf.orLens[i]; j++) { // num of or'ed expressions in this clause. - usually 1

                if (cnf.orList[i][j].whichAtt1 == fileOrderMaker.whichAtts[k]) {
                    //cout << "\n Come here if attr 1 matches ";
                    if (cnf.orList[i][j].operand2 == Literal) { //other operand is a literal
                        if (cnf.orList[i][j].op == Equals) { // comparing literal with equals sign
                            //WhichAtts[NumAtts] = fileOrderMaker.whichAtts[k];
                            WhichAtts[NumAtts] = NumAtts;
                            WhichTypes[NumAtts] = fileOrderMaker.whichTypes[k];
                            NumAtts++;
                            IsthisAdded = true;
                        }
                    }
                } else if (cnf.orList[i][j].whichAtt1 == fileOrderMaker.whichAtts[k]) {
                    //cout << "\n Come here if attr 2 matches ";
                    if (cnf.orList[i][j].operand1 == Literal) { //other operand is a literal
                        if (cnf.orList[i][j].op == Equals) { // comparing literal with equals sign
                            //WhichAtts[NumAtts] = fileOrderMaker.whichAtts[k];
                            WhichAtts[NumAtts] = NumAtts;
                            WhichTypes[NumAtts] = fileOrderMaker.whichTypes[k];
                            NumAtts++;
                            IsthisAdded = true;
                        }
                    }
                }
            }
        }
        if (!IsthisAdded) {
            break;
        }
    }

    if (NumAtts > 0) {
        comparator.Initialize(NumAtts, WhichAtts, WhichTypes);

        cout << "\n Now printing the query ordermaker ";
        cout << "\n----------------- begin query  --------------------\n";
        comparator.Print();
        cout << "\n----------------- end query  --------------------\n";

        return 1;
    }
    return 0;
}
/*
int SortedDBFile::BuildOrderMaker(CNF &cnf, OrderMaker &fileOrderMaker, OrderMaker &CNFOrderMaker) {
    int i, j;
    int whichAtts[MAX_ANDS];
    Type whichTypes[MAX_ANDS];
    int numAttrs = 0;

    //set buildOrderMakerHistory, so that in future we will not run it again
    buildOrderMakerHistory = true;
    for (i = 0; i < fileOrderMaker.numAtts; i++) {
        for (j = 0; j < CNFOrderMaker.numAtts; j++) {
            if (fileOrderMaker.whichAtts[i] == CNFOrderMaker.whichAtts[j]) {
                whichAtts[numAttrs] = numAttrs++;
                whichTypes[numAttrs] = fileOrderMaker.whichTypes[numAttrs];
                break;
            }
        }
    }

    if (numAttrs > 0) {
        comparator.Initialize(numAttrs, whichAtts, whichTypes);

        cout << "\n\n----------------- custom query  --------------------\n";
        comparator.Print();
        cout << "----------------- Custom query  --------------------\n\n";
        
        return 1;
    }
    return 0;
}*/

/*
 * How this algorithm works:
 * Check the first record of mid page with literal and check following conds
 * 1. If equals, check previous pages for any equality as there might be other solutions
 * 2. If less than, set tail to mid -1
 * 3. if greater than, check all the elements of the page for equality.
 *    If found any record equal to literal then return page no. If not set
 *    head = mid+1
 */
int SortedDBFile::BinarySearch(CNF &cnf, Record &literal, off_t &result) {
    off_t head;
    off_t tail;
    off_t mid;
    int found = 0;
    Page tempPage;
    Record tempRecord;
    ComparisonEngine CompEngine;

    cout << "*********************" << endl;
    cout << "Inside Binary Search" << endl;
    cout << "*********************" << endl;

    //Set the begin and eng of the binary search
    head = 0;
    tail = file.GetActualLength() - 1;

    while ((!found) && (head <= tail)) {

        mid = (head + tail) / 2;

        file.GetPage(&tempPage, mid);
        tempPage.GetFirst(&tempRecord);

        int condition;
        //condition = CompEngine.Compare(&literal, &tempRecord, &comparator);
        condition = CompEngine.Compare(&literal, &comparator, &tempRecord, &ordermaker);

        /*cout << "--------------------" << endl;
        tempRecord.Print(new Schema("./catalog", "part"));
        //literal.Print(new Schema("./catalog","part"));
        cout << "condition: " << condition << endl;
        cout << "--------------------" << endl;*/

        if (condition == 0) {
            found = 1;
            result = mid;

            //Check if there are any element previous to current which are
            //equal to the literal
            off_t back = mid - 1;
            while (back >= 0) {
                Page p;
                Record r;
                file.GetPage(&p, back);
                int first = 0;

                while (p.GetFirst(&r) == 1) {

                    //if (CompEngine.Compare(&literal, &r, &comparator) == 0) {
                    if (CompEngine.Compare(&literal, &comparator, &tempRecord, &ordermaker) == 0) {
                        found = 1;
                        result = back;
                        //Check if this record is the first element of the page
                        //if yes then we need to scan the previous page
                        if (first == 0)
                            back--;
                        break;
                    }
                    first++;
                }
                //if the matching element is not the first element of the page
                //then no need of searching any previous page.
                if (first > 0)
                    break;
            }

        } else if (condition < 0) {
            tail = mid - 1;

        } else if (condition > 0) {
            //check all the elements of the mid page to see if equality matches.
            //If no match found then set head = mid+1  
            Page p;
            Record r;
            file.GetPage(&p, mid);

            while (p.GetFirst(&r) == 1) {

                //if (CompEngine.Compare(&literal, &r, &comparator) == 0) {
                if (CompEngine.Compare(&literal, &comparator, &tempRecord, &ordermaker)) {
                    cout << "mid: " << mid;
                    found = 1;
                    result = mid;
                    break;
                }

            }
            //if (mid + 1 < file.GetActualLength())
            head = mid + 1;
        }

    }

    if (found)
        return 1;
    else
        return -1;
}

/*
 * GetNext works in conjuction with BinarySort() and SequentialGetNext()
 * If there is any similar coloumns between the CNF and the sort order used to create
 * the sorted binary file. Then we will create a ordermaker which will have only the 
 * common columns. 
 * 
 * If we are only checking for simple equality then we can use the binary sort to 
 * pin point the page where the record is present then from that position will do a
 * sequential search using the CNF to find the exact match. 
 * 
 * 1. If there is no common column between the CNF and file sort order, then we have 
 * to do a sequeantial search from the beginning of the.  
 */

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record & literal) {
    if (mode == writing) {
        //Calling close will merge the any sorted records in BigQ to file
        //and we can read from the file, this will change mode to reading
        mode = reading;
        //Open the file for reading
        file.Open(1, f_path);
        MoveFirst();
        //Close the inputPipe, will cause the BigQ to sort and send data to outputPipe
        inputPipe->ShutDown();
        //Calling this function will merge pipe and filedb
        Merge();
        //Delete the space allocated to bigQ
        if (bigQ != NULL)
            delete(bigQ);
        comparatorExists = true;
    }
    OrderMaker CNFOrderMaker;
    OrderMaker dummy;
    cnf.GetSortOrders(CNFOrderMaker, dummy);

    /*cout << "--------------------" << endl;
     CNFOrderMaker.Print();
     ordermaker.Print();
    cout << "--------------------" << endl;*/

    int midFound;
    off_t result;

    /*Initially comparatorExists will alway true, as we need this to check if there
     * are any common columns present between the CNF and file ordermaker. 
     */
    if (comparatorExists) {
        //if Both match then go for binary search or go for sequential search
        if (buildOrderMakerHistory == true || BuildOrderMaker(cnf, ordermaker, CNFOrderMaker)) {
            //Check if we ran binary search and get the begin and end pages
            if (binarySearchHistory == false) {
                binarySearchHistory = true;

                midFound = BinarySearch(cnf, literal, result);

                if (midFound == -1) {
                    return 0;
                } else {
                    CurrentPage = result;
                    file.GetPage(&page, CurrentPage);
                    return SequentialGetNext(fetchme, cnf, literal);
                }

            } else {
                return SequentialGetNext(fetchme, cnf, literal);
            }

        } else {
            comparatorExists = false;
            return SequentialGetNext(fetchme, cnf, literal);
        }
    } else {
        //Come here if CNFOrderMaker and fileOrdermaker does not match
        return SequentialGetNext(fetchme, cnf, literal);
    }
}

//This is a private function used by Merge() to get records from the binary database file

int SortedDBFile::GetRecord(Record & fetchme) {
    //Total no of pages in the db
    NoOfPages = file.GetActualLength();

    //Get record from page or go to else
    if (page.GetFirst(&fetchme)) {
        return 1;
    } else {
        //Check if there are more pages left in the file
        if (CurrentPage + 1 < NoOfPages) {
            CurrentPage++;
            file.GetPage(&page, CurrentPage);
            page.GetFirst(&fetchme);
            return 1;
        } else {
            return 0;
        }
    }
}
