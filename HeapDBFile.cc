/* 
 * File:   HeapDBFile.cc
 * Author: Abhijeet
 *
 * Created on 2 March, 2013, 6:39 PM
 */

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Errors.h"
#include <iostream>
#include <fstream>
#include <string.h>

extern "C" {
    int yyparse(void); // defined in y.tab.c
}

HeapDBFile::HeapDBFile() {
    NoOfPages = 0;
    CurrentPage = 0;
    f_path = NULL;
}

HeapDBFile::~HeapDBFile() {
    free(f_path);
}

//Create the binary db file to store file class

int HeapDBFile::Create(char *fpath, fType f_type, void *startup) {
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
    metafile << "heap\n";
    metafile.close();

    return 1;
}

//Save the data from the text file to binary db file
void HeapDBFile::Load(Schema &f_schema, char *loadpath) {

    //Create a page to hold the page data
    Page page;

    //Create a record to hold the records
    Record record;

    //Count the no of pages
    off_t pageCount = 0;

    //Check if Create is called and DB file is created before calling Load
    if (f_path != NULL) {

        //Open the binary file database to which data will be dumped
        file.Open(1, f_path);

        //Open the text database from which data will be fetched
        FILE *TextFile = fopen(loadpath, "r");

        //Clean the page before further processing
        //page.EmptyItOut();

        //If text file opened successfully, start processing it
        if (TextFile != NULL) {

            //Read all the records from the text file
            while (record.SuckNextRecord(&f_schema, TextFile)) {

                //Write the records to the page
                if (!page.Append(&record)) {
                    //Come here if page is full
                    //add the full page to file and empty it's content
                    file.AddPage(&page, pageCount);
                    pageCount++;
                    page.EmptyItOut();
                    page.Append(&record);
                }
            }
            //Check if the page is empty, if not flush its content to DB
            if (!page.IsEmpty()) {
                file.AddPage(&page, pageCount);
                page.EmptyItOut();
            }
        } else { //file failed to open
            string message = "Failed to open the file";
            message.append(loadpath);
            SYS_FATAL(message.c_str());
        }

        //Finally close file
        //Close();
    } else { //else of error for calling load without create
        SYS_FATAL("First call create and then call Load function");
    }
} //End of Load()

//Open the binary db file for reading

int HeapDBFile::Open(char *f_path) {
    //First check if the f_path is valid
    ifstream infile;
    infile.open(f_path, ifstream::in);

    if (!infile.good()) {
        cout << "File " << f_path << " does not exist" << endl;
        //Return failure
        return 0;
    }

    //Close the stream
    infile.close();

    //Open the DB file
    file.Open(1, f_path);

    //Return Success
    return 1;
} //End of Open

void HeapDBFile::Add(Record &rec) {
    //Get total no of pages in the file
    off_t LastPage;
    int pages = file.GetLength();

    //GetLength will return 0 if the db is empty
    //And return 2 if there is only 1 page and so on
    if (pages >= 2) {
        LastPage = pages - 2;
    }

    //If there is at least one page in the file
    if (pages >= 2) {
        //Fetch the last page
        file.GetPage(&page, LastPage);

        //Add the record to the page and if it failes then create new page
        if (!page.Append(&rec)) {
            page.EmptyItOut();
            page.Append(&rec);
            LastPage++;
        }
    } else { //If the file is empty
        page.Append(&rec);
        LastPage = 0;
    }
    //Finally write the page to file
    file.AddPage(&page, LastPage);
} //End of Add

//Close the binary db file

int HeapDBFile::Close() {

    //file.Close() will return 0 for success and -1 for failure
    int retval = file.Close();

    if (retval == 0)
        return 1;
    else
        return 0;
}

void HeapDBFile::MoveFirst() {
    //Cleanup the page 
    page.EmptyItOut();
    //Load the page with first page data
    file.GetPage(&page, 0);
    //Set page counter
    CurrentPage = 0;
} //End of MoveFirst

//Fetch records from the binary DB file,
//after it is open by Open function

int HeapDBFile::GetNext(Record &fetchme) {

    //Total no of pages in the db
    NoOfPages = file.GetLength() - 1;

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
} //End of GetNext()

int HeapDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

    Record temp;

    //Total no of pages in the db
    NoOfPages = file.GetLength() - 1;

    //Continue processing till there are more pages in the file
    while (CurrentPage < NoOfPages) {
        //Fetch the record from class
        if (page.GetFirst(&temp)) {
            if (comp.Compare(&temp, &literal, &cnf)) {
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
} // End of GetNext()
