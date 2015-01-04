// Copyright Abhijeet Nayak @2013

#include "DBFile.h"

extern "C" {
    int yyparse(void); // defined in y.tab.c
}

DBFile::DBFile() {
}

DBFile::~DBFile() {
    delete(dbfile);
}

//Create the binary db file to store file class

int DBFile::Create(char *fpath, fType f_type, void *startup) {

    if (f_type == heap) {
        dbfile = new HeapDBFile;
    } else if (f_type == sorted) {
        dbfile = new SortedDBFile;
    }

    return dbfile->Create(fpath, f_type, startup);
}

//Save the data from the text file to binary db file

void DBFile::Load(Schema &f_schema, char *loadpath) {
    dbfile->Load(f_schema, loadpath);
} //End of Load()

//Open the binary db file for reading

int DBFile::Open(char *f_path) {

    //Read the metadata
    string metafilename;
    ifstream metafile;
    metafilename += f_path;
    metafilename += ".meta";
    metafile.open(metafilename.c_str());
    string buffer;
    metafile >> buffer;
    metafile.close();

    //cout << "buffer: " << buffer << "bufferlen: " << strlen(buffer.c_str()) << endl;

    if (buffer == "heap") {
        //cout << "creating heap object";
        dbfile = new HeapDBFile();

    } else if (buffer == "sorted") {
        dbfile = new SortedDBFile();
    }
    dbfile->Open(f_path);
    return 1;
} //End of Open

void DBFile::Add(Record &rec) {
    dbfile->Add(rec);
} //End of Add

//Close the binary db file

int DBFile::Close() {
    return dbfile->Close();
}

void DBFile::MoveFirst() {
    dbfile->MoveFirst();
} //End of MoveFirst

//Fetch records from the binary DB file,
//after it is open by Open function

int DBFile::GetNext(Record &fetchme) {
    return dbfile->GetNext(fetchme);
} //End of GetNext()

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

    return dbfile->GetNext(fetchme, cnf, literal);
} // End of GetNext()
