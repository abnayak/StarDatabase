#include <iostream>
#include "Record.h"
#include "Schema.h"
#include "DBFile.h"
#include <stdlib.h>
#include "BigQ.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "RelOp.h";


extern struct AndList *final;
extern struct FuncOperator *finalfunc;

extern "C" {
    int yyparse(void); // defined in y.tab.c
    int yyfuncparse(void); // defined in yyfunc.tab.c
    void init_lexical_parser(char *); // defined in lex.yy.c (from Lexer.l)
    void close_lexical_parser(); // defined in lex.yy.c
    void init_lexical_parser_func(char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
    void close_lexical_parser_func(); // defined in lex.yyfunc.c
}

int main() {
    char* tableName = "nation";
    char* tableLoc = "./dbfile_dir/nation.bin";
    Schema schema("catalog", tableName);
    Record record;
    DBFile dbfile;
    int i;
    Page page;
    File file;
    ComparisonEngine ceng;
    Pipe outpipe;
    CNF cnf;
    Record literal;

    dbfile.Open(tableLoc);
    dbfile.MoveFirst();

    //Pipe drinput, droutput;
    //DuplicateRemoval duplicateRemoval;//(drinput,droutput,schema);


    SelectFile fileSelect;

    fileSelect.Run(dbfile, outpipe, cnf, literal);

    fileSelect.WaitUntilDone();

    Pipe wopipe;
    WriteOut writeout;

    Pipe soutpipe, sinpipe;

    //duplicateRemoval.Run(drinput,droutput,schema);

    //while (dbfile.GetNext( record)) {
    while (outpipe.Remove(&record)) {
        // drinput.Insert(&record);
        //wopipe.Insert(&record);
        sinpipe.Insert(&record);
    }



    /*wopipe.ShutDown();

    FILE *outfile = fopen("writeout.txt", "w");

    writeout.Run(wopipe, outfile, schema);
    writeout.WaitUntilDone();

    fclose(outfile);*/

    //drinput.ShutDown();

    //while(droutput.Remove(&record)){
    //  record.Print(&schema);
    // }


    //test sum functionality:
    Attribute n_regionkey[] = {
        {"n_regionkey", Int}};
    Schema join_sch("join_sch", 1, n_regionkey);
    Function func;

    init_lexical_parser_func("n_regionkey");
    if (yyfuncparse() != 0) {
        cout << " Error: can't parse your arithmetic expr. " << "n_regionkey" << endl;
        exit(1);
    }
    func.GrowFromParseTree(finalfunc, join_sch); // constructs CNF predicate
    close_lexical_parser_func();


    Sum sum;
    sum.Run(sinpipe, soutpipe, func);
    sinpipe.ShutDown();
    sum.WaitUntilDone();

    if (soutpipe.Remove(&record))
        record.Print(&join_sch);



    /*Create a sorted dbfile from text tbl file
    cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF.\n";
        exit(1);
    }
    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, &schema, literal); // constructs CNF predicate
    OrderMaker dummy, sortorder;
    sort_pred.GetSortOrders(sortorder, dummy);

    int runlen = 0;
    while (runlen < 1) {
        cout << "\t\n specify runlength:\n\t ";
        cin >> runlen;
    }

    struct {
        OrderMaker *o;
        int l;
    } startup = {&sortorder, runlen};


    dbfile.Create("./dbfile_dir/orders.bin", sorted, &startup);
    dbfile.Close();
    
    dbfile.Open("./dbfile_dir/orders.bin");
    dbfile.Load(schema, "/cise/tmp/dbi_sp11/DATA/1G/orders.tbl");
    dbfile.Close();
           
     */

    //Create a sorted dbfile from text tbl file



    /*dbfile.Open("./dbfile_dir/lineitem.bin");
    
    file.Open(1,"./dbfile_dir/lineitem.bin");
    
    file.GetPage(&page,0);
    int count =0;
    while (page.GetFirst(&record))
        count++;
    
    cout << count << endl;
    
    file.GetPage(&page,1);
    count =0;
    while (page.GetFirst(&record))
        count++;
    
    cout << count << endl;*/




    /*
        int whichAtts[MAX_ANDS];
        Type whichTypes[MAX_ANDS];
        int numAttrs = 0;

        dbfile.Open("./dbfile_dir/nation.bin");
        dbfile.MoveFirst();

        OrderMaker CNFordermaker;
        OrderMaker Fileordermaker;

        //whichAtts[0] = 0;
        //whichTypes[0] = Int;
        // whichAtts[1] = 2;
        // whichTypes[1] = Int;
        //numAttrs = 1;

        //CNFordermaker.Initialize(numAttrs, whichAtts, whichTypes);

        whichAtts[0] = 0;
        whichAtts[1] = 1;
        whichTypes[0] = Int;
        whichTypes[1] = String;
        numAttrs = 2;
        Fileordermaker.Initialize(numAttrs, whichAtts, whichTypes);

        cout << "\n enter CNF predicate (when done press ctrl-D):\n\t";
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF.\n";
            exit(1);
        }

        CNF cnf_pred;
        Record literal;
        cnf_pred.GrowFromParseTree(final, &schema, literal);
        OrderMaker dummy;
        cnf_pred.GetSortOrders(CNFordermaker, dummy);

        cout << "-- cnf order maker --" << endl;
        CNFordermaker.Print();
        literal.Print(&schema);
        cout << "-- ---------------- --" << endl;
        //literal.Print(&schema);

        while (dbfile.GetNext(record)) {
            cout << ceng.Compare(&literal, &CNFordermaker, &record, &Fileordermaker) << endl;

            //cout << ceng.Compare(&literal,&record,&cnf_pred) << endl;

            //cout << ceng.Compare(&literal,&record,&CNFordermaker) << endl;
        }

     */

    //dbfile.Create("./records.db", heap, NULL);
    //dbfile.Load(schema, "./10M/orders.tbl");
    //dbfile.MoveFirst();
    //dbfile.Open("./nation.db");

    //BigQ bigq;

    /*
    DBFile myfile;
    //myfile.Open("./SortedPageLenRecords");
    myfile.Open("./heapDB/orders.bin");
    myfile.MoveFirst();
    Record newrecord;

    cout << "========================================================" << endl;
    while (myfile.GetNext(newrecord))
    {
            newrecord.Print(&schema);
    }
    myfile.Close();*/


    /*dbfile.Close();*/

    /*FILE * file = fopen ( "./10M/lineitem.tbl", "r");
    fclose(file);
    dbfile.Close();
    dbfile.Open("./lineitem.db");
    dbfile.Close();
    File lineitem;
    lineitem.Open(1, "./lineitem.db");
    cout << "total pages in the table : "  << lineitem.GetLength()-1 << endl;
    lineitem.GetPage(&page,0);
    while ( page.GetFirst(&record) )
    {
            record.Print(&schema);
		
    }
	
    while( dbfile.GetNext( record ) )
    {
            record.Print(&schema);
    }
		
    int countPage=0;
    for( i = 0 ; i < 101; i++ ){
                    cout << "fetching page: " << countPage++ << endl;
                    lineitem.GetPage(&page, i);
    }
    fclose(file);	
     */
    return 0;
}
