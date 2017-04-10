
#ifndef AGGREGATE_C
#define AGGREGATE_C

#include "Aggregate.h"
#include <unordered_map>
#include "MyDB_PageReaderWriter.h"


Aggregate::Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
        vector <pair <MyDB_AggType, string>> aggsToCompute,
vector <string> groupings, string selectionPredicate){
    inputTable = input;
    outputTable = output;
    this->aggsToCompute = aggsToCompute;
    this->groupings = groupings;
    this->selectionPredicate = selectionPredicate;
}

void Aggregate::run() {
    //key is the hash of all columns in agg and grouping
    unordered_map<size_t, vector<void *>> myHash;

    //prepare output schema
    MyDB_RecordPtr inputRec = inputTable->getEmptyRecord();

    vector<func> groupFuncs;
    for (auto g:groupings) {
        groupFuncs.push_back(inputRec->compileComputation(g));
    }

    vector<func> groupAggs;
    for (auto agg:aggsToCompute) {
        groupAggs.push_back(inputRec->compileComputation(agg.second));
    }

    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
    for (auto p : outputTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    //get all pages from input table
    int pagesize=0,numpage=0;
    vector <MyDB_PageReaderWriter> allData;

    for (int i = 0; i < inputTable->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = inputTable->getPinned (i);
        pagesize = temp.getPageSize();
        if (temp.getType () == MyDB_PageType :: RegularPage)
            allData.push_back (inputTable->getPinned (i));
    }


    //Scan Input table Write, write record to new page & Hash record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);

    //MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
    MyDB_BufferManager bm = MyDB_BufferManager(pagesize,numpage, "Aggregate");
    MyDB_PageReaderWriterPtr pageRW = make_shared <MyDB_PageReaderWriter>(bm);

    while (myIter->advance ()) {
        // hash the current record
        myIter->getCurrent (inputRec);
        size_t hashVal = 0;
        int i=0;
        for(auto f:groupFuncs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i++)->set(f());
        }

        for(auto f:groupAggs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i++)->set(f());
        }

        func finalPredicate = combinedRec->compileComputation (selectionPredicate);
        if(finalPredicate()->toBool()){
            void * ptr = pageRW->appendAndReturnLocation(combinedRec);
            myHash [hashVal].push_back (ptr);
        }

    }

    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordPtr tempRec = make_shared <MyDB_Record> (mySchemaOut);
    for ( auto it = myHash.begin(); it!= myHash.end(); ++it ){
        vector <void*> &groupRec = myHash [it->first];
        int count = groupRec.size();
        int aggCount= aggsToCompute.size();
        int sum =0;
        for(void* rec:groupRec){
            tempRec->fromBinary(rec);
            for(int i=0;i<aggCount;i++){
                //{sum, avg, cnt}
                if(aggsToCompute[i].first == (MyDB_AggType)0) sum += tempRec->getAtt(i).get()->toInt();
            }
        }
        for(int i=0;i<tempRec->getSchema()->getAtts().size();i++){
            if(i<aggCount){
                switch (aggsToCompute[i].first){
                    case (MyDB_AggType)0:
                        outputRec->getAtt(i)->fromInt(sum);
                        break;
                    case (MyDB_AggType)1:
                        outputRec->getAtt(i)->fromInt(sum/count);
                        break;
                    case (MyDB_AggType)2:
                        outputRec->getAtt(i)->fromInt(count);
                        break;
                }
            }else{
                outputRec->getAtt(i)->set(tempRec->getAtt(i));
            }
        }
        outputRec->recordContentHasChanged();
        outputTable->append(outputRec);
    }

}

#endif