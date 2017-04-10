
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

    cout<<"pushed"<<endl;

    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
    for (auto p : outputTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    //get all pages from input table
    int pagesize=0,numpage=0;
    vector <MyDB_PageReaderWriter> allData;

    cout<<"schema built"<<endl;

    for (int i = 0; i < inputTable->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = inputTable->getPinned (i);
        pagesize = temp.getPageSize();
        if (temp.getType () == MyDB_PageType :: RegularPage){
//            allData.push_back (inputTable->getPinned (i));
            allData.push_back((*inputTable)[i]);
        }

    }

    cout<<"allData loaded"<<endl;
    MyDB_RecordIteratorAltPtr myIter1 = getIteratorAlt(allData);

    while(myIter1->advance()){
        myIter1->getCurrent (inputRec);
        cout<< inputRec->getAtt(0)->toString() <<endl;
    }

    //Scan Input table Write, write record to new page & Hash record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
//    MyDB_RecordPtr combinedRec = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);

    //MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
    MyDB_BufferManager bm = MyDB_BufferManager(pagesize,numpage, "Aggregate");
    MyDB_PageReaderWriterPtr pageRW = make_shared <MyDB_PageReaderWriter>(*outputTable->getBufferMgr());

    func finalPredicate = combinedRec->compileComputation (selectionPredicate);

    while (myIter->advance ()) {
        // hash the current record
        myIter->getCurrent (inputRec);
        size_t hashVal = 0;
        int i=0;
        for(auto f:groupFuncs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i++)->set(f());
            cout<<"f() : " << f () <<endl;
        }

        for(auto f:groupAggs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i++)->set(f());
            cout<<"f() : " << f () <<endl;
        }


       // if(finalPredicate()->toBool()){
            void * ptr = pageRW->appendAndReturnLocation(combinedRec);
            myHash [hashVal].push_back (ptr);
        //}

    }

    cout<<"temp done"<<endl;

    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordPtr tempRec = make_shared <MyDB_Record> (mySchemaOut);
    for ( auto it = myHash.begin(); it!= myHash.end(); ++it ){
        vector <void*> &groupRec = myHash [it->first];
        int count = groupRec.size();
        int groupNum= groupings.size();
        int sum =0;
        for(void* rec:groupRec){
            tempRec->fromBinary(rec);
            cout<< "tempRec: "<<tempRec->getAtt(0)->toString()<<endl;

            for(int i=0;i<tempRec->getSchema()->getAtts().size();i++){
                if(i>=groupNum && aggsToCompute[i].first == (MyDB_AggType)0) sum += tempRec->getAtt(i).get()->toInt();
            }
        }

        for(int i=0;i<tempRec->getSchema()->getAtts().size();i++){
            if(i<groupNum){
                outputRec->getAtt(i)->set(tempRec->getAtt(i));
            }else{
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
            }
        }

        outputRec->recordContentHasChanged();
        outputTable->append(outputRec);
    }

}

#endif