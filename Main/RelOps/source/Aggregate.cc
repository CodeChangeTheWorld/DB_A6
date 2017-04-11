
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
        cout<< g <<endl;
        groupFuncs.push_back(inputRec->compileComputation(g));
    }

    vector<func> groupAggs;
    for (auto agg:aggsToCompute) {
        cout<< agg.second <<endl;
        groupAggs.push_back(inputRec->compileComputation(agg.second));
    }

    cout<<"pushed"<<endl;

    MyDB_SchemaPtr mySchemaOut = outputTable->getTable()->getSchema();
    mySchemaOut->appendAtt (make_pair ("count", make_shared <MyDB_IntAttType> ()));


    vector <MyDB_PageReaderWriter> allData;

    cout<<"schema built"<<endl;

    for (int i = 0; i < inputTable->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = (*inputTable)[i];
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back(temp);
        }

    }

    cout<<"allData loaded"<<endl;

    //Scan Input table Write, write record to new page & Hash record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);

    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);
    MyDB_BufferManagerPtr myMgr1 = make_shared <MyDB_BufferManager> (131072, 128, "tempFile1");
    vector <MyDB_PageReaderWriter> tmpPages;
    MyDB_PageReaderWriter pageRW =  MyDB_PageReaderWriter(*myMgr1);//*outputTable->getBufferMgr());


    func finalPredicate = combinedRec->compileComputation (selectionPredicate);
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

        combinedRec->getAtt(combinedRec->getSchema()->getAtts().size()-1)->fromInt(0);

        if(finalPredicate ()->toBool()) {
            combinedRec->recordContentHasChanged();
            void *ptr = pageRW.appendAndReturnLocation(combinedRec);

            if(ptr == nullptr){
                tmpPages.push_back(pageRW);
                pageRW = MyDB_PageReaderWriter(*myMgr1);
                ptr= pageRW.appendAndReturnLocation(combinedRec);
            }
            myHash[hashVal].push_back(ptr);
        }
    }

    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordPtr tempRec = make_shared <MyDB_Record> (mySchemaOut);
    int groupNum= groupings.size();
    int aggNum = aggsToCompute.size();
    vector<func> aggList;

    for(int i= groupNum;i<groupNum+aggNum;i++){
        if(aggsToCompute[i-groupNum].first == MyDB_AggType ::sum || aggsToCompute[i-groupNum].first == MyDB_AggType ::avg)
            tempRec->getSchema()->appendAtt(make_pair ("[MyDB_AggAtt" + to_string (i-groupNum) + "]", mySchemaOut->getAtts()[i].second));
    }


    for ( auto it = myHash.begin(); it!= myHash.end(); ++it){
        vector <void*> &groupRec = myHash [it->first];
        int count = groupRec.size();

        for(int i=0;i<groupNum+aggNum;i++){
            tempRec->fromBinary(groupRec[i]);
            for(int i=0;i<tempRec->getSchema()->getAtts().size();i++){
            cout<<"tempRec:"<<tempRec->getAtt(i)<<endl;
//                if(i>=groupNum && (aggsToCompute[i-groupNum].first == MyDB_AggType ::sum || aggsToCompute[i-groupNum].first == MyDB_AggType ::avg )){
//                    sum += tempRec->getAtt(i).get()->toInt();
//                }
            }
        }

        for(int i=0;i<outputRec->getSchema()->getAtts().size();i++){
            if(i<groupNum){
                cout<< "group attr"<<endl;
                outputRec->getAtt(i)->set(tempRec->getAtt(i));
            }else{
                cout<<"sum:"<<sum<<endl;
                switch (aggsToCompute[i-groupNum].first){
                    case MyDB_AggType ::sum :
                        cout<<"agg:sum"<<endl;
                        outputRec->getAtt(i)->fromInt(sum);
                        break;
                    case MyDB_AggType ::avg :
                        cout<<"agg:avg"<<endl;
                        outputRec->getAtt(i)->fromInt(sum/count);
                        break;
                    case MyDB_AggType :: cnt:
                        cout<<"agg:count"<<endl;
                        outputRec->getAtt(i)->fromInt(count);
                        break;
                }
            }

        }
        for(int i=0;i<outputRec->getSchema()->getAtts().size();i++)  {
            cout<<"outRec Att: "<< outputRec->getAtt(i).get()->toString()<<endl;
        }

        outputRec->recordContentHasChanged();
        outputTable->append(outputRec);
    }

        tmpPages.clear();
        remove("tempFile1");
}

#endif