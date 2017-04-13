
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

    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for(auto att: outputTable->getTable()->getSchema()->getAtts()){
        mySchemaOut->appendAtt(att);
    }

    int groupNum= groupings.size();
    int aggNum = aggsToCompute.size();

    for(int i= 0;i<aggNum;i++){
        if(aggsToCompute[i].first == MyDB_AggType ::sum || aggsToCompute[i].first == MyDB_AggType ::avg)
            mySchemaOut->appendAtt(make_pair ("aggAtt" + to_string (i), outputTable->getTable()->getSchema()->getAtts()[i+groupNum].second));
    }

    mySchemaOut->appendAtt (make_pair ("MyCount", make_shared <MyDB_IntAttType> ()));


    int attNum = mySchemaOut->getAtts().size();
    vector <MyDB_PageReaderWriter> allData;

    for (int i = 0; i < inputTable->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = (*inputTable)[i];
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back(temp);
        }
    }

    //Scan Input table Write, write record to new page & Hash record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);
    vector <MyDB_PageReaderWriter> tmpPages;
    MyDB_PageReaderWriter pageRW =  MyDB_PageReaderWriter(*inputTable->getBufferMgr());

    MyDB_RecordPtr groupRec = make_shared <MyDB_Record> (mySchemaOut);
    func finalPredicate = inputRec->compileComputation (selectionPredicate);

    vector<func> aggList;
    vector<func> avgList;


    for (int i=0;i<aggsToCompute.size();i++) {
        auto s = aggsToCompute[i];
        if(s.first == MyDB_AggType::avg || s.first == MyDB_AggType::sum){
            aggList.push_back(combinedRec->compileComputation("+([" + mySchemaOut->getAtts()[i+groupNum].first + "], [aggAtt" + to_string (i) + "])"));
        }
        if(s.first == MyDB_AggType::avg){
            avgList.push_back(combinedRec->compileComputation("/([aggAtt" + to_string (i) + "],[MyCount])"));
        }
    }
    func cntfunc =  combinedRec->compileComputation("+( int[1], [MyCount])");

    int recnum=-1;
    while (myIter->advance ()) {
        // hash the current record
        myIter->getCurrent (inputRec);

        if(finalPredicate ()->toBool()) continue;

        size_t hashVal = 0;
        int i=0;
        for(auto f:groupFuncs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i++)->set(f());
        }

        for(auto f:groupAggs){
            combinedRec->getAtt(i++)->set(f());
        }

        while(i<attNum) combinedRec->getAtt(i++)->fromInt(0);

        void * ptr = myHash[hashVal][0];

        if(ptr != nullptr){
            groupRec->fromBinary(ptr);
            combinedRec->copyRecord(groupRec, groupNum+aggNum);
        }

        int app = -1;
        for(int j= groupNum; j<groupNum+aggNum;j++){
            if(aggsToCompute[j-groupNum].first == MyDB_AggType::sum || aggsToCompute[j-groupNum].first == MyDB_AggType::avg) {
                int idx = groupNum + aggNum + (++app);
                func f = aggList[app];
                combinedRec->getAtt(idx)->set(f ());
            }
        }

        combinedRec->getAtt(attNum-1)->set(cntfunc ());

        if(ptr == nullptr){
            combinedRec->recordContentHasChanged();
            ptr = pageRW.appendAndReturnLocation(combinedRec);

            if(ptr == nullptr){
                tmpPages.push_back(pageRW);
                pageRW = MyDB_PageReaderWriter(*inputTable->getBufferMgr());
                ptr= pageRW.appendAndReturnLocation(combinedRec);
            }
            myHash[hashVal].push_back(ptr);
        }else{
            combinedRec->toBinary(ptr);
        }
    }


    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr myIterAgain = getIteratorAlt (tmpPages);

    while(myIterAgain->advance()){
        myIter->getCurrent (combinedRec);

        int agg=0,div=0;
        for(int i=0;i<outputRec->getSchema()->getAtts().size();i++){
            if(i<groupNum ){
                outputRec->getAtt(i)->set(combinedRec->getAtt(i));
            }else{
                switch(aggsToCompute[i-groupNum].first){
                    case MyDB_AggType::sum :{
                        outputRec->getAtt(i)->set(combinedRec->getAtt(groupNum+aggNum+(agg++)));
                        break;
                    }
                    case MyDB_AggType::avg :{
                       // cout << "agg:avg" << endl;
                        agg++;
                        if (avgList.size() > 0) {
                            func f = avgList[div++];
                            outputRec->getAtt(i)->set(f());
                        }
                        break;
                    }
                    case MyDB_AggType::cnt:{
                        outputRec->getAtt(i)->set(combinedRec->getAtt(attNum-1));
                        break;
                    }
                }
             }

                outputRec->recordContentHasChanged();
                outputTable->append(outputRec);
        }
    }
}

#endif