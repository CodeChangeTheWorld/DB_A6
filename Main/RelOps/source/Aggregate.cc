
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
            mySchemaOut->appendAtt(make_pair ("MyDB_AggAtt" + to_string (i), outputTable->getTable()->getSchema()->getAtts()[i+groupNum].second));
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
    MyDB_BufferManagerPtr myMgr1 = make_shared <MyDB_BufferManager> (131072, 128, "tempFile1");
    vector <MyDB_PageReaderWriter> tmpPages;
    MyDB_PageReaderWriter pageRW =  MyDB_PageReaderWriter(*myMgr1);

    MyDB_RecordPtr testRec = make_shared <MyDB_Record> (mySchemaOut);
    func finalPredicate = combinedRec->compileComputation (selectionPredicate);

    while (myIter->advance ()) {
        // hash the current record
        myIter->getCurrent (inputRec);

        size_t hashVal = 0;
        int i=0;
        for(auto f:groupFuncs){
            hashVal ^= f ()->hash ();
            combinedRec->getAtt(i)->set(f());
            cout<<"combinedRec:"<<combinedRec->getAtt(i++).get()->toString()<<endl;
        }


        for(auto f:groupAggs){
            combinedRec->getAtt(i++)->set(f());
        }

        while(i<attNum) combinedRec->getAtt(i++)->fromInt(0);

        if(finalPredicate ()->toBool()) {
            combinedRec->recordContentHasChanged();
            void *ptr = pageRW.appendAndReturnLocation(combinedRec);

            if(ptr == nullptr){
                tmpPages.push_back(pageRW);
                pageRW = MyDB_PageReaderWriter(*myMgr1);
                ptr= pageRW.appendAndReturnLocation(combinedRec);
            }
            myHash[hashVal].push_back(ptr);
            testRec->fromBinary(ptr);

            cout<<"Hash Val:"<< hashVal<< endl;
            cout<< "myHash adds: "<<myHash[hashVal][myHash[hashVal].size()-1] <<endl;
            cout<<"New comb Att:" << testRec->getAtt(0).get()->toString()<<endl;


        }
    }

    cout << "print hash"<< myHash.size() << endl;
         for(auto it:myHash){
             cout<<"HashVal:"<<it.first<<endl;
             testRec->fromBinary(it.second[0])->getAtt(0).get()->toString()<<endl;
         }

    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordPtr tempRec = make_shared <MyDB_Record> (mySchemaOut);
    vector<func> aggList;
    vector<func> avgList;


    for (int i=0;i<aggsToCompute.size();i++) {
        auto s = aggsToCompute[i];
        if(s.first == MyDB_AggType::avg || s.first == MyDB_AggType::sum){
            aggList.push_back(tempRec->compileComputation("+([" + mySchemaOut->getAtts()[i+groupNum].first + "], [MyDB_AggAtt" + to_string (i) + "])"));
        }
        if(s.first == MyDB_AggType::avg){
            avgList.push_back(tempRec->compileComputation("/([MyDB_AggAtt" + to_string (i) + "],[MyCount])"));
        }
    }

    for ( auto it:myHash){
        cout<<"Hash val new:"<<it.first<<endl;
        vector <void*> &groupRec = it.second;
        int count = groupRec.size();
        cout<<"bucket count:"<<count;
        for(int i=0;i<count;i++){
            cout<<"groupRec[i]:"<<groupRec[i]<<endl;
            tempRec->fromBinary(groupRec[i]);
            cout<<"tempRec:"<<tempRec->getAtt(0).get()->toString() <<endl;

            int app = -1;
            for(int j= groupNum; j<groupNum+aggNum;j++){
                if(aggsToCompute[j-groupNum].first == MyDB_AggType::sum || aggsToCompute[j-groupNum].first == MyDB_AggType::avg) {
                    int idx = groupNum + aggNum + (++app);
                    if (i > 0) tempRec->getAtt(idx)->set(outputRec->getAtt(j));
                    func f = aggList[app];
                    tempRec->getAtt(idx)->set(f ());
                  //  cout << "New Assigned Sum : "<< tempRec->getAtt(idx).get()->toString()<<endl;
                    outputRec->getAtt(j)->set(tempRec->getAtt(idx));
                }
            }

            tempRec->getAtt(attNum-1)->fromInt(count);
        }

        int div=0;
        for(int i=0;i<outputRec->getSchema()->getAtts().size();i++){

            if(i<groupNum){
                outputRec->getAtt(i)->set(tempRec->getAtt(i));
            }else{
                MyDB_AggType aggtype = aggsToCompute[i-groupNum].first;
                switch(aggtype){
                    case MyDB_AggType::sum :{
                      //  cout<<"agg:sum"<<endl;
                        break;
                    }
                    case MyDB_AggType::avg : {
                       // cout << "agg:avg" << endl;
                        if (avgList.size() > 0) {
                        //    cout<<"in div"<<endl;
                            func f = avgList[div++];
                            outputRec->getAtt(i)->set(f());
                        }
                        break;
                    }
                    case MyDB_AggType::cnt:{
                       // cout<<"agg:count"<<endl;
                        outputRec->getAtt(i)->fromInt(count);
                        break;
                    }
                }
            }

            cout<< "outputRec:"<< outputRec->getAtt(i).get()->toString() <<endl;
        }

        outputRec->recordContentHasChanged();
        outputTable->append(outputRec);
    }

        tmpPages.clear();
        remove("tempFile1");
}

#endif