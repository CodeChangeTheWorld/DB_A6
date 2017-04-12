
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
            mySchemaOut->appendAtt(make_pair ("MyDB_AggAtt" + to_string (i), mySchemaOut->getAtts()[i+groupNum].second));
    }

    mySchemaOut->appendAtt (make_pair ("MyCount", make_shared <MyDB_IntAttType> ()));

    for(auto att:mySchemaOut->getAtts()){
        cout<<"SchemaOut: "<< att.first<<endl;
        cout<<"SchemaOut Type: "<< att.second->toString()<<endl;
    }
    int attNum = mySchemaOut->getAtts().size();

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
            combinedRec->getAtt(i++)->set(f());
        }

        for(auto f:groupAggs){
            hashVal ^= f ()->hash ();
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
            testRec->fromBinary(myHash[hashVal][myHash[hashVal].size()-1]);
            for(int i=0;i<attNum;i++){
                cout<<"combinedrec Att:" << testRec->getAtt(i).get()->toString()<<endl;
            }
        }
    }

    MyDB_RecordPtr outputRec = outputTable->getEmptyRecord();
    MyDB_RecordPtr tempRec = make_shared <MyDB_Record> (mySchemaOut);
    vector<func> aggList;
    //vector<func> avgList;


    for (int i=0;i<aggsToCompute.size();i++) {
        auto s = aggsToCompute[i];
        if(s.first == MyDB_AggType::avg || s.first == MyDB_AggType::sum){
            cout<<"Build Agg List: "<<"+([" + mySchemaOut->getAtts()[i+groupNum].first + "], [MyDB_AggAtt" + to_string (i) + "])" <<endl;
            aggList.push_back(tempRec->compileComputation("+([" + mySchemaOut->getAtts()[i+groupNum].first + "], [MyDB_AggAtt" + to_string (i) + "])"));
        }
        if(s.first == MyDB_AggType::avg){
            cout<<"Build Avg List: "<<"/( [MyDB_AggAtt" + to_string (i) + "],[MyCount])" <<endl;
           // avgList.push_back(tempRec->compileComputation("/([MyDB_AggAtt" + to_string (i) + "],[MyCount])"));
        }
    }

    for ( auto it = myHash.begin(); it!= myHash.end(); ++it){
        cout<< "hashVal:"<<it->first<<endl;
        vector <void*> &groupRec = myHash [it->first];
        int count = groupRec.size();

        for(int i=0;i<count;i++){
            cout<<"groupRec[i]:"<<groupRec[i]<<endl;
            tempRec->fromBinary(groupRec[i]);

            for(int i=0;i<attNum;i++){
                cout<<"tempRec:"<<tempRec->getAtt(i).get()->toString() <<endl;
            }
            int app = -1;
            for(int j= groupNum; j<groupNum+aggNum;j++){
                cout<<"groupNum+aggNum:"<<groupNum+aggNum<<endl;
                if(aggsToCompute[j-groupNum].first == MyDB_AggType::sum || aggsToCompute[j-groupNum].first == MyDB_AggType::avg) {
                    int idx = groupNum + aggNum + (++app);
                    if (i > 0) tempRec->getAtt(idx)->set(outputRec->getAtt(j));
                    func f = aggList[app];
                    tempRec->getAtt(idx)->set(f ());
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
                        cout<<"agg:sum"<<endl;
                        break;
                    }
                    case MyDB_AggType::avg : {
                        cout << "agg:avg" << endl;
                        //if (avgList.size() > 0) {
                           // func f = avgList[div++];
                           // outputRec->getAtt(i)->set(f());
                            outputRec->getAtt(i)->fromInt(0);
                       // }
                        break;
                    }
                    case MyDB_AggType::cnt:{
                        cout<<"agg:count"<<endl;
                        outputRec->getAtt(i)->fromInt(count);
                        break;
                    }
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