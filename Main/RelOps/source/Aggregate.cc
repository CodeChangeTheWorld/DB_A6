//
// Created by Liu Fang on 4/1/17.
//

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

using namespace std;

Aggregate::Aggregate(MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                     vector<pair<MyDB_AggType, string>> aggsToCompute, vector<string> groupings,
                     string selectionPredicate) {
    this->input=input;
    this->output=output;
    this->aggsToCompute = aggsToCompute;
    this->groupings = groupings;
    this->selectionPredicate=selectionPredicate;

}

// execute the aggregation
void Aggregate::run() {

    unordered_map <size_t, vector <void *>> myHash;
    vector<MyDB_PageReaderWriterPtr> allData;

    //create schema
    MyDB_SchemaPtr groupSchema = make_shared<MyDB_Schema>();
    unordered_set<string> attrSet = getAttFromGroup(groupings, aggsToCompute);
    for (auto p : input->getTable ()->getSchema ()->getAtts ()){
        if(attrSet.find(p.first) != attrSet.end()){
            groupSchema->appendAtt(p);
        }
    }
    //store records with those attributes in annonymous pin pages
    vector <MyDB_PageReaderWriter> allData;
    for (int i = 0; i < input->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = input->getPinned(i);
        if (temp.getType () == MyDB_PageType :: RegularPage)
            allData.push_back (input->getPinned (i));
    }


    //chose cols in schema and write to pagese
    MyDB_RecordPtr groupRec = make_shared <MyDB_Record> (groupSchema);
    groupRec= input->getEmptyRecord();


}

unordered_set<string> getAttFromGroup(vector<string> groups,vector<pair<MyDB_AggType, string>> aggsToCompute){

    unordered_set<string> set;
    regex exp("\\[(.*?)\\]");
    smatch res;
    for(string str:groups){
        while (regex_search(str, res, exp)) {
            string s = res.str();
            s.erase(0,1).erase(s.length()-1,1);
            set.insert(s);
            str = res.suffix();
        }
    }

    for(pair<MyDB_AggType, string> agg:aggsToCompute){
        string str = agg.second;
        while (regex_search(str , res, exp)) {
            string s = res.str();
            s.erase(0,1).erase(s.length()-1,1);
            set.insert(s);
            str = res.suffix();
        }
    }
    return set;
}

