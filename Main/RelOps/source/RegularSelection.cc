//
// Created by Chengjiu Zhang on 4/9/17.
//

#include "RegularSelection.h"

RegularSelection::RegularSelection(MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                                   string selectionPredicateIn, vector <string> projectionsIn) {
    output = outputIn;
    selectionPredicate = selectionPredicateIn;
    projections = projectionsIn;
    input = inputIn;

}

void RegularSelection::run() {

    MyDB_RecordPtr judgeRec = input->getEmptyRecord();
    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    func finalPredicate = judgeRec->compileComputation(selectionPredicate);

    vector <func> finalComputations;
    for(string s : projections){
        finalComputations.push_back(judgeRec->compileComputation(s));
    }

    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();

    while(myIter->advance()){
        myIter->getCurrent(judgeRec);

        if(finalPredicate()->toBool()){
//            cout<<"got one\n";
            int i = 0;
            for(auto f : finalComputations){
                outputRec->getAtt(i++)->set(f());
            }

            outputRec->recordContentHasChanged();
            output->append(outputRec);
        }

    }

}