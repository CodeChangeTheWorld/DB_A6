#include <BPlusSelection.h>

//
// Created by Chengjiu Zhang on 4/9/17.
//
BPlusSelection::BPlusSelection(MyDB_BPlusTreeReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                               MyDB_AttValPtr lowIn, MyDB_AttValPtr highIn, string selectionPredicateIn,
                               vector<string> projectionsIn) {

    output = outputIn;
    selectionPredicate = selectionPredicateIn;
    projections = projectionsIn;
    low = lowIn;
    high = highIn;
    input = inputIn;

}



void BPlusSelection::run() {

    MyDB_RecordPtr judgeRec = input->getEmptyRecord();
    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    func finalPredicate = judgeRec->compileComputation(selectionPredicate);

    vector <func> finalComputations;
    for(string s : projections){
        finalComputations.push_back(judgeRec->compileComputation(s));
    }

    MyDB_RecordIteratorAltPtr myIter = input->getSortedRangeIteratorAlt(low, high);

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

