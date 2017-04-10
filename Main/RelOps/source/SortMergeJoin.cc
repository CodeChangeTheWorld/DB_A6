//
// Created by Chengjiu Zhang on 4/8/17.
//

#ifndef SORT_MERGE_JOIN_C
#define SORT_MERGE_JOIN_C

#include <Sorting.h>
#include <IteratorComparator.h>
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "MyDB_PageReaderWriter.h"

#define RUNSIZE 64

SortMergeJoin ::SortMergeJoin(MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
                              MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
                              vector <string> projectionsIn, pair <string, string> equalityCheckIn,
                              string leftSelectionPredicateIn, string rightSelectionPredicateIn) {

    output = outputIn;
    finalSelectionPredicate = finalSelectionPredicateIn;
    projections = projectionsIn;
    leftTable = leftInputIn;
    rightTable = rightInputIn;
    leftSelectionPredicate = leftSelectionPredicateIn;
    rightSelectionPredicate = rightSelectionPredicateIn;
    equalityCheck = equalityCheckIn;

}


//void appendRecord (MyDB_PageReaderWriter &curPage, vector <MyDB_PageReaderWriter> &returnVal,
//                   MyDB_RecordPtr appendMe, MyDB_BufferManagerPtr parent) {
//
//    // try to append to the current page
//    if (!curPage.append (appendMe)) {
//
//        // if we cannot, then add a new one to the output vector
//        returnVal.push_back (curPage);
//        MyDB_PageReaderWriter temp (*parent);
//        temp.append (appendMe);
//        curPage = temp;
//    }
//}

//vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
//                                              MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
//
//    vector <MyDB_PageReaderWriter> returnVal;
//    MyDB_PageReaderWriter curPage (*parent);
//    bool lhsLoaded = false, rhsLoaded = false;
//
//    // if one of the runs is empty, get outta here
//    if (!leftIter->advance ()) {
//        while (rightIter->advance ()) {
//            rightIter->getCurrent (rhs);
//            appendRecord (curPage, returnVal, rhs, parent);
//        }
//    } else if (!rightIter->advance ()) {
//        while (leftIter->advance ()) {
//            leftIter->getCurrent (lhs);
//            appendRecord (curPage, returnVal, lhs, parent);
//        }
//    } else {
//        while (true) {
//
//            // get the two records
//
//            // here's a bit of an optimization... if one of the records is loaded, don't re-load
//            if (!lhsLoaded) {
//                leftIter->getCurrent (lhs);
//                lhsLoaded = true;
//            }
//
//            if (!rhsLoaded) {
//                rightIter->getCurrent (rhs);
//                rhsLoaded = true;
//            }
//
//            // see if the lhs is less
//            if (comparator ()) {
//                appendRecord (curPage, returnVal, lhs, parent);
//                lhsLoaded = false;
//
//                // deal with the case where we have to append all of the right records to the output
//                if (!leftIter->advance ()) {
//                    appendRecord (curPage, returnVal, rhs, parent);
//                    while (rightIter->advance ()) {
//                        rightIter->getCurrent (rhs);
//                        appendRecord (curPage, returnVal, rhs, parent);
//                    }
//                    break;
//                }
//            } else {
//                appendRecord (curPage, returnVal, rhs, parent);
//                rhsLoaded = false;
//
//                // deal with the ase where we have to append all of the right records to the output
//                if (!rightIter->advance ()) {
//                    appendRecord (curPage, returnVal, lhs, parent);
//                    while (leftIter->advance ()) {
//                        leftIter->getCurrent (lhs);
//                        appendRecord (curPage, returnVal, lhs, parent);
//                    }
//                    break;
//                }
//            }
//        }
//    }
//
//    // remember the current page
//    returnVal.push_back (curPage);
//
//    // outta here!
//    return returnVal;
//}

void loaddata(MyDB_TableReaderWriterPtr cpfrom, MyDB_TableReaderWriterPtr cpto, string SelectionPredicate){

//    vector <MyDB_PageReaderWriter> allData;
//    for (int i = 0; i < cpfrom->getNumPages (); i++) {
//        MyDB_PageReaderWriter temp = cpfrom->getPinned (i);
//        if (temp.getType () == MyDB_PageType :: RegularPage)
//            allData.push_back ((*cpfrom)[i]);
//    }

//    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);
    MyDB_RecordIteratorAltPtr myIter = cpfrom->getIteratorAlt();
    MyDB_RecordPtr inputRec = cpfrom->getEmptyRecord ();
    func pred = inputRec->compileComputation(SelectionPredicate);

    while(myIter->advance()){
        myIter->getCurrent(inputRec);

        if(!pred() -> toBool()){
            continue;
        }

        cpto->append(inputRec);
    }
}


vector <MyDB_RecordIteratorAltPtr> sort (int runSize, MyDB_TableReaderWriterPtr sortMe,
           function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs){

    vector <vector<MyDB_PageReaderWriter>> pagesToSort;

    vector <MyDB_RecordIteratorAltPtr> runIters;

    for (int i = 0; i < sortMe->getNumPages(); i++){

        vector <MyDB_PageReaderWriter> run;
        (*sortMe)[i].sortInPlace(comparator, lhs, rhs);
        run.push_back((*sortMe)[i]);

        if (pagesToSort.size() != runSize && i != sortMe->getNumPages() - 1){
            continue;
        }

        while (pagesToSort.size() > 1){
            vector <vector<MyDB_PageReaderWriter>> newPagesToSort;

            // repeatedly merge the last two pages
            while (pagesToSort.size () > 0) {

                // if there is one run, then just add it
                if (pagesToSort.size () == 1) {
                    newPagesToSort.push_back (pagesToSort.back ());
                    pagesToSort.pop_back ();
                    continue;
                }

                // get the next two runs
                vector<MyDB_PageReaderWriter> runOne = pagesToSort.back ();
                pagesToSort.pop_back ();
                vector<MyDB_PageReaderWriter> runTwo = pagesToSort.back ();
                pagesToSort.pop_back ();

                // merge them
                newPagesToSort.push_back (mergeIntoList (sortMe->getBufferMgr (), getIteratorAlt (runOne),
                                                         getIteratorAlt (runTwo), comparator, lhs, rhs));
            }

            pagesToSort = newPagesToSort;
        }

        runIters.push_back (getIteratorAlt (pagesToSort[0]));

        // and start over on the next run
        pagesToSort.clear ();
    }

    return runIters;

}

void SortMergeJoin::run() {

    string leftKey = equalityCheck.first;
    string rightKey = equalityCheck.second;

    MyDB_RecordPtr leftInputRec = leftTable->getEmptyRecord();
    MyDB_RecordPtr leftInputRecOther = leftTable->getEmptyRecord();
    MyDB_RecordPtr rightInputRec = rightTable->getEmptyRecord();
    MyDB_RecordPtr rightInputRecOther = rightTable->getEmptyRecord();

    function <bool()> leftComp = buildRecordComparator(leftInputRec, leftInputRecOther, leftKey);
    function <bool()> rightComp = buildRecordComparator(rightInputRec, rightInputRecOther, rightKey);

    MyDB_TablePtr tempLeftTable = make_shared <MyDB_Table> ("tempLeft", "tempLeft.bin", leftTable->getTable()->getSchema());
    MyDB_TablePtr tempRightTable = make_shared <MyDB_Table> ("tempRight", "tempRight.bin", rightTable->getTable()->getSchema());

    MyDB_TableReaderWriterPtr tempLeft = make_shared <MyDB_TableReaderWriter>(tempLeftTable, leftTable->getBufferMgr());
    MyDB_TableReaderWriterPtr tempRight = make_shared <MyDB_TableReaderWriter>(tempRightTable, rightTable->getBufferMgr());

    loaddata(leftTable, tempLeft, leftSelectionPredicate);
    loaddata(rightTable, tempRight, rightSelectionPredicate);

    // sort phase
    vector <MyDB_RecordIteratorAltPtr> leftIters = sort(RUNSIZE, tempLeft, leftComp, leftInputRec, leftInputRecOther);
    vector <MyDB_RecordIteratorAltPtr> rightIters = sort(RUNSIZE, tempRight, rightComp, rightInputRec, rightInputRecOther);

    // build

    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for (auto p : leftTable->getTable ()->getSchema ()->getAtts ())
        mySchemaOut->appendAtt (p);
    for (auto p : rightTable->getTable ()->getSchema ()->getAtts ())
        mySchemaOut->appendAtt (p);

    // get the combined record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);

    // and make it a composite of the two input records
//    if (!hadToSwapThem)
        combinedRec->buildFrom (leftInputRec, rightInputRec);
//    else
//        combinedRec->buildFrom (rightInputRec, leftInputRec);

    // now, get the final predicate over it
    func finalPredicate = combinedRec->compileComputation (finalSelectionPredicate);

    // and get the final set of computatoins that will be used to buld the output record
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back (combinedRec->compileComputation (s));
    }


    // merge
    IteratorComparator tempLeftC (leftComp, leftInputRec, leftInputRecOther);
    priority_queue <MyDB_RecordIteratorAltPtr, vector <MyDB_RecordIteratorAltPtr>, IteratorComparator> pqLeft (tempLeftC);

    IteratorComparator tempRightC (rightComp, rightInputRec, rightInputRecOther);
    priority_queue <MyDB_RecordIteratorAltPtr, vector <MyDB_RecordIteratorAltPtr>, IteratorComparator> pqRight (tempRightC);

    function <bool()> ltComp = buildRecordComparator(leftInputRec, rightInputRec, leftKey);
    function <bool()> gtComp = buildRecordComparator(rightInputRec, leftInputRec, leftKey);

    for(MyDB_RecordIteratorAltPtr l : leftIters){
        if(l->advance()){
            pqLeft.push(l);
        }
    }

    for(MyDB_RecordIteratorAltPtr r : rightIters){
        if(r->advance()){
            pqRight.push(r);
        }
    }

    vector <MyDB_PageReaderWriter> leftPages;
    vector <MyDB_PageReaderWriter> rightPages;



    MyDB_RecordPtr outputRec = output->getEmptyRecord();

//    auto liter;
//    auto riter;

    while(pqLeft.size() != 0 && pqRight.size() != 0){

        auto liter = pqLeft.top();
        liter->getCurrent(leftInputRec);

        auto riter = pqRight.top();
        riter->getCurrent(rightInputRec);

        if(ltComp()){
            pqLeft.pop();
            if(liter->advance()){
                pqLeft.push(liter);
            }
        }
        else if(gtComp()){
            pqRight.pop();
            if(riter->advance()){
                pqRight.push(riter);
            }
        }
        else{
            MyDB_PageReaderWriter lpage (true, *leftTable->getBufferMgr());
            MyDB_PageReaderWriter rpage (true, *rightTable->getBufferMgr());
            liter->getCurrent(leftInputRecOther);
            while(!leftComp()){
                if(!lpage.append(leftInputRecOther)){
                    leftPages.push_back(lpage);
                    lpage = MyDB_PageReaderWriter(true, *leftTable->getBufferMgr());
                    lpage.append(leftInputRecOther);
                }
                pqLeft.pop();
                if(liter->advance()){
                    pqLeft.push(liter);

                }
                if(pqLeft.size() == 0){
                    break;
                }
                liter = pqLeft.top();
                liter->getCurrent(leftInputRecOther);
            }

            riter->getCurrent(rightInputRecOther);
            while(!rightComp()){
                if(!rpage.append(rightInputRecOther)){
                    rightPages.push_back(rpage);
                    rpage = MyDB_PageReaderWriter(true, *rightTable->getBufferMgr());
                    rpage.append(rightInputRecOther);
                }
                pqRight.pop();
                if(riter->advance()){
                    pqRight.push(riter);

                }
                if(pqRight.size() == 0){
                    break;
                }
                riter = pqRight.top();
                riter->getCurrent(rightInputRecOther);
            }

            MyDB_RecordIteratorAltPtr listLeft = getIteratorAlt(leftPages);
            MyDB_RecordIteratorAltPtr listRight = getIteratorAlt(rightPages);
            while(listLeft->advance()){
                listLeft->getCurrent(leftInputRec);
                while(listRight->advance()){
                    listRight->getCurrent(rightInputRec);
                    if(finalPredicate() -> toBool()){
                        int i = 0;
                        for(auto f : finalComputations){
                            outputRec->getAtt(i++)->set(f());
                        }

                        outputRec->recordContentHasChanged();
                        output->append(outputRec);
                    }
                }
            }

//            for(int i = 0; i < leftPages.size(); i++){
//                delete leftPages[i];
//            }
//            for(int i = 0; i < rightPages.size(); i++){
//                delete rightPages[i];
//            }
            leftPages.clear();
            rightPages.clear();

        }
    }




}



#endif