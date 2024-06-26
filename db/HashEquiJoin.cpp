#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) : p(p){
    // TODO pa3.1: some code goes here
    this->child1 = child1;
    this->child2 = child2;
    this->tupleChild1 = nullptr; // initialize tupleChild1 & tupleChild2
    //this->tupleChild2 = nullptr;
    td = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return &p;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return td;
}

std::string HashEquiJoin::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return child1->getTupleDesc().getFieldName(p.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return child2->getTupleDesc().getFieldName(p.getField2());
}

void HashEquiJoin::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child1->open();
    child2->open();
}

void HashEquiJoin::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    child1->close();
    child2->close();
    //tupleChild1 = nullptr;
    //tupleChild2 = nullptr;
}

void HashEquiJoin::rewind() {
    // TODO pa3.1: some code goes here
    this->close();
    this->open();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    // TODO pa3.1: some code goes here
    std::vector<DbIterator *> children;
    children.push_back(child1);
    children.push_back(child2);
    return children;
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size() != 2) {
        throw std::invalid_argument("Expected exactly two children");
    }
    child1 = children[0];
    child2 = children[1];
    td = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    // TODO pa3.1: some code goes here
    while (tupleChild1 != nullptr || child1->hasNext()) {
        if (tupleChild1 == nullptr) {
            tupleChild1 = new Tuple(child1->next());
        }
        while (child2->hasNext()) {
            tupleChild2 = child2->next(); //infinite loop
            std::optional<Tuple> merged = std::nullopt;

            if (p.filter(tupleChild1, &tupleChild2)) {
                int numField1 = tupleChild1->getTupleDesc().numFields();
                int numField2 = tupleChild2.getTupleDesc().numFields();
                Tuple merge(this->getTupleDesc());
                int numFieldMerge = merge.getTupleDesc().numFields();

                if ((numField1 + numField2) == numFieldMerge) {
                    for (int i = 0; i < numField1; i++) {
                        merge.setField(i, &tupleChild1->getField(i));
                    }
                    for (int j = numField1; j < numFieldMerge; j++) {
                        merge.setField(j, &tupleChild2.getField(j - numField1));
                    }
                    merged = merge;
                }
            }
            if (merged.has_value()) {
                return merged;
            }
        }

        if (child1->hasNext()) {
            tupleChild1 = nullptr;
            child2->rewind();
        } else {
            break;
        }
    }

    return std::nullopt;
}