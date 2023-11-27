#include <db/Join.h>
using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) {
    this->p = p;
    this->child1 = child1;
    this->child2 = child2;
    // initially set them to null
    this->tupleChild1 = nullptr;
    this->tupleChild2 = nullptr;
}


JoinPredicate *Join::getJoinPredicate() {
    return p;
}

std::string Join::getJoinField1Name() {
    return child1->getTupleDesc().getFieldName(p->getField1());
}

std::string Join::getJoinField2Name() {
    return child2->getTupleDesc().getFieldName(p->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    mergedTupleDesc = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
    return mergedTupleDesc;
}

void Join::open() {
    Operator::open();
    child1->open();
    child2->open();
}

void Join::close() {
    Operator::close();
    child1->close();
    child2->close();
    tupleChild1 = nullptr;
    tupleChild2 = nullptr;
}

void Join::rewind() {
    this->close();
    this->open();
}

std::vector<DbIterator *> Join::getChildren() {
    std::vector<DbIterator *> children;
    children.push_back(child1);
    children.push_back(child2);
    return children;
}

void Join::setChildren(std::vector<DbIterator *> children) {
    if (children.size() != 2) {
        throw std::invalid_argument("Expected exactly two children");
    }
    child1 = children[0];
    child2 = children[1];
}

std::optional<Tuple> Join::fetchNext() {
    while (true) {
        if (tupleChild1 == nullptr) {
            if (child1->hasNext()) {
                tupleChild1 = new Tuple(child1->next());
            } else {
                return std::nullopt;
            }
        }
        while (child2->hasNext()) {
            tupleChild2 = new Tuple(child2->next());

            if (p->filter(tupleChild1, tupleChild2)) {

                Tuple joinedTuple(this->getTupleDesc());

                int fields1 = tupleChild1->getTupleDesc().numFields();
                int fields2 = tupleChild2->getTupleDesc().numFields();

                // iterate over the fields of the fist tuple and populate the joinedTuple
                for (int index1 = 0; index1 < fields1; ++index1) {
                    const Field& field1 = tupleChild1->getField(index1);
                    joinedTuple.setField(index1, &field1);
                }
                
                // iterate over the fields of the second tuple and add them to the joinedTuple
                for (int index2 = 0; index2 < fields2; ++index2) {
                    const Field& field2 = tupleChild2->getField(index2);
                    joinedTuple.setField(fields1 + index2, &field2);
                }
                return joinedTuple;
            }
        }
        tupleChild1 = nullptr;
        child2->rewind();
    }
}
