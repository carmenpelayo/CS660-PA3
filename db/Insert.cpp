#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;


std::optional<Tuple> Insert::fetchNext() {
    static bool fetched = false;

    if (fetched) return std::nullopt;

    int counter = 0;
    fetched = true;

    while (child->hasNext()) {
        Tuple tempTuple = child->next();
        Database::getBufferPool().insertTuple(t, tableId, &tempTuple);
        counter++;
    }

    Tuple newTup(td); // Use the TupleDesc passed to the constructor
    IntField* field = new IntField(counter);
    newTup.setField(0, field); // The Tuple object now owns the IntField pointer
    return newTup;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId, const TupleDesc &td) {
    // TODO pa3.3: some code goes here
    this->t = t;
    this->child = child;
    this->tableId = tableId;
    this->td = td;
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return td;
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    child->open();
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    Operator::close();
    child->close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    child->rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return {child};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}