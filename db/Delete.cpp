#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) {
    this->transactionId = t;
    this->childOperator = child;
    std::vector<db::Types::Type> types = {db::Types::INT_TYPE};
    this->deleteTupleDesc = db::TupleDesc(types);
    this->countOfDeletedRecords = 0;
    this->isOpen = false;
}

const TupleDesc &Delete::getTupleDesc() const {
    return this->deleteTupleDesc;
}

void Delete::open() {
    this->childOperator->open();
    this->isOpen = true;
}

void Delete::close() {
    this->childOperator->close();
    this->isOpen = false;
}

void Delete::rewind() {
    this->childOperator->rewind();
    this->countOfDeletedRecords = 0;
}

std::vector<DbIterator *> Delete::getChildren() {
    return {this->childOperator};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    if (!children.empty()) {
        this->childOperator = children[0];
    }
}

std::optional<Tuple> Delete::fetchNext() {
    
    // Use a reference or object instead of a pointer
    BufferPool &bufferPool = Database::getBufferPool();
    while (this->childOperator->hasNext()) {
        Tuple tuple = this->childOperator->next();

        // Pass the address of the tuple
        bufferPool.deleteTuple(this->transactionId, &tuple);
        this->countOfDeletedRecords++;
    }

    if (this->countOfDeletedRecords > 0) {
        Tuple returnTuple = Tuple(this->deleteTupleDesc);
        
        // Create an IntField object and pass its address
        IntField countField(this->countOfDeletedRecords);
        returnTuple.setField(0, &countField);
        return returnTuple;
    }

    return std::nullopt;
}

