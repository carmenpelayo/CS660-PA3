#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {

    if (aggIterator != nullptr && aggIterator->hasNext()) {
        return aggIterator->next();
    }
    return std::nullopt;  // Return no tuple if no more results are available
}


Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) {
    this->child = child;
    this->afield = afield;
    this->gfield = gfield;
    this->aop = aop;
    initialized = false;

    // Set aggTupleDesc based on the expected output of the aggregation
    std::vector<Types::Type> fieldTypes;
    std::vector<std::string> fieldNames;

    // Add group field to tuple descriptor if there is grouping
    if (gfield != Aggregator::NO_GROUPING) {
        fieldTypes.push_back(child->getTupleDesc().getFieldType(gfield));
        fieldNames.push_back(child->getTupleDesc().getFieldName(gfield));
    }

    // Add aggregate result field to tuple descriptor
    // For now only handles of type INT_TYPE
    fieldTypes.push_back(Types::INT_TYPE);
    std::string aggFieldName = nameOfAggregatorOp(aop) + "(" + child->getTupleDesc().getFieldName(afield) + ")";
    fieldNames.push_back(aggFieldName);

    aggTupleDesc = TupleDesc(fieldTypes, fieldNames);
}


int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    return child->getTupleDesc().getFieldName(gfield);
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    return nameOfAggregatorOp(aop) + "(" + child->getTupleDesc().getFieldName(afield) + ")";
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;
}

void Aggregate::open() {
    child->open();
    while (child->hasNext()) {
        Tuple nextTuple = child->next();
        aggregator->mergeTupleIntoGroup(&nextTuple);  // Pass a pointer to the tuple
    }
    aggIterator = aggregator->iterator();
    aggIterator->open();
    initialized = true;
}


void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    aggIterator->rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    return aggTupleDesc;
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    aggIterator->close();
    child->close();
    initialized = false;
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    return {child};
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}


