#include <db/Filter.h>

using namespace db;



Filter::Filter(Predicate p, DbIterator *child) : predicate(p) {
    this->child = child;
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return &this->predicate;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return child->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child->open();

}

void Filter::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    child->close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    child->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    std::vector<DbIterator *> children;
    children.push_back(child);
    return children;
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size() != 1) {
        throw std::invalid_argument("Expected exactly one child");
    }
    child = children[0];
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    while (child->hasNext()) {
        Tuple iterateTuple = child->next();
        if (predicate.filter(iterateTuple)) {
            return iterateTuple;
        }
    }
    return std::nullopt;
}