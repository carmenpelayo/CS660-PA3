#include <db/IntegerAggregator.h>
#include <db/IntField.h>
#include <iostream> 
using namespace db;

#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    using MapIterator = std::unordered_map<Field *, int>::const_iterator;

    int groupByFieldIndex;
    TupleDesc tupleDesc;
    std::unordered_map<Field *, int> counts;
    MapIterator current;
    MapIterator end;
public:
    IntegerAggregatorIterator(int gbfield,
                            const TupleDesc &td,
                            const std::unordered_map<Field *, int> &count) {
        groupByFieldIndex = gbfield;
        tupleDesc = td;
        counts = count;
        current = counts.begin();
        end = counts.end();
        // Additional initialization code if needed
    }

    void open() override {
        current = counts.begin();
    }

    bool hasNext() override {
        return current != end;
    }

    Tuple next() override {
        std::cout << "TupleDesc: " << tupleDesc.to_string() << std::endl;
        Tuple tuple = Tuple(tupleDesc);
        if (groupByFieldIndex == Aggregator::NO_GROUPING) {
            tuple.setField(0, new IntField(current->second));

        } else {
            tuple.setField(0, current->first);  // Assuming current->first is already a Field pointer
            tuple.setField(1, new IntField(current->second));
        }

        ++current;
        return tuple;
    }

    void rewind() override {
        current = counts.begin();
    }

    const TupleDesc &getTupleDesc() const override {
        return tupleDesc;
    }

    void close() override {
        current = end;
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield, Op what)
    : groupByFieldIndex(gbfield), groupByFieldType(gbfieldtype), aggregateFieldIndex(afield), aggregateOp(what),
      noGrouping(gbfield == Aggregator::NO_GROUPING), isFirstTuple(true), sum(0), count(0) {

    aggregateValue = 0;

    if (!noGrouping) {
        aggregationResults.clear();
    }
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // Extract the value of the aggregate field from the tuple
    const Field& aggFieldRef = tup->getField(aggregateFieldIndex);
    const IntField* aggField = dynamic_cast<const IntField*>(&aggFieldRef);
    int aggValue = aggField->getValue();

    if (noGrouping) {
        // Handling aggregation without grouping
        switch (aggregateOp) {
            case Aggregator::Op::SUM:
                aggregateValue += aggValue;
                break;
            case Aggregator::Op::MIN:
                aggregateValue = std::min(aggregateValue, aggValue);
                break;
            case Aggregator::Op::MAX:
                aggregateValue = std::max(aggregateValue, aggValue);
                break;
            case Aggregator::Op::COUNT:
                aggregateValue++;
                break;
            case Aggregator::Op::AVG:
                sum += aggValue;
                count++;
                break;
        }
        isFirstTuple = false;
    } else {
        // Handling aggregation with grouping
        const Field& groupFieldRef = tup->getField(groupByFieldIndex);
        const IntField* groupField = dynamic_cast<const IntField*>(&groupFieldRef);
        int groupValue = groupField->getValue();

        // Retrieve or initialize the current group's aggregate result
        auto& result = aggregationResults[groupValue];
        switch (aggregateOp) {
            case Aggregator::Op::SUM:
                result += aggValue;
                break;
            case Aggregator::Op::MIN:
                result = (result == 0 || isFirstTuple) ? aggValue : std::min(result, aggValue);
                break;
            case Aggregator::Op::MAX:
                result = (result == 0 || isFirstTuple) ? aggValue : std::max(result, aggValue);
                break;
            case Aggregator::Op::COUNT:
                result++;
                break;
            case Aggregator::Op::AVG:
                avgResults[groupValue].first += aggValue; 
                avgResults[groupValue].second++;
                break;
        }
        isFirstTuple = false;
    }
}


DbIterator* IntegerAggregator::iterator() const {
    std::unordered_map<Field*, int> formattedResults;

    TupleDesc newTupleDesc;

    if (noGrouping) {
        newTupleDesc = TupleDesc({Types::INT_TYPE});
        if (aggregateOp == Aggregator::Op::AVG) {
            int avgValue = count > 0 ? sum / count : 0; // Protect against division by zero
            formattedResults[new IntField(0)] = avgValue;
        } else {
            formattedResults[new IntField(0)] = aggregateValue;
        }
    } else {
        newTupleDesc = TupleDesc({groupByFieldType.value(), Types::INT_TYPE});
        for (const auto& pair : aggregationResults) {
            if (aggregateOp == Aggregator::Op::AVG) {
                const auto& sumCount = avgResults.at(pair.first); // Use at() for const access
                int avgValue = sumCount.second > 0 ? sumCount.first / sumCount.second : 0;
                formattedResults[new IntField(pair.first)] = avgValue;
            } else {
                formattedResults[new IntField(pair.first)] = pair.second;
            }
        }
    }

    return new IntegerAggregatorIterator(groupByFieldIndex, newTupleDesc, formattedResults);
}

