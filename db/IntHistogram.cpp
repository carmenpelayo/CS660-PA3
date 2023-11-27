#include "db/IntHistogram.h"

using namespace db;

IntHistogram::IntHistogram(int buckets, int min, int max)
        : num_buckets(buckets), min(min), max(max),
          buckets(buckets, 0), totalNumber(0),
          width(static_cast<int>(std::ceil(static_cast<double>(max - min + 1) / num_buckets))) {}

void IntHistogram::addValue(int v) {
    // TODO pa4.1: some code goes here
    int bucket = (v - this->min) / width;
    buckets[bucket]++;
    totalNumber++;
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    int bucket = (v - this->min) / width;
    if (bucket < 0 || bucket >= num_buckets) {
        return 0.0;
    }

    auto SelectivityEq = [&](int b) -> double {
        if (b < 0 || b >= num_buckets)
            return 0.0;
        return static_cast<double>(buckets[b]) / width / totalNumber;
    };

    auto SelectivityNeq = [&](int b, Predicate::Op operation, int val) -> double {
        int right, left, height, f;
        if (b < 0) {
            right = 0;
            left = -1;
            f = 0;
            height = 0;
        } else if (b >= num_buckets) {
            right = num_buckets;
            left = num_buckets - 1;
            f = 0;
            height = 0;
        } else {
            right = b + 1;
            left = b - 1;
            f = -1;
            height = buckets[b];
        }
        double resultSelectivity = 0.0;

        if (operation == Predicate::Op::GREATER_THAN) {
            if (f == -1) {
                f = ((right * width) + this->min - val) / width;
            }
            resultSelectivity = static_cast<double>(height * f) / totalNumber;
            if (right >= num_buckets)
                return resultSelectivity;
            for (int i = right; i < num_buckets; i++) {
                resultSelectivity += buckets[i];
            }
            return resultSelectivity / totalNumber;
        } else if (operation == Predicate::Op::LESS_THAN) {
            if (f == -1) {
                f = (val - (left * width) + this->min) / width;
            }
            resultSelectivity = static_cast<double>(height * f) / totalNumber;
            if (left < 0)
                return resultSelectivity;
            for (int i = left; i >= 0; i--) {
                resultSelectivity += buckets[i];
            }
            return resultSelectivity / totalNumber;
        } else {
            return -1.0;
        }
    };

    switch (op) {
        case Predicate::Op::EQUALS:
        case Predicate::Op::LIKE:
            return SelectivityEq(bucket);
        case Predicate::Op::GREATER_THAN:
        case Predicate::Op::LESS_THAN:
            return SelectivityNeq(bucket, op, v);
        case Predicate::Op::GREATER_THAN_OR_EQ:
            return SelectivityEq(bucket) + SelectivityNeq(bucket, Predicate::Op::GREATER_THAN, v);
        case Predicate::Op::LESS_THAN_OR_EQ:
            return SelectivityEq(bucket) + SelectivityNeq(bucket, Predicate::Op::LESS_THAN, v);
        case Predicate::Op::NOT_EQUALS:
            return 1.0 - SelectivityEq(bucket);
        default:
            return -1.0;
    }
}

double IntHistogram::avgSelectivity() const {
    // TODO pa4.1: some code goes here
    return 1.0;
}

std::string IntHistogram::to_string() const {
    std::string result;
    for (int i = 0; i < num_buckets; i++) {
        result += std::to_string(i) + "bucket: ";
        for (int j = 0; j < buckets[i]; j++) {
            result += "|";
        }
        result += '\n';
    }
    return result;
}
