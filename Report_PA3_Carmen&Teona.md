### CS 660 - Programming Assignment 3 - Report

For CS660’s programming assignment 4, we, Carmen Pelayo and Teona Bagashvili, worked together, splitting the work equally.
Carmen completed exercise 1 (the `Filter` and `Join` functionalities), while Teona completed exercise 2 (the `Aggregate` functionality).
As for exercise 3, Carmen built the `Insert` while Teona made the `Delete`. We worked together to debug the `Join` functionality and write
new unit tests to verify the correctness of our implementation. This work took a total of 20 hours to complete. 

## Filter

In our database system, the `Filter` and `Join` functionalities are pivotal components, implemented through several classes with
distinct responsibilities. The Filter class, defined in *Filter.cpp* and *Filter.h*, embodies the relational select operation.
It integrates a Predicate and a child DbIterator, applying the predicate to each tuple yielded by the child iterator. This mechanism
allows for effective data filtering based on specified criteria. 

## Join

The core of the `Join` functionality is encapsulated in *Join.cpp* and *Join.h*, where the Join class uses two child iterators and a
JoinPredicate to execute the join operation. The JoinPredicate class, detailed in *JoinPredicate.cpp* and *JoinPredicate.h*, is crucial
for comparing tuple fields using specified predicates, thereby guiding the join process. We further specialized our join capability
in *HashEquiJoin.cpp* and *HashEquiJoin.h* through the HashEquiJoin class, which implements a hash-based equi-join. This approach
significantly optimizes performance for equi-join scenarios by utilizing hash tables for efficient data matching. 

## Aggregate

Our `IntegerAggregator` class is responsible for processing and aggregating integer values from tuples. In our implementation,
we have support for SUM, MIN, MAX, COUNT, and AVG. Our IntegerAggregator class supports both grouped and ungrouped aggregations.
In grouped aggregation, values are aggregated within groups identified by a specific field, while in ungrouped aggregation,
all values are aggregated together. When grouping is not used (noGrouping is true), the aggregation results are stored in a
single field. In the case of grouping, the aggregation results are stored alongside their respective group values. For AVG operation,
we maintain sum and count to calculate the average when the iterator is requested.

## Insert

The `Insert` class is designed to handle the insertion of tuples into a specified table, with the operation conducted under the purview of a transaction, denoted by the TransactionId. The process begins by sourcing tuples from a child DbIterator, an approach that provides flexibility in the origin of data. Each tuple is subsequently inserted into the designated table, as identified by the tableId. This simple yet effective mechanism is a cornerstone of our database's data handling capability, ensuring that data insertion is both efficient and consistent with the overall transactional integrity of the system. The implementation also includes a counter for the number of tuples inserted, a feature that is used for both transaction management and data integrity verification. 


## Delete

Our `Delete` functionality is designed as follows. After running the open method, the class starts processing by opening the
child iterator. The fetchNext method iterates over all tuples provided by the child iterator, deleting each one using the
BufferPool's deleteTuple method, which performs the actual deletion in the database. It keeps track of the number of records
deleted. Once all records are processed, it returns a tuple containing the count of deleted records. 

## Tests

The provided unit tests only tested the `Filter`, `Join`, and `Aggregate` ungrouped SUM functionalities. Therefore we added new unit
tests to ensure the correctness of our approach.

- MIN: test checks that in the ungrouped case the returned minimum value for the first column is 0.
- MAX: test checks that in the ungrouped case the returned maximum value for the first column is 69.
- AVG: test checks that in the ungrouped case the returned average value of the first column is 34 (rounded down).
- SUM (grouped): test was run when we group by first column and sum third column. In the third column, the values are only `2`.
  therefore the output matches our expected outcome: `std::unordered_map<int, int> expectedSums = {{0, 140}, {1, 140}, {2, 140}, {3, 140}, {4, 140}};`
- HashEquiJoin: In this test the condition is that the values in the first column need to be greater than or equal to 1, and the expected result matches the actual outcome `1750`.
- Insert:
- Delete:

We did not upload the updated test files on Gradescope since we are only supposed to upload the files contained in `db` and `ìnclude`, but we can provide these files upon request.

## Challenges

No changes were made to the API. We did not encounter major challenges but a small issue was encountered during the implementation of the Join operation. The issue involved a segmentation fault when merging tuple descriptors from two child iterators. The segmentation fault occurred when directly returning the result of *TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc())* in the `Join::getTupleDesc` method. The problem was resolved by introducing a mutable member variable mergedTupleDesc in the Join class. This allowed the method to first assign the merged tuple descriptor to mergedTupleDesc and then return a reference to this member variable.

In conclusion, this assignment was a rich learning experience, offering a deep dive into the mechanisms of table filtering, management and updating, even though it presented its own set of challenges and complexities. 
