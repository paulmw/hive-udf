Hive UDFs
=========

Taking inspiration from Edward Capriolo, this project shows how to perform the rank() and dense_rank() functions
within Hive, taking into account tied ranks. Also available is an implementation of first_value().


Building
--------

This project uses Maven. To build the software, simply use "mvn package".


Deploying within Hive
---------------------

To make the jar available on a temporary basis:

  hive> add jar /home/paul/hive-udf-0.1-SNAPSHOT.jar;

To make the function available on a temporary basis:

  hive> CREATE TEMPORARY FUNCTION rank AS 'com.cloudera.hive.examples.Rank';


Usage
-----

The rank(), dense_rank() and first_value() functions take a minimum of one parameter - the column that is to be ranked / retrieved. Since the
functions have no ability to sort data for themselves, we use a sub-query to appropriately distribute and sort data before passing to rank().

For example, imagine we have a table such as the following:

select * from items;

+--------+----------+-------+
| item   | category | price |
+--------+----------+-------+
| Orange | Fruit    | 0.30  |
| Apple  | Fruit    | 0.25  |
| Banana | Fruit    | 0.75  |
| Carrot | Veg      | 0.20  |
| Sprout | Veg      | 1.75  |
| Kiwi   | Fruit    | 0.30  |
+--------+----------+-------+

To use the rank function, prepare the data with an inner query:

  select item, category, price from items distribute by category sort by category, price;

Then wrap this in another query that applies the rank function:

  select item, category, price, rank(price, category) from (
    select item, category, price from items distribute by category sort by category, price) inner;

+--------+----------+-------+------+
| item   | category | price | rank |
+--------+----------+-------+------+
| Apple  | Fruit    | 0.25  | 1    |
| Orange | Fruit    | 0.30  | 2    |
| Kiwi   | Fruit    | 0.30  | 2    |
| Banana | Fruit    | 0.75  | 4    |
| Carrot | Veg      | 0.20  | 1    |
| Sprout | Veg      | 1.75  | 2    |
+--------+----------+-------+------+

Notice that rank() takes the price column as the first parameter, the rest of the parameters are used to determine the row groupings.