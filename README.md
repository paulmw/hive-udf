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

<table>
<tr><th>item</th><th>category</th><th>price</th></tr>
<tr<td>Orange</td><td>Fruit</td><td>0.30</td></tr>
<tr<td>Apple</td><td>Fruit</td><td>0.25</td></tr>
<tr<td>Banana</td><td>Fruit</td><td>0.75</td></tr>
<tr<td>Carrot</td><td>Veg</td><td>0.20</td></tr>
<tr<td>Sprout</td><td>Veg</td><td>1.75</td></tr>
<tr<td>Kiwi</td><td>Fruit</td><td>0.30</td></tr>
</table>

To use the rank function, prepare the data with an inner query:

  select item, category, price from items distribute by category sort by category, price;

Then wrap this in another query that applies the rank function:

  select item, category, price, rank(price, category) from (
    select item, category, price from items distribute by category sort by category, price) inner;

<table>
<tr><th>item</th><th>category</th><th>price</th><th>rank</th></tr>
<tr<td>Apple</td><td>Fruit</td><td>0.25</td><td>1</td></tr>
<tr<td>Orange</td><td>Fruit</td><td>0.30</td><td>2</td></tr>
<tr<td>Kiwi</td><td>Fruit</td><td>0.30</td><td>2</td></tr>
<tr<td>Banana</td><td>Fruit</td><td>0.75</td><td>4</td></tr>
<tr<td>Carrot</td><td>Veg</td><td>0.20</td><td>1</td></tr>
<tr<td>Sprout</td><td>Veg</td><td>1.75</td><td>2</td></tr>
</table>

Notice that rank() takes the price column as the first parameter, the rest of the parameters are used to determine the row groupings.