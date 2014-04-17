Hive UDFs
=========

Taking inspiration from Edward Capriolo, this project shows how to perform the rank() and dense_rank() functions
within Hive, taking into account tied ranks. Also available is an implementation of first_value() and a UDTF of parse_key_val_tuple().


Building
--------

This project uses Maven. To build the software, simply use "mvn package".


Deploying within Hive
---------------------

To make the jar available on a temporary basis:

  hive> add jar /home/paul/hive-udf-0.1-SNAPSHOT.jar;

To make the function available on a temporary basis:

  hive> CREATE TEMPORARY FUNCTION rank AS 'com.cloudera.hive.examples.Rank';


Usage: rank(), dense_rank() and first_value()
---------------------------------------------

The rank(), dense_rank() and first_value() functions take a minimum of one parameter - the column that is to be ranked / retrieved. Since the
functions have no ability to sort data for themselves, we use a sub-query to appropriately distribute and sort data before passing to rank().

For example, imagine we have a table such as the following:

select * from items;

<table>
<tr><th>item</th><th>category</th><th>price</th></tr>
<tr><td>Orange</td><td>Fruit</td><td>0.30</td></tr>
<tr><td>Apple</td><td>Fruit</td><td>0.25</td></tr>
<tr><td>Banana</td><td>Fruit</td><td>0.75</td></tr>
<tr><td>Carrot</td><td>Veg</td><td>0.20</td></tr>
<tr><td>Sprout</td><td>Veg</td><td>1.75</td></tr>
<tr><td>Kiwi</td><td>Fruit</td><td>0.30</td></tr>
</table>

To use the rank function, prepare the data with an inner query:

  select item, category, price from items distribute by category sort by category, price;

Then wrap this in another query that applies the rank function:

  select item, category, price, rank(price, category) from (
    select item, category, price from items distribute by category sort by category, price) inner;

<table>
<tr><th>item</th><th>category</th><th>price</th><th>rank</th></tr>
<tr><td>Apple</td><td>Fruit</td><td>0.25</td><td>1</td></tr>
<tr><td>Orange</td><td>Fruit</td><td>0.30</td><td>2</td></tr>
<tr><td>Kiwi</td><td>Fruit</td><td>0.30</td><td>2</td></tr>
<tr><td>Banana</td><td>Fruit</td><td>0.75</td><td>4</td></tr>
<tr><td>Carrot</td><td>Veg</td><td>0.20</td><td>1</td></tr>
<tr><td>Sprout</td><td>Veg</td><td>1.75</td><td>2</td></tr>
</table>

Notice that rank() takes the price column as the first parameter, the rest of the parameters are used to determine the row groupings.


Usage: parse_key_val_tuple()
----------------------------

The parse_key_val_tuple() function is a UDTF that takes a minimum of 4 parameters. The input string to be parsed, the delimiter between all the fields,
the separator between key and value pairs, and 1 to many Keys that you would like to extract. Note that the parameters are case sensitive.

See the following links for Hive UDTF usage instructions

- [Hive Table-GeneratingFunctions(UDTF)](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inTable-GeneratingFunctions\(UDTF\))
- [Hive Lateral Views](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView)

Below are two sample queries that show the functions basic capabilities and perhaps illustrate the outcome of some edge cases:

*Demo Setup*

A one row table called 'dual' is used for selecting demo values statically for demo purposes only.

    echo "X" > dummy.txt
    hive
    CREATE TABLE dual(dummy STRING);
    LOAD DATA LOCAL INPATH 'dummy.txt' OVERWRITE INTO TABLE dual;

    ADD JAR /full/path/to/local/jar/hive-udf-0.1-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION parse_key_val_tuple AS 'com.cloudera.hive.udf.functions.ParseKeyValueTuple';

*Static Parameter Sample:*

    SELECT data.label, t.*
    -- Static data from 'dummy' table for demo
    FROM(
        -- String to show many edge cases
        SELECT 'edge' AS label, 'foo=bar&extra=extra=separator&&empty=&bad&unused=string' AS text FROM dual LIMIT 1
        UNION ALL
        -- Good string
        SELECT 'good' AS label, 'foo=bar&extra=not-extra&&empty=not-empty&bad=not-bad' AS text FROM dual LIMIT 1
        UNION ALL
        -- Empty string
        SELECT 'empty' AS label, '' AS text FROM dual LIMIT 1
        UNION ALL
        -- NULL string
        SELECT 'null' AS label, NULL AS text FROM dual LIMIT 1
        ) data
    LATERAL VIEW parse_key_val_tuple(text, '&', '=', 'foo', 'extra', 'empty', 'bad') t AS foo, extra, empty, bad;


*Dynamic Parameter Sample:*

    SELECT t.*
    -- Static data from 'dummy' table for demo
    FROM(
        -- type = person
        SELECT 'greeting=Hello\;person=Mr. Smith' AS text, 'person' AS type FROM dual LIMIT 1
        UNION ALL
        -- type = thing
        SELECT 'greeting=Hi\;thing=World' AS text, 'thing' AS type FROM dual LIMIT 1
        ) data
    -- lookup name by type
    LATERAL VIEW parse_key_val_tuple(text, '\;', '=', 'greeting', data.type) t AS greeting, name;

