# AQL [![Build Status](https://travis-ci.org/JPDSousa/AQL.svg?branch=master)](https://travis-ci.org/JPDSousa/AQL)


An SQL-like interface for [AntidoteDB](https://github.com/SyncFree/antidote).

## Installation

AQL can be used through Docker (recommended), or directly through rebar3.

### Docker

To use AQL through Docker simply pull the image from Docker Hub
[here](https://hub.docker.com/r/pedromslopes/aql/). If docker can't 
pull the lastest version of the image, do it manually by:

```
    docker pull pedromslopes/aql:<latest-version-available>
```

This image starts an AntidoteDB server 
(uses [this](https://hub.docker.com/r/pedromslopes/antidotedb/) image) on
background and runs an AQL instance of top.

This is the recommended way of running AQL, at least for single-client use.

### Rebar3
This project can also be installed "manually" through rebar3. In order to do so,
clone this repository and checkout this branch:

```
    $ git clone https://github.com/pedromslopes/AQL
    $ cd AQL && git checkout new_features
```

Open a terminal in the project folder (`cd AQL`) and then compile the project:

```
    $ make compile
```

Now, to run the client, you must have one (or more) running instances of AntidoteDB.
To start in shell mode run one of the following commands:

```
    $ make aqlshell
```
or
```
    $ make shell
```
The last command starts a Rebar3 shell which in turn initializes an HTTP server for AQL in background.

You can also start in development mode with:
```
    $ make dev
```

## Getting started

AQL is an SQL-variant, designed to work with AntidoteDB API.

### Shell
AQL provides a shell mode, which is the easiest way to use the
client. In shell mode (command `make aqlshell`), you'll see a prompt like this:
```
    AQL>
```
Use the prompt to input AQL statements.

### API

The AQL API is pretty straightforward. There is a main module called
`aqlparser` with a method `parse`, which takes a tuple as well as the AntidoteDB node long name. The two options
available are:
```Erlang
{str, AQLCommand}
```
Which parses the `AQLCommand` and outputs the result.
```Erlang
{file, AQLFile}
```
Which takes a path to a file (no specific format) and parses its content as an
AQL command.

For instance, to show all existing tables in the database through the API, use:
```Erlang
aqlparser:parse({str, "SHOW TABLES"}, 'antidote@127.0.0.1').
```
This API can only be used on the second shell alternative (the Rebar3 shell mode) and on the development mode.

## AQL Docs

AQL supports multiple SQL-like operations such as:
* Data definition
  * CREATE TABLE
* Data manipulation
  * SELECT
  * INSERT
  * UPDATE
  * DELETE
* Admin
  * SHOW TABLES/INDEX
* Transactions
  * BEGIN
  * COMMIT
  * ABORT

AQL supports a limited set of types:
* VARCHAR - common text data type (similar to SQL's VARCHAR)
* INTEGER/INT - common integer data type (similar to SQL's INTEGER)
* BOOLEAN - common boolean
* COUNTER_INT - integer counter, with bounds
(based on AntidoteDB's
[Bounded Counter](http://www.gsd.inesc-id.pt/~rodrigo/srds15.pdf))

### CREATE TABLE

Creates a new table. If the table already exists the new table will overwrite it
 (any concurrent conflicts will be resolved with a *Last Writer Wins* CRP).

```SQL
CREATE @AW TABLE Student (
	StudentID INT PRIMARY KEY,
	Name VARCHAR,
	Age INT DEFAULT 18,
	YearsLeft COUNTER_INT CHECK (YearsLeft > 0),
	Passport_id INTEGER FOREIGN KEY @UPDATE-WINS REFERENCES Passport(id)
);
```

#### Primary keys

The primary key constraint must be specified after the column which is to be
set as the primary key (multiple columns as primary keys are not supported).
Any datatype can be a primary key.

Primary keys only guarantee uniqueness. Although, if two rows with the same 
primary key are inserted (either concurrently or not), both insertions will be
merged, and all columns will also be merged according to its datatypes.

#### Check Constraints

AQL also supports constraints on counters (`counter_int`). Assign numeric bounds
to any `COUNTER_INT` column by:
```SQL
CHECK (column_name [ > | < ] value)
```

Where `column_name` is the column and `value` is the respective bound.

#### Default Values

You can also define a default value for a record (not allowed in primary keys).
Default values are used when no value is specified for a record.

Syntax:
```SQL
column_x data_type DEFAULT value
```

Where `value` is the default value.

#### Foreign Keys

Foreign keys allow users to create custom relations between elements of different
tables. To create a foreign key relation simply add to the column that will be 
the foreign key: `FOREIGN KEY [ @UPDATE-WINS | @REMOVE-WINS ] REFERENCES parentTable(parentColumn) [ ON DELETE CASCADE ]`, 
where `parentTable` is the parent table name (e.g. `Passport`) and `parentColumn` is
the parent column name (e.g. `id`). All foreign keys must point to columns with a 
unique constraint, which is only guaranteed in primary keys.

Additionally you can define a row's behaviour upon a parent deletion through the notation
`ON DELETE CASCADE`, which tells a record to be removed if its parent row is deleted.
The absence of this notation implies that the parent cannot be deleted if one or more rows point to it.

Update-wins (`@UPDATE-WINS`) and Remove-wins (`@REMOVE-WINS`) are conflict resolution policies used
to resolve any referential integrity related conflicts generated by concurrent operations.
Update-wins will revive all records (deleted) involved in the conflict, while
Remove-wins deletes all involved records in case of conflict.

### SELECT

SELECT is the main read operation in AQL (similar to SQL). The operation issues
a read operation in the database engine (AntidoteDB).
```SQL
SELECT * FROM Student WHERE StudentID = 20;
```

This operation supports conjunctions (`AND`) and disjunctions (`OR`), and parenthesis to group sub queries.
A sub query may be a sequence of one or more comparisons on the form:
```SQL
column_name [ = | <> | < | <= | > | >= ] value
```

### INSERT

Inserts new records in a table. If a value with the primary key already exists it
 will be overwritten.
```SQL
INSERT INTO (StudentID, Name, Age, YearsLeft, Passport_id) VALUES (10000, 'John', 'Smith', '24', 'ABC');
```

The table columns may be omitted, in which case all columns will be considered on the insertion.
*Note*: string values must always be between single quotes (`'`).

### UPDATE

Updates an already-existent row on the specified table.

```SQL
UPDATE Student
SET Age ASSIGN 25
WHERE StudentID = 10000;
```

Updates all rows in table `Students` where `StudentID` has value 1. The update
sets column `Age` to value `25`. The operation keyword (e.g.
`ASSIGN`) depends on the AQL datatype:
* *VARCHAR*
  * `ASSIGN` - sets the column(s) of type `VARCHAR` to the value specified.
* *INTEGER*
  * `ASSIGN` - sets the column(s) of type `INTEGER` or `INT` to the value specified.
* *COUNTER_INT*
  * `INCREMENT` - increments the column(s) of type `COUNTER_INT` by the value specified.
  * `DECREMENT` - decrements the column(s) of type `COUNTER_INT` by the value specified.
* *BOOLEAN*
  * `ENABLE`- sets the boolean value to `true`
  * `DISABLE`- sets the boolean value to `false`
  
Just like in a SELECT operation, the where clause can only filter primary keys.
  
### DELETE

Deletes a set of records from the specified table.

```SQL
DELETE FROM Persons Where StudentID = 20525;
```

Just like in a SELECT operation, the where clause can only filter primary keys.
If the `WHERE`clause is absent, all the records in the table are deleted.


### TRANSACTION

Just like in SQL, AQL allows to execute a set of queries inside a transaction.
```SQL
BEGIN TRANSACTION;
query_1;
query_2;
...
query_n;
[ COMMIT | ABORT ] TRANSACTION;
```

At the end, the transaction can be committed or aborted.
An ongoing transaction must always be terminated first before starting a new one.
