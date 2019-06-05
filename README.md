# Overview

`Spark Tasks` is a simple solution for problems that we need to solve using `Apache Spark` on given Dataset.

## Dataset (example)

Category  |  Product             |  User Id   |  Event Time          |  Event Type
----------|----------------------|------------|----------------------|--------------
books     |  Scala for Dummies   |  user 1    |  2018-03-01 12:00:02 | like
books     |  Scala for Dummies   |  user 1    |  2018-03-01 12:01:40 | check status
books     |  Java for Dummies    |  user 1    |  2018-03-01 12:01:50 | view description
books     |  Romeo and Juliet    |  user 2    |  2018-03-01 12:02:45 | add to bucket

## Problem descriptions

### Problem #1

We need to enrich incoming data (dataset) with user sessions. By session we mean consecutive events that belong to a single category and are not more than 5 minutes away from each other.

Expected output:
```category, eventTime, eventType, ..., sessionId, sessionStartTime, sessionEndTime```

### Problem #2

We also need to compute the following statistics:
* for each category find median session duration
* for each category find number of unique users spending less than 1 minute, 1 to 5 minutes and more than 5 minutes
* for each category find top 10 products ranked by time spent by users on product pages; for this particular task, session lasts until the user is looking at particular product, when particular user switches to another product the new session starts

# Technologies

The solution has been made on top of such technologies as:

1. Scala
2. Apache Spark (DataFrame API, Window Functions, DataSet Aggregators)

## How to run

To run application you just need to open `sbt` shell and execute `run` command:

```bash
$> run
```

As a result you will see application output in console.

## How to test

Open `sbt` shell and execute `test` command:

```bash
$> test
``` 
