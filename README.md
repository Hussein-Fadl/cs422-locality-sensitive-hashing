# cs422-locality-sensitive-hashing
This project implements a large-scale data processing pipeline over IMDB dataset for rating aggregation and similarity search.

## Overview
Given a dataset based on **IMDB** (Internet Movie Database). The dataset includes: 
1. A set of titles (e.g. movies, TV series) and their related keywords.
2. A log of "user rates movie" actions.

A video streaming application requires large-scale data processing over the given dataset. The application has a user-facing component (we are not concerned with) that serves recommendations and interesting statistics to the end-users. Specifically, for each title, it displays:
* All the movies that are "similar" in terms of keywords
* A differentially-private average rating

In the background, the application issues Spark jobs that pre-compute the information that is served. 


## 1. Load Bulk Data
We implement the `load()` methods for `TitlesLoader` and `RatingsLoader`, which load data from `titles.csv` and `ratings.csv` respectively. We read the CSV files and convert rows into the corresponding tuples. 

After converting the data into the appropriate RDD, load() also persists the RDDs in memory to avoid reading and parsing the files for every data processing operation.

## 2. Average rating pipeline
The application needs to display an average rating for each title. This pipeline aggregates ratings in order to compute the average rating for all available titles (and display `0.0` for titles that haven’t been rated yet). Furthermore, it enables ad-hoc queries that compute the average rating across titles that are related to specific combinations of keywords. 

Finally, when a batch of new ratings is provided by the application, the pipeline incrementally maintains existing aggregate ratings. All the functionality is implemented in the **Aggregator** class. In the rest of the Section, we discuss how the pipeline is implemented.

### 2.1 Rating Aggregation
The ratings log includes all the "user (re-)rates movie" actions that the application has registered. The `init()` computes the average rating for each title by aggregating the corresponding actions. Titles with no ratings have a `0.0` rating. For each title in the aggregate result, we maintain the related keywords. `init()` persists the resulting RDD **in-memory** to avoid recomputation for every data processing operation.

To retrieve the result of the aggregation, the applications need to use the `getResult()` method which returns the title name followed by the rating.

### 2.2 Ad-hoc keyword rollup
The `getKeywordQueryResult()` implements queries over the aggregated ratings. The parameter of the queries is a *set of keywords* and the queries compute **the average rating among the titles that contain all of the given keywords**. 

The average of averages excludes unrated titles and all titles contribute equally. The result is returned to the driver which then returns it to the end-user. If all titles are unrated the query returns `0.0`, whereas if no titles exist for the given keywords the query returns `−1.0`.

### 2.3 Incremental maintenance
The application periodically applies batches of *append-only* updates in the batches in the `updateResult()` method. Updates are provided as arrays of rating tuples accordingly. 

Given the aggregates before the update and the array of updates, the method computes new updated aggregates that include both old and new ratings. We persist the resulting RDD in memory to avoid additional recomputation and unpersist the old version.

## 3. Similarity-search pipeline
The application identifies titles that are "similar" in each user’s history to make recommendations. In order to retrieve "similar" titles, it batches a large number of **near-neighbor queries** i.e. queries that return titles with similar keywords. Then, using **locality-sensitive hashing (LSH)**, it computes the near-neighbors, which are pushed to interested users.


In the rest of the Section, we describe the implementation concretely. First, we provide a brief introduction of LSH, which contains all the required information for implementing the project. Task 3.1 describes indexing titles to support distributed LSH lookups. Task 3.2 describes the implementation of distributed LSH lookups themselves. Task 3.3 describes a caching mechanism to reduce the cost of lookups. Finally, Task 3.4 describes a simple cache policy which you will implement in the project.

### 3.1 Indexing the dataset
To identify signature matches efficiently, data needs to be organized accordingly. The **LSHIndex** class restructures the title RDD so that subsequent lookups can find matches with the same signature efficiently. This requires the following steps, which are implemented in the constructor:
1. **Hashing**: In the first step, we use the provided hash function to compute the signature for each title. The signature is used to label the title there upon.
2. **Bucketization**: In the second step, we cluster titles with the same signature together. Suppose our RDD consists of the following five annotated titles:

```
            (555, "LotR", ["ring"], 5)
            (556, "Star Wars", ["space"], 3)
            (557, "The Hobbit", ["ring"], 5)
            (558, "Inception", ["dream"], 1)
            (559, "Star Trek", ["space"], 3)
```
Then, bucketization should result in the following data structure:
```
             (1, [(558, "Inception", ["dream"])])
             (3, [(559, "Star Trek", ["space"]), (556, "Star Wars", ["space"])])
             (5, [(557, "The Hobbit", ["ring"]), (555, "LotR", ["ring"])])
```

### 3.2 Near-neighbor lookups
At runtime, the application submits batches of near-neighbor queries for processing. Each query is expressed as a list of keywords. Computing the near-neighbors requires the following steps:
1. **Hashing:** We use the provided hash function to compute the signature for each query. The signature is used to label the query thereupon.
2. **Lookup:** The signature is used to retrieve the set of titles with the same signature as the query. This will result in a shuffle for the given queries.

We implement the `lookup()` method in **LSHIndex** which performs signature matching against the constructed buckets. Also, we implement `lookup()` in class NNLookup which handles hashing and also uses `LSHIndex.lookup()` for matching.

### 3.3 Near-neighbor cache
Each batch of queries requires shuffling the queries which is expensive. For this reason, a cache is added to the pipeline. The cache is broadcasted/replicated across Spark workers and includes the near-neighbors from frequently requested signatures. Then, each worker can immediately compute the results of its local queries that hit the cache. Distributed lookups occur only for queries that result in cache misses. The results are merged and returned to the applications.

We implement the logic for exploiting the cache in `cacheLookup()` method in class **NNLookupWithCache**. Specifically, we use the cache to separate queries that result in a **cache hit** and queries that result in a **cache miss**. Then, for queries that hit the cache, we retrieve and return the near-neighbors. If there is no cache, the RDD of hits is null.

Moreover, we implement the `lookup()` in **NNLookupWithCache**, which combines `cacheLookup()` and the `lookup()` method of **LSHIndex** to compute the full result for the queries.

Finally, for testing purposes, we implement the `buildExternal()` method of `NNLookupWithCache` which receives a broadcast object that should be used as a cache for testing purposes.

### 3.4 Cache Policy
The cache needs to be populated and distributed to the workers. In this subtask, we implement two components:
1. **Bookkeeping**: We measure how frequently each signature occurs among queries. We assume that the number of distinct signatures is low and that a histogram can fit in memory. We intercept the signatures of the queries in `cacheLookup()`, and update a histogram for occurrences of signatures.
2. **Cache creation**: The cache creation algorithm computes signatures that occur in more that 1% (> 1%) of the queries. Then, the buckets for these frequent signatures are retrieved, merged into one data structure and broadcasted to workers. Subsequently, the histogram data structure is reset. The algorithm is implemented in the `build()` method of `NNLookupWithCache`.
