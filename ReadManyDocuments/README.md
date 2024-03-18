# Read Multiple Documents
Script with 5 examples to fetch multiple documents (based on WHERE [column] IN (..) pattern). 

```
// Example 1 - Generate a SQL query with the IN clause
// https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/keywords#in
sample1_SqlIn(ids, container, false);

//
// Exmaple 2 - Generate a SQL query with the array_contains function
// https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/array-contains
sample2_SqlArrayContains(ids, container, false);

//
// Example 3 - Generate a parameterized query with the IN clause
// https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/parameterized-queries
sample3_SqlInParameterized(ids, container, false);

//
// Example 4 - Fetching all the ids with ReadItem() including the partition key
// https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/samples-java#item-examples
sample4_SqlApiReadItem(ids, container, false);

//
// Example 5 - Fetching all the ids with ReadMany() including the partition key
// https://devblogs.microsoft.com/cosmosdb/read-many-items-fast-with-the-java-sdk-for-azure-cosmos-db/
sample5_SqlApiReadMany(ids, container, false);
```

Using the following Cosmos DB related Maven dependency:
```
    <dependency>
      <groupId>com.azure</groupId>
      <artifactId>azure-cosmos</artifactId>
      <version>4.11.0</version>
    </dependency>
```
