package com.example;

import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosItemIdentity;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class App 
{
    public static final int docsAmount = 1000;

    public static void main( String[] args )
    {

        //
        // Replace <your-cosmos-endpoint>, <your-cosmos-key>, <your-database-name>, and <your-container-name>
        String endpoint = "https://<your-cosmos-endpoint>.documents.azure.com:443/";
        String key = "<your-cosmos-key>";
        
        String databaseName = "<your-database-name>";
        String containerName = "CosmosContainer"; 

        // Create a new CosmosClient instance
        try (CosmosClient client = new CosmosClientBuilder()
                .endpoint(endpoint)
                //.consistencyLevel(ConsistencyLevel.EVENTUAL)
                .key(key)
                .buildClient()) {

            // Get a reference to the database and container
            CosmosDatabase database = client.getDatabase(databaseName);
            CosmosContainer container = database.getContainer(containerName);

            //
            // Add your list of ID's to fetch
            // Example 4 might throw exceptions in the current context if the ID's are not found.
            List<String> ids = new ArrayList<>();

            ids.add("5eea7116-8f00-4bed-a85a-13068c6b8cff");
            ids.add("433c2839-a5e0-4f7c-ae5e-85154d81b1fa");
           
            //
            // Generate test documents
            // Expects container with partion key on /id
            Boolean generateTestData = false;
            if(generateTestData) {
                ArrayList<JsonNode> docs;
                docs = generateDocs(docsAmount);
                createManyItems(docs, container);
            }

            // 
            // Example 1 - Generate a SQL query with the IN clause
            // https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/keywords#in
            sample1_SqlIn(ids, container, false);

            //
            // Example 2 - Generate a SQL query with the array_contains function
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

        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }


    // 
    // Example 1 - Generate a SQL query with the IN clause
    //
    private static void sample1_SqlIn(List<String> ids, CosmosContainer container, Boolean queryOutput) {

        final long startTime = System.currentTimeMillis();
        double totalRequestCharge = 0.0;
        String joinedIds = String.join("','", ids); // Creates a single string of IDs separated by "','"
        String sqlQuery = "SELECT * FROM c WHERE c.id IN ('" + joinedIds + "')";

        // Prepare and Execute the query
        SqlQuerySpec querySpec = new SqlQuerySpec(sqlQuery);     
        Iterator<FeedResponse<Object>> iterator = container.queryItems(querySpec, new CosmosQueryRequestOptions(), Object.class).iterableByPage().iterator();

        while (iterator.hasNext()) {
            FeedResponse<Object> page = iterator.next();
            List<Object> results = page.getResults();

            // Process or print your results as needed
            if(queryOutput) {   
                results.forEach(result -> System.out.println(result.toString()));
            }
            // Accumulate the request charge
            totalRequestCharge += page.getRequestCharge();
        }
        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);

        // Print the total RU charge for the query operation
        System.out.println("Example 1 - Total Request Charge: " + totalRequestCharge + " RUs." + " Duration: " + duration + "ms");
    }


    // 
    // Example 2 - Generate a SQL query with the array_contains function
    //
    private static void sample2_SqlArrayContains(List<String> ids, CosmosContainer container, Boolean queryOutput) {

        final long startTime = System.currentTimeMillis();
        final int[] itemCount = {0};
        final double[] totalRequestCharge = {0.0};

        //Create the SqlParameterList and add your list as a parameter
        SqlParameter params = new SqlParameter("@ids", ids);

        // Prepare your query using array_contains
        SqlQuerySpec querySpec = new SqlQuerySpec(
                    "SELECT * FROM c WHERE array_contains(@ids, c.id, false)",
                    params);

        // Execute the query
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        //options.setMaxDegreeOfParallelism(1);
        //options.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

        CosmosPagedIterable<Object> pagedIterable = container.queryItems(querySpec, options, Object.class);

        // Iterate through the results
        pagedIterable.iterableByPage().forEach(page -> {
            double pageRequestCharge = page.getRequestCharge();
            totalRequestCharge[0] += pageRequestCharge;

            for (Object item : page.getResults()) {
                itemCount[0]++;
                if(queryOutput) { 
                    System.out.println("Example 2 - Result " + itemCount[0] + " ["+page.getRequestCharge()+"RU]: " + item.toString());
                } 
            }
        });

        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);

        System.out.println("Example 2 - Total Request Charge: " + totalRequestCharge[0] + " RUs." + " Duration: " + duration + "ms");

    }


    //
    // Example 3 - Generate a parameterized query with the IN clause
    //
    private static void sample3_SqlInParameterized(List<String> ids, CosmosContainer container, Boolean queryOutput) {

        final long startTime = System.currentTimeMillis();

        //Building the query string with the IN clause
        String queryString = "SELECT * FROM c WHERE c.id IN (";

        // Dynamically adding parameters for each ID in the list
        StringJoiner sj = new StringJoiner(", ");
        for (int i = 0; i < ids.size(); i++) {
            sj.add("@id" + i);
        }
        queryString += sj.toString() + ")";
        System.out.println("Example 3 - Querystring: " + queryString);

        // Create a new SqlQuerySpec
        SqlQuerySpec querySpec = new SqlQuerySpec(queryString);

        // Add parameters to the query spec for each ID
        for (int i = 0; i < ids.size(); i++) {
            querySpec.getParameters().add(new SqlParameter("@id" + i, ids.get(i)));
        }

        // Execute the query
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        CosmosPagedIterable<Object> pagedIterable = container.queryItems(querySpec, options, Object.class);
        
        final int[] itemCount = {0};
        final double[] totalRequestCharge = {0.0};

        // Iterate through the results
        pagedIterable.iterableByPage().forEach(page -> {
            double pageRequestCharge = page.getRequestCharge();
            totalRequestCharge[0] += pageRequestCharge;

            for (Object item : page.getResults()) {
                itemCount[0]++;
                if(queryOutput){
                    System.out.println("Example 3 - " + itemCount[0] + " ["+page.getRequestCharge()+"RU]: " + item.toString());
                }    
            }
        });

        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);
        
        System.out.println("Example 3 - Total Request Charge: " + totalRequestCharge[0] + " RUs." + " Duration: " + duration + "ms");
    }


    //
    // Example 4 - Fetching all the ids with ReadItem() including the partition key
    //
    private static void sample4_SqlApiReadItem(List<String> ids, CosmosContainer container, Boolean queryOutput) { 

        final long startTime = System.currentTimeMillis();
        double totalRequestCharge = 0.0;

        for (String id : ids) {
            try {
                // Perform a point read by specifying the ID and the partition key value (which is the same as the ID in this case)
                CosmosItemResponse<Object> itemResponse = container.readItem(id, new PartitionKey(id), Object.class);
                
                if(itemResponse.getStatusCode() == 200) {
                    // Extract the RU charge for the current operation
                    double requestCharge = itemResponse.getRequestCharge();
                    totalRequestCharge += requestCharge;

                    // Get the item as a generic Object
                    Object item = itemResponse.getItem();
                    
                    // Print the item and the RU charge for this read operation
                    if(queryOutput) {
                        System.out.println(item.toString());
                        System.out.println("Example 4 - Request Charge for this operation: " + requestCharge + " RUs."); 
                    }
                }
            } catch (CosmosException e) {
                // Handle the case where the item is not found or any other Cosmos DB access issue
                    System.err.println("Example 4 - An error occurred during point read: " + e.getMessage());
            }
        }

        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);

        // Print the total RU charge for all read operations
        System.out.println("Example 4 - Total Request Charge: " + totalRequestCharge + " RUs." + " Duration: " + duration + "ms");
    }


    //
    // Example 5 - Fetching all the ids with ReadMany() including the partition key
    //
    private static void sample5_SqlApiReadMany(List<String> ids, CosmosContainer container, Boolean queryOutput) { 
       
        final long startTime = System.currentTimeMillis();

         List<CosmosItemIdentity> itemsToRead = new ArrayList<>();
        for (String id : ids) {
            itemsToRead.add(new CosmosItemIdentity(new PartitionKey(id), id));
        }
        
        //  readMany call to properly handle the FeedResponse<Object> return type
        FeedResponse<Object> feedResponse = container.readMany(itemsToRead, Object.class);
                    
        // Iterate over the items in the response
        if(queryOutput) {
            for (Object item : feedResponse.getResults()) {
                System.out.println(item.toString());
            }
        }
        
        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);

        double requestCharge = feedResponse.getRequestCharge();
        System.out.println("Example 5 - Total Request Charge: " + requestCharge + " RUs." + " Duration: " + duration + "ms");
    }


    //
    // Generate test documents
    public static ArrayList<JsonNode> generateDocs(int N) {
        ArrayList<JsonNode> docs = new ArrayList<JsonNode>();
        ObjectMapper mapper = Utils.getSimpleObjectMapper();

        try {
            for (int i = 1; i <= N; i++) {
                docs.add(mapper.readTree(
                        "{" +
                                "\"id\": " +
                                "\"" + UUID.randomUUID().toString() + "\"" +
                                "}"));
            }
        } catch (Exception err) {
            System.out.println("Failed generating documents: " + err);
        }

        return docs;
    }


    //
    // Add to the Cosmos DB container
    public static void createManyItems(ArrayList<JsonNode> docs, CosmosContainer container) throws Exception {
        AtomicInteger numberDocsInserted = new AtomicInteger(0);

        for (Object doc : docs) {
            try {
                CosmosItemResponse<?> response = container.createItem(doc);
                if (response.getStatusCode() == 201) {
                    numberDocsInserted.getAndIncrement();
                } else {
                    System.out.println("WARNING insert status code " + response.getStatusCode() + " != 201");
                }
            } catch (CosmosException e) {
                System.out.println("Exception occurred during item creation: " + e.getMessage());
            }
        }

        System.out.println(numberDocsInserted.get() + " documents inserted successfully.");
    }
}
