az login

## create CosmosDB account
az cosmosdb create -g testengie --locations westeurope --kind GlobalDocumentDB --name engiecosmosdb

## take note of keys for next steps and to configure connection string for ingestion app
az cosmosdb list-keys -g engie --name ingestiontest

## Create database container
az cosmosdb database create -g testengie --name dbaccount --key <insertkey> --db-name dbname

# Create partitioned collection with high throughput
az cosmosdb collection create -g testengie --name ingestion --name dbaccount --key <insertkey> --db-name dbname --throughput 100000 --partition-key-path /partitionKey