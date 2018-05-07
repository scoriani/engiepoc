az login

## create CosmosDB account
az cosmosdb create --resource-group testengie --kind GlobalDocumentDB --name engiecosmosdb

## take note of keys for next steps and to configure connection string for ingestion app
az cosmosdb list-keys -g testengie --name engiecosmosdb

## Create database container
az cosmosdb database create -g testengie --name engiecosmosdb --key <insertkey> --db-name dbengiepoc

# Create partitioned collection with high throughput
az cosmosdb collection create -g testengie --collection-name ingestion --name engiecosmosdb --key <insertkey> --db-name dbengiepoc --throughput 100000 --partition-key-path /partitionKey
