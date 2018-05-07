az login

az storage account create -g testengie --name engieaccount --kind StorageV2 --location westeurope --sku Standard_LRS

az storage account show-connection-string -g testengie --name engieaccount

## Take note of connection string to configure task projects
