az login

az storage account create -g testengie --name engieaccount --kind StorageV2 --location westsurope --sku Standard_LRS --kind BlobStorage

az storage account show-connection-string -g testengie --name testengie

## Take note of connection string to configure task projects