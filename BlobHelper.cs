using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace AnalysisServerDataExtract
{
    class BlobHelper
    {
        private string connectionString = "DefaultEndpointsProtocol=https;AccountName=account_name;AccountKey=account_key;EndpointSuffix=core.windows.net";
        private string containerName = "azure-blob-container-name";

        public BlobHelper()
        {
        }

        public BlobHelper(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void setContainerName(string containerName)
        {
            this.containerName = containerName;
        }

        public void writeCsvToBlob(List<string> tableContent, string fileName)
        {
            bool overwrite = true;

            // Write list to azure blob
            BlobContainerClient container = new BlobContainerClient(connectionString, containerName);
            BlobClient blob = container.GetBlobClient(fileName);

            byte[] dataAsBytes = tableContent.SelectMany(row => System.Text.Encoding.UTF8.GetBytes(row + "\n")).ToArray();

            var memoryStream = new MemoryStream(dataAsBytes);

            blob.Upload(memoryStream, overwrite);
        }

        public void writeLocalCsvToBlob(List<string> tableContent, string localFilePath, string blobfileName)
        {
            bool overwrite = true;

            BlobContainerClient container = new BlobContainerClient(connectionString, containerName);
            BlobClient blob = container.GetBlobClient(blobfileName);

            blob.Upload(localFilePath, overwrite);
        }
    }
}
