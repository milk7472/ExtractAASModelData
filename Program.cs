using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnalysisServerDataExtract
{
    class Program
    {
        /// <summary>
        /// Get configure file contents
        /// </summary>
        public class Item
        {
            public string storage_key1 = String.Empty;
            public string storage_key2 = String.Empty;
            public string storage_connection_string1 = String.Empty;
            public string storage_connection_string2 = String.Empty;
            public string model_01 = String.Empty;
        }

        static async Task Main(string[] args)
        {
            string modelName = Console.ReadLine();

            string outputFolderName = "AAS_OUTPUT";

            Console.WriteLine("Start to extract model tables to csv, please wait...");
            Console.WriteLine("Please make sure you have folder under C disk.");

            string serverName = "asazure://southeastasia.asazure.windows.net/aas_server_name";
            string tenantId = "00000000-0000-0000-0000-000000000000";
            string appId = "00000000-0000-0000-0000-000000000000";
            string appSecret = "app_secret";

            // 取得storage金鑰
            List<Item> items = null;

            using (StreamReader r = new StreamReader("C:/config.json"))
            {
                string json = r.ReadToEnd();
                items = JsonConvert.DeserializeObject<List<Item>>(json);
            }

            BlobHelper blobHelper = new BlobHelper(items[0].storage_connection_string2);

            // Connect to AAS server and extract all tables to list
            AASHelper aasHelper = new AASHelper(tenantId, appId, appSecret, serverName, modelName);
            string accessToken = await aasHelper.GetAccessToken();
            aasHelper.initial(accessToken);

            List<string> tableNameList = aasHelper.getDatabaseTableList();
            List<string> fileNameList = new List<string>(tableNameList);

            Console.WriteLine(String.Format("AAS Model {0} has {1} tables.", modelName, fileNameList.Count()));

            // Rename output filenames
            string filePrefix = modelName.Replace("_", "");
            for (int i = 0; i < fileNameList.Count; i++)
            {
                fileNameList[i] = String.Format("{0}/{1}_{2}.csv", modelName, filePrefix, fileNameList[i]);
            }

            List<List<string>> allTables = aasHelper.getAllTableContents();

            aasHelper.closeConnection();
            Console.WriteLine(String.Format("Model {0} content already obtained.", modelName));

            List<string> localFileNameList = new List<string>();
            for (int i = 0; i < fileNameList.Count; i++)
            {
                localFileNameList.Add(String.Format(@"C:\{0}\{1}\{2}.csv", outputFolderName, modelName, tableNameList[i]));
                ModelToCSV(allTables[i], localFileNameList[i]);
            }


            for (int i = 0; i < allTables.Count; i++)
            {
                //blobHelper.writeCsvToBlob(allTables[i], fileNameList[i]); // Directly write stream object to azure blob
                blobHelper.writeLocalCsvToBlob(allTables[i], localFileNameList[i], fileNameList[i]);
                Console.WriteLine(String.Format("Table {0} upload to blob storage", fileNameList[i]));
            }

            Console.WriteLine(String.Format("Model {0} extract and upload finished.", modelName));
        }

        static void ModelToCSV(List<string> data, string filePath)
        {
            using (var file = new StreamWriter(filePath))
            {
                foreach (var item in data)
                {
                    file.Write(item + "\n");
                }
            }
        }
    }
}
