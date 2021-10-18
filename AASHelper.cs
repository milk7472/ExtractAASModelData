using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace AnalysisServerDataExtract
{
    class AASHelper
    {
        string serverName = "asazure://southeastasia.asazure.windows.net/server_name";
        string aasUrl = "https://southeastasia.asazure.windows.net";
        string databaseName = "";

        // azure services principal
        string tenantId = "00000000-0000-0000-0000-000000000000";
        string appId = "00000000-0000-0000-0000-000000000000";
        string appSecret = "app_secret";
        string authorityUrl = "https://login.microsoftonline.com/";

        AdomdConnection adomdConnection = null;
        List<string> dbTableList = null;

        public AASHelper(string databaseName)
        {
            this.databaseName = databaseName;
            this.authorityUrl = String.Format("{0}{1}", this.authorityUrl, tenantId);
        }

        public AASHelper(string tenantId, string appId, string appSecret, string aasServerName, string databaseName)
        {
            this.tenantId = tenantId;
            this.appId = appId;
            this.appSecret = appSecret;

            this.serverName = aasServerName;
            this.databaseName = databaseName;

            this.authorityUrl = String.Format("{0}{1}", this.authorityUrl, tenantId);
        }

        public void initial(string accessToken)
        {
            try
            {
                //string accessToken = await GetAccessToken();
                string connectionString = String.Format("Provider = MSOLAP; Data Source = {0}; Initial Catalog = {1}; User ID =; Password ={2}; Persist Security Info = True; Impersonation Level = Impersonate",this.serverName, this.databaseName, accessToken);
                        
                adomdConnection = new AdomdConnection(connectionString);
                adomdConnection.Open();                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void closeConnection()
        {
            try
            {
                adomdConnection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task<string> GetAccessToken()
        {
            AuthenticationContext authContext = new AuthenticationContext(this.authorityUrl);
            AuthenticationResult authenticationResult = null;

            try
            {
                // Config for OAuth client credentials 
                authContext = new AuthenticationContext(this.authorityUrl);

                var clientCred = new ClientCredential(this.appId, this.appSecret);
                authenticationResult = await authContext.AcquireTokenAsync(this.aasUrl, clientCred);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return authenticationResult.AccessToken;
        }

        public List<string> getDatabaseTableList()
        {
            // Query all tables
            List<string> listTables = new List<string>();
            var mdX = @"SELECT [DIMENSION_CAPTION] FROM $SYSTEM.MDSCHEMA_DIMENSIONS";

            try {
                using (AdomdCommand command = new AdomdCommand(mdX, adomdConnection))
                {
                    AdomdDataReader results = command.ExecuteReader();

                    while (results.Read())
                    {
                        if (results.GetString(0).Contains("Measures"))
                            continue;

                        listTables.Add(results.GetString(0));
                    }

                    results.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }            

            return listTables;
        }

        public List<string> getTableContentByName(string tableName)
        {
            List<string> table = null;
            string mdX = String.Format(@"EVALUATE '{0}'", tableName);

            try
            {
                using (AdomdCommand command = new AdomdCommand(mdX, adomdConnection))
                {
                    command.CommandTimeout = 10 * 60;
                    var results = command.ExecuteReader();

                    table = toCsvFormat(results, true, ",", tableName);

                    results.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return table;
        }

        public List<List<string>> getAllTableContents()
        {
            List<List<string>> allTables = new List<List<string>>();

            dbTableList = getDatabaseTableList();

            foreach (var tableName in dbTableList)
            {
                List<string> tableContent = getTableContentByName(tableName);
                allTables.Add(tableContent);

                Console.WriteLine(String.Format("Get table {0} content finished.", tableName));
            }

            return allTables;
        }

        public List<string> getTableContent(string tableName)
        {
            List<string> tableContents = getTableContentByName(tableName);

            return tableContents;
        }

        public List<string> toCsvFormat(IDataReader dataReader, bool includeHeaderAsFirstRow, string separator, string tableName)
        {
            List<string> csvRows = new List<string>();
            StringBuilder sb = null;

            if (includeHeaderAsFirstRow)
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount; index++)
                {
                    if (dataReader.GetName(index) != null)
                        sb.Append(dataReader.GetName(index));

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(separator);
                }

                // The title extracted from AAS will contains model name, [, and ]. Remove them.
                sb.Replace(tableName, "").Replace("[", "").Replace("]", "").Replace(" ", "").Replace(".", "_");
                csvRows.Add(sb.ToString());
            }

            while (dataReader.Read())
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount - 1; index++)
                {
                    if (!dataReader.IsDBNull(index))
                    {
                        string value = dataReader.GetValue(index).ToString();
                        if (dataReader.GetFieldType(index) == typeof(String))
                        {
                            //If double quotes are used in value, ensure each are replaced but 2.
                            if (value.IndexOf("\"") >= 0)
                                value = value.Replace("\"", "\"\"");

                            //If separtor are is in value, ensure it is put in double quotes.
                            if (value.IndexOf(separator) >= 0)
                                value = "\"" + value + "\"";
                        }
                        sb.Append(value);
                    }

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(separator);
                }

                if (!dataReader.IsDBNull(dataReader.FieldCount - 1))
                    sb.Append(dataReader.GetValue(dataReader.FieldCount - 1).ToString().Replace(separator, " "));

                csvRows.Add(sb.ToString());
            }
            dataReader.Close();
            sb = null;
            return csvRows;
        }
    }
}
