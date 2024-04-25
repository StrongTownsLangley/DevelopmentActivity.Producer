using Elastic.Clients.Elasticsearch;
using DevelopmentActivity.Producer.Interfaces;
using DevelopmentActivity.Producer.Shared;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json.Nodes;

namespace DevelopmentActivity.Producer.ElasticSearch
{
    public class DataService : IDataService
    {
        public async Task<RequestResult> ProduceToDataStore(string jsonDataStr)
        {
            try
            {
                var client = ElasticSearch.ClientFactory.GetClient();
                var id = Guid.NewGuid().ToString();
                var wrappedData = new
                {
                    md5 = jsonDataStr.md5(), // Calculate MD5 hash of the original data
                    timestamp = DateTime.Now.ToFileTime(), // Timestamp
                    data = JsonNode.Parse(jsonDataStr) // Original Data
                };

                var dataResponse = await client.CreateAsync(wrappedData, Config.Index, id);

                if (dataResponse.IsValidResponse)
                {
                    Logger.Log("ELASTIC", $"Data indexed successfully.");
                    return RequestResult.OK;
                }
                else
                {
                    Logger.Log($"ELASTIC", $"Failed to index data: {dataResponse.DebugInformation}");
                    return RequestResult.Error;
                }
            }
            catch (Exception ex)
            {
                Logger.Log($"ELASTIC", $"Error indexing data to Elasticsearch: {ex.Message}");
                return RequestResult.Error;
            }
        }

        public async Task<RequestResult> CompareToDataStore(string jsonDataStr)
        {
            try
            {
                var client = ElasticSearch.ClientFactory.GetClient();

                // See if any records exist (otherwise search returns error)
                var countDataResponse = await client.CountAsync(Config.Index);
                if (!countDataResponse.IsValidResponse)
                {
                    Logger.Log($"ELASTIC", $"Error unable to determine record count [{countDataResponse.DebugInformation}]");
                    return RequestResult.Error;
                }
                else
                {
                    var count = countDataResponse.Count;
                    Logger.Log($"ELASTIC", $"[{Config.Index}] contains [{count}] records");
                    if (count == 0)
                        return RequestResult.Change;
                }

                var searchResponse = await client.SearchAsync<JsonNode>(s => s
                    .Index(Config.Index)
                    .Size(1)  // Retrieve only the last document
                    .Sort(sort => sort.Field("timestamp", new FieldSort { Order = SortOrder.Desc }))  // Sort by descending document ID to get the last document
                );

                if (!searchResponse.IsValidResponse || !searchResponse.Documents.Any())
                {
                    Logger.Log("ELASTIC", $"No previous data found, considering as different.");
                    return RequestResult.Change;
                }

                var previousWrappedData = searchResponse.Documents.FirstOrDefault();
                string previousDataHash = "";
                if (previousWrappedData != null)
                {
                    previousDataHash = previousWrappedData["md5"].ToString();
                    Logger.Log("ELASTIC", $"Last message has hash [{previousDataHash}]");
                }
                if (previousDataHash == jsonDataStr.md5())
                {
                    Logger.Log("ELASTIC", $"Last data point is the same as the new data.");
                    return RequestResult.NoChange;
                }

                Logger.Log("ELASTIC", $"Last data point is different from the new data.");
                return RequestResult.Change;
            }
            catch (Exception ex)
            {
                Logger.Log($"ELASTIC", $"Error comparing data in ElasticSearch: {ex.Message}");
                return RequestResult.Error;
            }
        }

        public async Task<RequestResult> VerifyContainerExists(string container = "")
        {
            if (string.IsNullOrEmpty(container))
                container = Config.Index;

            var client = ElasticSearch.ClientFactory.GetClient();
            var indexExistsResponse = await client.Indices.ExistsAsync(container);

            if (indexExistsResponse.ApiCallDetails.HttpStatusCode != 404 && !indexExistsResponse.IsValidResponse) // If 404 it does not exist
            {
                Logger.Log($"ELASTIC", $"Error Verifying Index [{container}] Exists [{indexExistsResponse.DebugInformation}");
                return RequestResult.Error;
            }

            if (indexExistsResponse.ApiCallDetails.HttpStatusCode == 404 || !indexExistsResponse.Exists)
            {
                var indexResponse = await client.Indices.CreateAsync(container);
                if (!indexExistsResponse.IsValidResponse)
                {
                    Logger.Log($"ELASTIC", $"Created Index [{container}]");
                }
                else
                {
                    Logger.Log($"ELASTIC", $"Error Creating Index [{container}] [{indexResponse.DebugInformation}");
                    return RequestResult.Error;
                }
            }
            else
            {
                Logger.Log($"ELASTIC", $"Verified Index [{container}] Exists");
            }
            return RequestResult.OK;
        }

        public Task<string> FetchData(string request)
        {
            throw new NotImplementedException();
        }


    }
}
