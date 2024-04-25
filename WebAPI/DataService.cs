using DevelopmentActivity.Producer.Interfaces;
using DevelopmentActivity.Producer.Shared;
using Elastic.Clients.Elasticsearch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace DevelopmentActivity.Producer.WebAPI
{
    public class DataService : IDataService
    {
        private static readonly HttpClient httpClient = new HttpClient();

        public async Task<string> FetchData(string url)
        {
            Logger.Log($"WEBAPI", $"Downloading from WebAPI");
            try
            {
                string jsonData = await httpClient.GetStringAsync(url);
                Logger.Log("WEBAPI", $"Download Complete");
                return jsonData;
            }
            catch (Exception ex)
            {
                Logger.Log($"WEBAPI", $"Error while fetching data: {ex.Message}");
                return null;
            }
        }

        public Task<RequestResult> CompareToDataStore(string jsonDataStr)
        {
            throw new NotImplementedException();
        }

        public Task<RequestResult> ProduceToDataStore(string jsonDataStr)
        {
            throw new NotImplementedException();
        }

        public Task<RequestResult> VerifyContainerExists(string container)
        {
            throw new NotImplementedException();
        }
    }
}
