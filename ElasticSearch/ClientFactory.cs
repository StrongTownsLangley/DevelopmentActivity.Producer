using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using System;

namespace DevelopmentActivity.Producer.ElasticSearch
{
    public static class ClientFactory
    {
        private static ElasticsearchClient _elasticSearchClient;

        public static ElasticsearchClient GetClient()
        {
            if (_elasticSearchClient == null)
            {
                var settings = new ElasticsearchClientSettings(new Uri("http://" + Config.Server))
                               .Authentication(new ApiKey(Config.APIKey));
                _elasticSearchClient = new ElasticsearchClient(settings);
            }
            return _elasticSearchClient;
        }
    }
}
