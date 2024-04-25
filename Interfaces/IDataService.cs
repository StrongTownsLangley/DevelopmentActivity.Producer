using System.Threading.Tasks;
using DevelopmentActivity.Producer.Shared;

namespace DevelopmentActivity.Producer.Interfaces
{
    public interface IDataService
    {
        Task<string> FetchData(string request);

        Task<RequestResult> VerifyContainerExists(string container);
        Task<RequestResult> ProduceToDataStore(string jsonDataStr);
        Task<RequestResult> CompareToDataStore(string jsonDataStr);
    }
}
