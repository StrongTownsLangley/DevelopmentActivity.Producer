using System.Security.Cryptography;
using System.Text;

namespace DevelopmentActivity.Producer.Shared
{
    public static class StringExtensions
    {
        public static string md5(this string input)
        {
            using (MD5 md5 = MD5.Create())
            {
                byte[] inputBytes = Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);
                return string.Concat(hashBytes.Select(b => b.ToString("x2")));
            }
        }
    }
}
