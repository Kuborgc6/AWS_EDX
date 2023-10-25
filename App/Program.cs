//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="DragonsInc">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------
namespace App
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Amazon.S3;
    using Amazon.S3.Model;
    using Amazon.SimpleSystemsManagement;
    using Amazon.SimpleSystemsManagement.Model;

    /// <summary>
    /// Framwwork exploration.
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Framework exploration.
        /// </summary>
        /// <param name="args">Not used.</param>
        public static void Main(string[] args)
        {
            var dragonData = ReadDragonData();
            Console.WriteLine(dragonData.Result);
        }

        private static async Task<string> ReadDragonData()
        {
            AmazonSimpleSystemsManagementClient ssm = new AmazonSimpleSystemsManagementClient();
            GetParameterResponse bucketResponse = await ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_bucket_name" });
            var bucketName = bucketResponse.Parameter.Value;
            GetParameterResponse fileResponse = await ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_file_name" });
            var key = fileResponse.Parameter.Value;

            var query = GetQuery();
            var request = new SelectObjectContentRequest
            {
                Bucket = bucketName,
                Key = key,
                Expression = query,
                ExpressionType = ExpressionType.SQL,
                InputSerialization = new InputSerialization()
                {
                    JSON = new JSONInput()
                    {
                        JsonType = JsonType.Document,
                    },
                    CompressionType = CompressionType.None,
                },
                OutputSerialization = new OutputSerialization()
                {
                    JSON = new JSONOutput(),
                },
            };
            var dragonData = QueryS3(request);
            return dragonData.Result;
        }

        private static async Task<string> QueryS3(SelectObjectContentRequest request)
        {
            AmazonS3Client s3 = new AmazonS3Client();
            var selectObjectContentResponse = await s3.SelectObjectContentAsync(request);

            // see also https://github.com/aws-samples/aws-netcore-webapp-using-amazonpersonalize/blob/main/AWS.Samples.Amazon.Personalize.Demo/Support/StorageService.cs
            var payload = selectObjectContentResponse.Payload;

            var stringBuilder = new StringBuilder();
            using (payload)
            {
                foreach (var ev in payload)
                {
                    if (ev is RecordsEvent records)
                    {
                        using (var reader = new StreamReader(records.Payload, Encoding.UTF8))
                        {
                            while (reader.Peek() >= 0)
                            {
                                string result = reader.ReadLine();
                                stringBuilder.Append(result);
                            }
                        }
                    }
                }
            }

            return stringBuilder.ToString();
        }

        private static string GetQuery()
        {
            // later on this method will return different results based
            // on query string parameters. For now, we will hardcode the results
            // to select *, which isn't the best showcase of S3 select
            // but don't worry we will get there
            return "select * from s3object s";
        }
    }
}
