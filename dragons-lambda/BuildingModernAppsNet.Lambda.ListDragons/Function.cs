//-----------------------------------------------------------------------
// <copyright file="Function.cs" company="Amazon.com, Inc">
//
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//  http://aws.amazon.com/apache2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using BuildingModernAppsNet.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace BuildingModernAppsNet.Lambda.ListDragons
{
    /// <summary>
    /// Lambda function to list Dragons.
    /// </summary>
    public class Function
    {
        private AmazonSimpleSystemsManagementClient ssm = new AmazonSimpleSystemsManagementClient();
        private AmazonS3Client s3 = new AmazonS3Client();

        /// <summary>
        /// Lambda function handler.
        /// </summary>
        /// <param name="input">Query string parameters.</param>
        /// <param name="context">Lambda context.</param>
        /// <returns>API gateway response.</returns>
        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest input, ILambdaContext context)
        {
            GetParameterResponse bucketResponse = await this.ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_bucket_name" });
            string bucketname = bucketResponse.Parameter.Value;

            GetParameterResponse fileResponse = await this.ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_file_name" });
            string filename = fileResponse.Parameter.Value;

            var query = this.GetQuery(input.QueryStringParameters);
            var request = new SelectObjectContentRequest
            {
                Bucket = bucketname,
                Key = filename,
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
            var dragonData = await this.QueryS3(request);

            Dictionary<string, string> headers = new Dictionary<string, string>();
            headers.Add("access-control-allow-origin", "*");
            var response = new APIGatewayProxyResponse
            {
                Headers = headers,
                StatusCode = (int)HttpStatusCode.OK,
                Body = JsonSerializer.Serialize(dragonData),
            };

            return response;
        }

        private async Task<List<Dragon>> QueryS3(SelectObjectContentRequest request)
        {
            var selectObjectContentResponse = await this.s3.SelectObjectContentAsync(request);

            // see also https://github.com/aws-samples/aws-netcore-webapp-using-amazonpersonalize/blob/main/AWS.Samples.Amazon.Personalize.Demo/Support/StorageService.cs
            var payload = selectObjectContentResponse.Payload;
            var dragons = new List<Dragon>();

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
                                Dragon dragon = JsonSerializer.Deserialize<Dragon>(result);
                                dragons.Add(dragon);
                            }
                        }
                    }
                }
            }

            return dragons;
        }

        private string GetQuery(IDictionary<string, string> queryStringParameters)
        {
            var query = "select * from S3Object[*][*] s";
            var condition = string.Empty;

            // Some different querystrings for testing:
            // { "queryStringParameters": { "dragonName": "Bahamethut" } }
            // { "queryStringParameters": { } }
            // { "queryStringParameters": { "family": "Red" } }

            // Worried about concatenating SQL strings, and injection attacks?  In our case we allow users to see
            // all of the data, there's also np UPDATE, INSERT or DELETE commands in S3 select
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-select.html
            if (queryStringParameters != null && queryStringParameters.Count > 0)
            {
                if (queryStringParameters.ContainsKey("family"))
                {
                    condition += $" where s.family_str = '{queryStringParameters["family"]}'";
                }

                if (queryStringParameters.ContainsKey("dragonName"))
                {
                    condition += string.IsNullOrEmpty(condition) ? " where " : " or ";
                    condition += $"s.dragon_name_str =  '{queryStringParameters["dragonName"]}'";
                }
            }

            return query + condition;
        }
    }
}