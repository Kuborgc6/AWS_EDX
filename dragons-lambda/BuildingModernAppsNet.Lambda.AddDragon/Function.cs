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
using System.Linq;
using System.Net;
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

namespace BuildingModernAppsNet.Lambda.AddDragon
{
    /// <summary>
    /// Lambda function to add a Dragon.
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
        public async Task<string> FunctionHandler(Dragon input, ILambdaContext context)
        {
            GetParameterResponse bucketResponse = await this.ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_bucket_name" });
            string bucketname = bucketResponse.Parameter.Value;

            GetParameterResponse fileResponse = await this.ssm.GetParameterAsync(new GetParameterRequest { Name = "dragon_data_file_name" });
            string filename = fileResponse.Parameter.Value;

            // get the dragons list from the s3 object and deserialize into a list of dragons.
            var s3get = await this.s3.GetObjectAsync(new GetObjectRequest { BucketName = bucketname, Key = filename });
            var dragons = await JsonSerializer.DeserializeAsync<List<Dragon>>(s3get.ResponseStream);

            // add the dragon we were just passed
            dragons.Add(input);

            // reserialize and send save back in s3
            // some concurrency concerns here: fine for now, let's consider a different data store when the project grows
            var s3put = await this.s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketname,
                Key = filename,
                ContentBody = JsonSerializer.Serialize(dragons),
            });

            return "Dragon added";
        }
    }
}
