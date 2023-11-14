//-----------------------------------------------------------------------
// <copyright file="DragonValidationException.cs" company="Amazon.com, Inc">
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
namespace BuildingModernAppsNet.Lambda.ValidateDragon
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Validation exception class.
    /// </summary>
    [Serializable]
    internal class DragonValidationException : Exception
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="message">Message string.</param>
        public DragonValidationException(string message)
            : base(message)
        {
        }
    }
}