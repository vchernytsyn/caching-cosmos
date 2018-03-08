using System;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Xunit;

namespace Eshopworld.Caching.Cosmos.Tests
{
    public class CosmosCacheFactoryTests
    {
        public class RedisCacheFactoryTests
        {
            [Fact]
            public void NewInstace_WithValidConnectionString_NoException()
            {
                // Arrange
                // Act
                using (new CosmosCacheFactory(LocalClusterCosmosDb.ConnectionURI, LocalClusterCosmosDb.AccessKey, LocalClusterCosmosDb.DbName)) ;
                // Assert
            }

            [Fact]
            public void Create_RedisCacheFactory_NoException()
            {
                // Arrange
                using (var factory = new CosmosCacheFactory(LocalClusterCosmosDb.ConnectionURI, LocalClusterCosmosDb.AccessKey, LocalClusterCosmosDb.DbName))
                {
                    // Act
                    var instance = factory.Create<SimpleObject>("testCache");

                    // Assert
                    Assert.IsType<CosmosCache<SimpleObject>>(instance);
                }
            }

            //[Fact]
            //public void Create_WithNonExistingCollection_NewCollectionIsCreated()
            //{
            //    var tempCollectionName = Guid.NewGuid().ToString();
            //    // Arrange
            //    using (var factory = new CosmosCacheFactory(LocalClusterCosmosDb.ConnectionURI, LocalClusterCosmosDb.AccessKey, LocalClusterCosmosDb.DbName))
            //    {
            //        // Act
            //        factory.Create<SimpleObject>(tempCollectionName);

            //        Assert.Equal( System.Net.HttpStatusCode.OK,factory.DocumentClient.ReadDocumentCollectionAsync(new Uri("")).GetAwaiter().GetResult().StatusCode);
            //    }
            //}
        }
    }

    internal class LocalClusterCosmosDb
    {
        public static Uri ConnectionURI { get; } = new Uri("https://localhost:8081");
        public static string AccessKey { get; } = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        public static string DbName { get; } = "test-db";
    }
}