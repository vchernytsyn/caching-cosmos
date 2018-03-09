using System;

namespace Eshopworld.Caching.Cosmos.Tests
{
    internal class LocalClusterCosmosDb
    {
        public static Uri ConnectionURI { get; } = new Uri("https://localhost:8081");
        public static string AccessKey { get; } = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        public static string DbName { get; } = "test-db";
    }
}