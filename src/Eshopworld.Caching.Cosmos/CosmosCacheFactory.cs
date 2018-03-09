using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Eshopworld.Caching.Core;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Eshopworld.Caching.Cosmos
{
    public class CosmosCacheFactory : ICacheFactory, IDisposable
    {
        private readonly string _dbName;
        private readonly ConcurrentDictionary<string, Uri> documentCollectionURILookup = new ConcurrentDictionary<string, Uri>();

        public DocumentClient DocumentClient { get; }

        public int NewCollectionDefaultDTU { get; set; } = 400;

        public CosmosCacheFactory(Uri cosmosAccountEndpoint, string cosmosAccountKey, string dbName)
        {
            _dbName = dbName ?? throw new ArgumentNullException(nameof(dbName));

            DocumentClient = new DocumentClient(cosmosAccountEndpoint, cosmosAccountKey);
        }

        public ICache<T> CreateDefault<T>() => Create<T>(typeof(T).Name);

        public ICache<T> Create<T>(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            var documentCollectionURI  = documentCollectionURILookup.GetOrAdd(name, TryCreateCollection);

            return new CosmosCache<T>(documentCollectionURI, DocumentClient);
        }

        private Uri TryCreateCollection(string name)
        {
            // todo: need to handle partition key, size etc
            var dc = DocumentClient.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(_dbName), new DocumentCollection() {Id = name}, new RequestOptions() {OfferThroughput = NewCollectionDefaultDTU})
                .GetAwaiter()
                .GetResult();

            return new Uri(dc.Resource.AltLink, UriKind.Relative);
        }

        public void Dispose() => DocumentClient.Dispose();
    }
}