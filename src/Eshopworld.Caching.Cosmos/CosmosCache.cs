using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Beatles.Caching;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Eshopworld.Caching.Cosmos
{
    public class CosmosCache<T> : IDistributedCache<T>
    {
        private readonly DocumentClient _documentClient;
        private readonly Uri _documentCollectionUri;

        public DocumentClient Database => _documentClient;

        public CosmosCache(DocumentClient documentClient)
        {
            _documentClient = documentClient;
        }

        public T Add(CacheItem<T> item) => AddAsync(item).ConfigureAwait(false).GetAwaiter().GetResult();
        public async Task<T> AddAsync(CacheItem<T> item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            var envelope = new Envelope(item.Key, item.Value.ToString());

            // todo: what to do with item.Duration ??
            var docResponse = await _documentClient.UpsertDocumentAsync(_documentCollectionUri, envelope).ConfigureAwait(false);

            //if (Logger.IsDebugEnabled) Logger.Debug($"Doc '{key}' saved to cosmos. Response Status:{docResponse.StatusCode}");

            if (docResponse.StatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException($"Unable to save document. Status:{docResponse.StatusCode}");
            }

            return item.Value;
        }


        public void Set(CacheItem<T> item) => Add(item);
        public Task SetAsync(CacheItem<T> item) => AddAsync(item);


        public bool Exists(string key) => GetResult(key).HasValue;
        public async Task<bool> ExistsAsync(string key) => (await GetResultAsync(key)).HasValue;

        public bool KeyExpire(string key, TimeSpan? expiry) => false;
        public Task<bool> KeyExpireAsync(string key, TimeSpan? expiry) => Task.FromResult(false);

        public void Remove(string key) => _documentClient.DeleteDocumentAsync(key).GetAwaiter().GetResult();
        public Task RemoveAsync(string key) => _documentClient.DeleteDocumentAsync(key);

        public async Task<T> GetAsync(string key) => (await GetResultAsync(key)).Value;
        public T Get(string key) => GetResult(key).Value;

        public CacheResult<T> GetResult(string key) => GetResultAsync(key).GetAwaiter().GetResult();
        public async Task<CacheResult<T>> GetResultAsync(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var uri = new Uri($"{_documentCollectionUri}/docs/{Uri.EscapeUriString(key)}", UriKind.Relative);

            var documentResponse = await _documentClient.ReadDocumentAsync<T>(uri).ConfigureAwait(false);
            
            return documentResponse.StatusCode != HttpStatusCode.OK  ? CacheResult<T>.Miss() : new CacheResult<T>(true, documentResponse.Document);
        }

        public IEnumerable<KeyValuePair<string, T>> Get(IEnumerable<string> keys) => GetAsync(keys).ConfigureAwait(false).GetAwaiter().GetResult();
        public async Task<IEnumerable<KeyValuePair<string, T>>> GetAsync(IEnumerable<string> keys)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));

            using (var queryable = _documentClient.CreateDocumentQuery<Envelope>(_documentCollectionUri)
                .Where(e => keys.Contains(e.id))
                .AsDocumentQuery())
            {
                var items = new Dictionary<string, T>();

                while (queryable.HasMoreResults)
                {
                    foreach(var e in await queryable.ExecuteNextAsync<Envelope>())
                    {
                        items[e.id] = (T) (object) e;
                    }
                }

                return items;
            }
        }


        class Envelope
        {
            public string id { get; } // do not uppercase this, ddb requires it lower so the document id matches the preorder code
            public string Blob { get; }

            public Envelope(string id, string blob)
            {
                this.id = id;
                this.Blob = blob;
            }
        }
    }
}