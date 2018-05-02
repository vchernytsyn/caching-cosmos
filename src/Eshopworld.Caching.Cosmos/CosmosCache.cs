using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Eshopworld.Caching.Core;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace Eshopworld.Caching.Cosmos
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>Warning: As the cosmos SDK doesnt provide sync methods, all non async methods call their async counterparts and just GetResult().</remarks>
    public class CosmosCache<T> : IDistributedCache<T>
    {
        private readonly DocumentClient _documentClient;
        private readonly Uri _documentCollectionUri;

        public DocumentClient Database => _documentClient;

        public CosmosCache(Uri documentCollectionUri, DocumentClient documentClient)
        {
            _documentCollectionUri = documentCollectionUri ?? throw new ArgumentNullException(nameof(documentCollectionUri));
            _documentClient = documentClient ?? throw new ArgumentNullException(nameof(documentClient));
        }

        public T Add(CacheItem<T> item) => AddAsync(item).ConfigureAwait(false).GetAwaiter().GetResult();
        public async Task<T> AddAsync(CacheItem<T> item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            int? expiryInSec = null;
            if (item.Duration != TimeSpan.MaxValue) expiryInSec = item.Duration.Seconds;

            var envelope = new Envelope(item.Key, Newtonsoft.Json.JsonConvert.SerializeObject(item.Value), expiryInSec);

            // todo: what to do with item.Duration ??
            var docResponse = await _documentClient.UpsertDocumentAsync(_documentCollectionUri, envelope).ConfigureAwait(false);

            if (!(docResponse.StatusCode == HttpStatusCode.Created || docResponse.StatusCode == HttpStatusCode.OK))
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

        public void Remove(string key) => RemoveAsync(key).GetAwaiter().GetResult();
        public Task RemoveAsync(string key) => _documentClient.DeleteDocumentAsync(CreateDocumentURI(key));

        public async Task<T> GetAsync(string key) => (await GetResultAsync(key)).Value;
        public T Get(string key) => GetResult(key).Value;

        public CacheResult<T> GetResult(string key) => GetResultAsync(key).GetAwaiter().GetResult();

        public async Task<CacheResult<T>> GetResultAsync(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            try
            {
                var documentResponse = await _documentClient.ReadDocumentAsync<Envelope>(CreateDocumentURI(key)).ConfigureAwait(false);

                return documentResponse.StatusCode != HttpStatusCode.OK
                    ? CacheResult<T>.Miss()
                    : new CacheResult<T>(true,Newtonsoft.Json.JsonConvert.DeserializeObject<T>(documentResponse.Document.Blob));
            }
            catch (DocumentClientException ex) when (ex.StatusCode.HasValue && ex.StatusCode.Value == HttpStatusCode.NotFound)
            {
                return CacheResult<T>.Miss();
            }
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


        private Uri CreateDocumentURI(string key) => new Uri($"{_documentCollectionUri}/docs/{Uri.EscapeUriString(key)}", UriKind.Relative);

        class Envelope
        {
            public string id { get; } // do not uppercase this, ddb requires it lower so the document id matches the preorder code
            public string Blob { get; }

            // used to set expiration policy
            [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
            public int? TimeToLive { get; set; }

            public Envelope(string id, string blob, int? expiry = null)
            {
                this.id = id;
                this.Blob = blob;
                TimeToLive = expiry;
            }
        }
    }
}