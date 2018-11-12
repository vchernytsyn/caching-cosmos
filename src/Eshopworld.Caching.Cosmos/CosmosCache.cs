using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Eshopworld.Caching.Core;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Eshopworld.Caching.Cosmos
{
    public class CosmosCache
    {
        public enum InsertMode
        {
            JSON,
            Document,
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>Warning: As the cosmos SDK doesnt provide sync methods, all non async methods call their async counterparts and just GetResult().</remarks>
    public class CosmosCache<T> : IDistributedCache<T>
    {
        private static readonly FieldInfo propertyBagField = typeof(Document).GetField("propertyBag", BindingFlags.Instance | BindingFlags.NonPublic);

        private readonly DocumentClient _documentClient;
        private readonly Uri _documentCollectionUri;
        private readonly bool _usePartitionKey = false;

        private readonly CosmosCache.InsertMode _insertMode = CosmosCache.InsertMode.JSON;

        public DocumentClient Database => _documentClient;

        public CosmosCache(Uri documentCollectionUri, DocumentClient documentClient) : this(documentCollectionUri, documentClient, CosmosCache.InsertMode.JSON,false) { }

        public CosmosCache(Uri documentCollectionUri, DocumentClient documentClient, CosmosCache.InsertMode _insertMode,bool usePartitionKey)
        {
            _documentCollectionUri = documentCollectionUri ?? throw new ArgumentNullException(nameof(documentCollectionUri));
            _documentClient = documentClient ?? throw new ArgumentNullException(nameof(documentClient));
            this._insertMode = _insertMode;
            this._usePartitionKey = usePartitionKey;
        }

        public T Add(CacheItem<T> item) => AddAsync(item).ConfigureAwait(false).GetAwaiter().GetResult();
        
        public async Task<T> AddAsync(CacheItem<T> item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            var requestOptions = _usePartitionKey ? new RequestOptions() { PartitionKey = new PartitionKey(item.Key) } : null;
            var docResponse = await _documentClient.UpsertDocumentAsync(_documentCollectionUri, CreateDocument(item), requestOptions).ConfigureAwait(false);

            if (!(docResponse.StatusCode == HttpStatusCode.Created || docResponse.StatusCode == HttpStatusCode.OK))
            {
                throw new InvalidOperationException($"Unable to save document. Status:{docResponse.StatusCode}");
            }

            return item.Value;
        }

        private object CreateDocument(CacheItem<T> item)
        {
            var doc = new Document();
            var ttl = item.Duration != TimeSpan.MaxValue ? (int?)item.Duration.TotalSeconds : null;
            
            
            if (_insertMode == CosmosCache.InsertMode.Document)
            {
                propertyBagField.SetValue(doc, JObject.FromObject(item.Value)); // todo: there has to be a better way to do this. its either this, or call the internal 'FromObject' method..
                doc.Id = item.Key;
                doc.TimeToLive = ttl;

                return doc;
            }
            else if (_insertMode == CosmosCache.InsertMode.JSON)
            {
                return new Envelope(item.Key, JsonConvert.SerializeObject(item.Value), ttl);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(_insertMode), _insertMode, "Condition not supported");
            }
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
                var pair = await GetDocument(key);

                return pair.statusCode != HttpStatusCode.OK
                    ? CacheResult<T>.Miss()
                    : new CacheResult<T>(true, pair.body);
            }
            catch (DocumentClientException ex) when (ex.StatusCode.HasValue && ex.StatusCode.Value == HttpStatusCode.NotFound)
            {
                return CacheResult<T>.Miss();
            }
        }

        private async Task<(HttpStatusCode statusCode, T body)> GetDocument(string key)
        {
            var documentUri = CreateDocumentURI(key);
            var requestOptions = _usePartitionKey ? new RequestOptions() { PartitionKey = new PartitionKey(key) } : null;

            if (_insertMode == CosmosCache.InsertMode.JSON)
            {
                var documentResponse = await _documentClient.ReadDocumentAsync<Envelope>(documentUri, requestOptions).ConfigureAwait(false);

                return (documentResponse.StatusCode, JsonConvert.DeserializeObject<T>(documentResponse.Document.Blob));
            }
            else if (_insertMode == CosmosCache.InsertMode.Document)
            {
                var documentResponse = await _documentClient.ReadDocumentAsync<T>(documentUri, requestOptions).ConfigureAwait(false);
                return (documentResponse.StatusCode, documentResponse.Document);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(_insertMode), _insertMode, "Condition not supported");
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


        public Uri CreateDocumentURI(string key) => new Uri($"{_documentCollectionUri}/docs/{Uri.EscapeUriString(key)}", UriKind.Relative);

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