using System;
using System.Linq;
using System.Threading.Tasks;
using Eshopworld.Caching.Core;
using Eshopworld.Tests.Core;
using Xunit;

namespace Eshopworld.Caching.Cosmos.Tests
{
    public class CosmosCacheTests
    {
        private const string CacheKey = "item";
        private CosmosCacheFactory cacheFactory;
        private CosmosCache<string> stringCache;

        public CosmosCacheTests()
        {
            cacheFactory = new CosmosCacheFactory(LocalClusterCosmosDb.ConnectionURI, LocalClusterCosmosDb.AccessKey, LocalClusterCosmosDb.DbName);

            stringCache = (CosmosCache<string>)cacheFactory.Create<string>("string-collection");
            //stringCache.Remove(CacheKey);
        }


        [Fact,IsIntegration]
        public void SetString_DoesNotThrow()
        {
            // Arrange
            // Act
            stringCache.Set(new CacheItem<string>(CacheKey, "Test", TimeSpan.FromSeconds(5)));
            // Assert
        }

        [Fact,IsIntegration]
        public async Task SetAsyncString_DoesNotThrow()
        {
            // Arrange
            // Act
            await stringCache.SetAsync(new CacheItem<string>(CacheKey, "Test", TimeSpan.FromSeconds(5)));
            // Assert
        }


        [Fact(Skip = "Not supported by cosmos implementation (yet)")]
        public void SetStringWithTimeout_ItemDoesNotExistAfterTimeout()
        {
            // Arrange
            // Act
            var duration = TimeSpan.FromSeconds(3);
            stringCache.Set(new CacheItem<string>(CacheKey, "Test", duration));

            // Assert
            Assert.True(stringCache.Exists(CacheKey));
            System.Threading.Thread.Sleep(duration.Add(TimeSpan.FromSeconds(1)));
            Assert.False(stringCache.Exists(CacheKey));
        }

        [Fact,IsIntegration]
        public void GetString_ItemExistsInCache()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            var result = stringCache.Get(CacheKey);

            // Assert
            Assert.Equal(result, cacheValue);
        }

        [Fact,IsIntegration]
        public async Task GetAsyncString_ItemExistsInCache()
        {
            // Arrange
            var cacheValue = "Test";

            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            var result = await stringCache.GetAsync(CacheKey);

            // Assert
            Assert.Equal(result, cacheValue);
        }




        [Fact,IsIntegration]
        public void Remove_AfterRemove_GetReturnsNull()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            stringCache.Remove(CacheKey);

            // Assert
            var result = stringCache.Get(CacheKey);
            Assert.Null(result);
        }

        [Fact,IsIntegration]
        public async Task RemoveAsync_AfterRemove_GetReturnsNull()
        {
            // Arrange
            var cacheValue = "Test";


            await stringCache.SetAsync(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            await stringCache.RemoveAsync(CacheKey);

            // Assert
            var result = await stringCache.GetAsync(CacheKey);
            Assert.Null(result);
        }



        [Fact,IsIntegration]
        public void Exists_AfterAdding_ReturnsTrue()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            // Assert
            Assert.True(stringCache.Exists(CacheKey));
        }

        [Fact,IsIntegration]
        public async Task ExistsAsync_AfterAdding_ReturnsTrue()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            // Assert
            Assert.True(await stringCache.ExistsAsync(CacheKey));
        }

        [Fact,IsIntegration]
        public void Exists_AfterAddingAndRemoving_ReturnsFalse()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));
            stringCache.Remove(CacheKey);
            // Act
            // Assert
            Assert.False(stringCache.Exists(CacheKey));
        }

        [Fact,IsIntegration]
        public async Task Exists_AfterAddingAndRemoving_GetReturnsNull()
        {
            // Arrange
            var cacheValue = "Test";


            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            // Assert
            Assert.True(await stringCache.ExistsAsync(CacheKey));
        }


        [Fact(Skip = "Not supported by cosmos implementation (yet)")]
        public void Expire_AfterSettingExpireAndWaiting_ItemDoesntExistInCache()
        {
            // Arrange
            var cacheValue = "Test";
            var expireIn = new TimeSpan(0, 0, 2);

            stringCache.Set(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            stringCache.KeyExpire(CacheKey, expireIn);
            System.Threading.Thread.Sleep(expireIn.Add(TimeSpan.FromSeconds(1)));

            // Assert
            Assert.False(stringCache.Exists(CacheKey));
        }

        [Fact(Skip = "Not supported by cosmos implementation (yet)")]
        public async Task ExpireAsync_AfterSettingExpireAndWaiting_ItemDoesntExistInCache()
        {
            // Arrange
            var cacheValue = "Test";
            var expireIn = new TimeSpan(0, 0, 2);

            await stringCache.SetAsync(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            await stringCache.KeyExpireAsync(CacheKey, expireIn);
            await Task.Delay(expireIn.Add(TimeSpan.FromSeconds(1)));


            // Assert
            Assert.False(await stringCache.ExistsAsync(CacheKey));
        }


        [Fact,IsIntegration]
        public void Get_SimpleObject_ReturnedObjectIsIdentical()
        {
            // Arrange
            var cache = cacheFactory.CreateDefault<SimpleObject>();
            var value = SimpleObject.Create();
            cache.Set(new CacheItem<SimpleObject>(CacheKey, value, TimeSpan.FromSeconds(5)));

            // Act
            var result = cache.Get(CacheKey);

            // Assert
            Assert.False(object.ReferenceEquals(result, value));
            Assert.Equal(result, value);
        }

        [Fact,IsIntegration]
        public void Get_ComplexObject_ReturnedObjectIsIdentical()
        {
            // Arrange
            var cache = cacheFactory.CreateDefault<ComplexObject>();

            var value = ComplexObject.Create();
            cache.Set(new CacheItem<ComplexObject>(CacheKey, value, TimeSpan.FromSeconds(5)));

            // Act
            var result = cache.Get(CacheKey);

            // Assert
            Assert.False(object.ReferenceEquals(result, value));
            Assert.Equal(result, value);
        }


        [Fact,IsIntegration]
        public void GetResult_SimpleObject_ReturnedObjectIsIdentical()
        {
            // Arrange
            var cache = cacheFactory.CreateDefault<SimpleObject>();
            var value = SimpleObject.Create();
            cache.Set(new CacheItem<SimpleObject>(CacheKey, value, TimeSpan.FromSeconds(5)));

            // Act
            var result = cache.GetResult(CacheKey);

            // Assert
            Assert.NotNull(result);
            Assert.True(result.HasValue);
            Assert.False(object.ReferenceEquals(result.Value, value));
            Assert.Equal(result.Value, value);
        }

        [Fact,IsIntegration]
        public async Task GetResultAsync_SimpleObject_ReturnedObjectIsIdentical()
        {
            // Arrange
            var cache = cacheFactory.CreateDefault<SimpleObject>();
            var value = SimpleObject.Create();
            await cache.SetAsync(new CacheItem<SimpleObject>(CacheKey, value, TimeSpan.FromSeconds(5)));

            // Act
            var result = await cache.GetResultAsync(CacheKey);

            // Assert
            Assert.NotNull(result);
            Assert.True(result.HasValue);
            Assert.False(object.ReferenceEquals(result.Value, value));
            Assert.Equal(result.Value, value);
        }

        [Fact,IsIntegration]
        public void GetResult_MissingObject_ResultDoesNotHaveValue()
        {
            // Arrange
            var cache = cacheFactory.CreateDefault<SimpleObject>();

            // Act
            var result = cache.GetResult("doesntExist");

            // Assert
            Assert.NotNull(result);
            Assert.False(result.HasValue);
            Assert.Null(result.Value);
        }


        [Fact,IsIntegration]
        public void Set_MultipeTasks_NoExceptions()
        {
            // Arrange
            const int numberOfItems = 100;
            var items = Enumerable.Range(0, numberOfItems).Select(i => SimpleObject.Create()).ToArray();
            var cache = cacheFactory.CreateDefault<SimpleObject>();

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var loopResult = Parallel.For(0, 20, i =>
            {
                var index = i % numberOfItems;
                var item = items[index];
                cache.Set(new CacheItem<SimpleObject>("item-" + index, item, TimeSpan.FromSeconds(5)));
            });
            stopwatch.Stop();

            // Assert
            Console.WriteLine("Duration:" + stopwatch.ElapsedMilliseconds);
            Assert.True(loopResult.IsCompleted);
        }

        [Fact,IsIntegration]
        public void Get_MultipeTasks_NoExceptions()
        {
            // Arrange
            const int numberOfItems = 100;
            var cache = cacheFactory.CreateDefault<SimpleObject>();

            Enumerable.Range(0, numberOfItems)
                .Select(i => Tuple.Create("item-" + i, SimpleObject.Create()))
                .All(_ => { cache.Set(new CacheItem<SimpleObject>(_.Item1, _.Item2, TimeSpan.FromSeconds(5))); return true; });

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var loopResult = Parallel.For(0, 20, i =>
            {
                var index = i % numberOfItems;
                var result = cache.Get("item-" + index);

            });

            stopwatch.Stop();

            // Assert
            Console.WriteLine("Duration:" + stopwatch.ElapsedMilliseconds);
            Assert.True(loopResult.IsCompleted);
        }

        [Fact,IsIntegration]
        public async Task AddAsync_Set_ExpiryTimeAtDocumentLevel_ItemDoesntExistInCache()
        {
            // Arrange
            var cacheValue = "Test";
           
            await stringCache.AddAsync(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));

            // Act
            await Task.Delay(TimeSpan.FromSeconds(7));


            // Assert
            Assert.False(await stringCache.ExistsAsync(CacheKey));
        }

        [Fact, IsIntegration]
        public async Task AddAsync_Set_ExpiryTimeAtDocumentLevel_ItemExistInCache()
        {
            // Arrange
            var cacheValue = "Test";

            await stringCache.AddAsync(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.FromSeconds(5)));
            
            // Assert
            Assert.False(await stringCache.ExistsAsync(CacheKey));
        }

        [Fact, IsIntegration]
        public async Task AddAsync_No_ExpiryTimeSet()
        {
            // Arrange
            var cacheValue = "Test";

            await stringCache.AddAsync(new CacheItem<string>(CacheKey, cacheValue, TimeSpan.MaxValue));

            // Assert
            Assert.False(await stringCache.ExistsAsync(CacheKey));
        }
    }
}
