using System;
using System.Collections.Generic;
using System.Linq;

namespace Eshopworld.Caching.Cosmos.Tests
{
    public class SimpleObject : IEquatable<SimpleObject>
    {
        public string Foo { get; set; }
        public DateTime DateTime { get; set; }
        public int Value { get; set; }


        public bool Equals(SimpleObject other)
        {
            if (object.ReferenceEquals(this, other)) return true;
            if (object.ReferenceEquals(null, other)) return false;

            return this.Foo == other.Foo && this.DateTime == other.DateTime && this.Value == other.Value;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as SimpleObject);
        }

        public static SimpleObject Create()
        {
            var rnd = new Random();

            return new SimpleObject()
            {
                DateTime = DateTime.UtcNow,
                Foo = Guid.NewGuid().ToString(),
                Value = rnd.Next()
            };
        }
    }

    public class ComplexObject : IEquatable<ComplexObject>
    {
        public SimpleObject[] Items { get; set; }
        public List<SimpleObject> ItemList { get; set; }

        public bool Equals(ComplexObject other)
        {
            if (object.ReferenceEquals(this, other)) return true;
            if (object.ReferenceEquals(null, other)) return false;

            return this.Items.SequenceEqual(other.Items) && this.ItemList.SequenceEqual(other.ItemList);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ComplexObject);
        }

        public static ComplexObject Create()
        {
            return new ComplexObject()
            {
                Items = Enumerable.Range(0, 100).Select(i => SimpleObject.Create()).ToArray(),
                ItemList = Enumerable.Range(0, 10).Select(i => SimpleObject.Create()).ToList(),
            };
        }
    }
}