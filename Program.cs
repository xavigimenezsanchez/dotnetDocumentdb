using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft. Azure.Documents.Client;
using Microsoft.Azure.Documents;

//https://www.lynda.com/Azure-tutorials/Change-collection-performance/612187/649533-4.html?srchtrk=index%3a11%0alinktypeid%3a2%0aq%3acosmos%0apage%3a1%0as%3arelevance%0asa%3atrue%0aproducttypeid%3a2

namespace dotnetDocumentdb
{
    class Program
    {
        static readonly string endpointUrl = "https://localhost:8081";
        static readonly string authorizationKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        static DocumentClient client;
        static void Main(string[] args)
        {
            using(client = new DocumentClient(new Uri(endpointUrl), authorizationKey)) {
                //createDatabase().Wait();
                //createPartitionedCollectionWithCustomIndexing("Xavi", "Test", @"/category_code").Wait();
                // { id: "123", category_code: "AS", address: {city: "Los Angeles"}}
                //changeCollectionPerformance("Xavi", "Test").Wait();
                //AddTypedDocuemntsFromFile("Xavi", "Test");
                AddJsonFromFile("Xavi", "Company", "companies.json");
            }
        }

        static async Task<ResourceResponse<Database>> createDatabase() 
        {
            var response = await client.CreateDatabaseIfNotExistsAsync(new Microsoft.Azure.Documents.Database { Id = "Xavi"});
            
            return response;
        }
        static async Task<DocumentCollection> createPartitionedCollection(string databaseId, string collectionId, string PartitionKey)
        {
            DocumentCollection collection = new DocumentCollection();

            collection.Id = collectionId;
            collection.PartitionKey.Paths.Add(PartitionKey);

            DocumentCollection companyCollection = await client.CreateDocumentCollectionIfNotExistsAsync (
                UriFactory.CreateDatabaseUri(databaseId),
                collection,
                new RequestOptions { OfferThroughput = 10000 });

            return companyCollection;
        }

        static async Task<DocumentCollection> createPartitionedCollectionWithCustomIndexing(string databaseId, string collectionId, string PartitionKey)
        {
            DocumentCollection collection = new DocumentCollection();

            collection.Id = collectionId;
            collection.PartitionKey.Paths.Add(PartitionKey);
            collection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            DocumentCollection companyCollection = await client.CreateDocumentCollectionIfNotExistsAsync (
                UriFactory.CreateDatabaseUri(databaseId),
                collection,
                new RequestOptions {    OfferThroughput = 10000,
                                        ConsistencyLevel = ConsistencyLevel.Session
                                    });

            return companyCollection;
        }

        static async Task changeCollectionPerformance(string databaseId, string collectionId)
        {
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)
            );
            Offer offer = client.CreateOfferQuery().Where( o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();
            Console.WriteLine("Current offer is {0} and Collection Name is {1}", offer, collection.Id);

            Offer replaced = await client.ReplaceOfferAsync( new OfferV2(offer, 1200));

            offer = client.CreateOfferQuery().Where( o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();

            OfferV2 offerV2 = (OfferV2)offer;
            Console.WriteLine(offerV2.Content.OfferThroughput);

        }

        static void AddTypedDocuemntsFromFile(string databaseId, string collectionId)
        {
            using(StreamReader file = new StreamReader("companies.json")){
                string line;
                while ((line = file.ReadLine()) !=null)
                {
                    Console.WriteLine(line);
                    JObject json = JObject.Parse(line);
                    Company company = json.ToObject<Company>();
                    company.Id = (string)json.SelectToken("_id.$oid");
                    CreateTypedDocument(databaseId, collectionId, company).Wait();
                }
            }
        }

        private static async Task<Document> CreateTypedDocument(string databaseId, string collectionId, Company company)
        {
            var collection = await client.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));

            Document document = await client.CreateDocumentAsync(collection.Resource.SelfLink, company);

            return document;
        }

        static async void AddJsonFromFile(string databaseId, string collectionId, string filePath)
        {
            using (StreamReader file = new StreamReader(filePath))
            {
                string line;
                while((line = file.ReadLine()) != null)
                {
                    byte[] byteArray = Encoding.UTF8.GetBytes(line);
                    using (MemoryStream stream = new MemoryStream(byteArray))
                    {
                        CreateCollectionFromJson(databaseId, collectionId, stream).Wait();
                    }
                }
            }
        }
        static async Task<Document> CreateCollectionFromJson(string databaseId, string collectionId, Stream stream)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));
            Document document = await client.CreateDocumentAsync(collection.Resource.SelfLink, Resource.LoadFrom<Document>(stream));
            return document;
        }
    }
}
