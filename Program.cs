using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft. Azure.Documents.Client;
using Microsoft.Azure.Documents;


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
                /*
                    First steps with cosmos DB
                 */
                    //createDatabase().Wait();
                    //createPartitionedCollectionWithCustomIndexing("Xavi", "Test", @"/category_code").Wait();
                    // { id: "123", category_code: "AS", address: {city: "Los Angeles"}}
                    //changeCollectionPerformance("Xavi", "Test").Wait();
                    //AddTypedDocuemntsFromFile("Xavi", "Test");
                    //AddJsonFromFile("Xavi", "Company", "companies.json");
                /*
                    Data Manipulation
                 */
                    /* Simple stored procedure query */
                        QueryWithStoredProcs("Xavi", "Company", @"./Scripts/SimpleQuery.js").Wait();

            }
        }
#region Data_Import_Scenarios
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

        static void AddJsonFromFile(string databaseId, string collectionId, string filePath)
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

#endregion Data_Import_Scenarios

        #region Data_Manipulation
        static async Task QueryWithStoredProcs(string databaseId, string collectionId, string scriptPath)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));

            string scriptId = Path.GetFileNameWithoutExtension(scriptPath);

            var storedProcedure = new StoredProcedure 
            {
                Id = scriptId,
                Body = File.ReadAllText(scriptPath)
            };

            StoredProcedure procedure = client.CreateStoredProcedureQuery(collection.Resource.SelfLink).Where(x => x.Id == storedProcedure.Id).AsEnumerable().FirstOrDefault();
            if (procedure != null)
            {
                await client.DeleteStoredProcedureAsync(procedure.SelfLink);
            }

            storedProcedure = await client.CreateStoredProcedureAsync(collection.Resource.SelfLink, storedProcedure);

            var response = await client.ExecuteStoredProcedureAsync<string>(storedProcedure.SelfLink, 
                                                                            new RequestOptions { PartitionKey = new PartitionKey("web")},
                                                                            " where r.name= 'Wetpaint'");
            Console.WriteLine("The response is {0}", response.Response);
        }
        #endregion Data_Manipulation
    }
}
