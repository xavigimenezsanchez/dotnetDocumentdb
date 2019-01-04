using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
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
                        //QueryWithStoredProcs("Xavi", "Company", @"./Scripts/SimpleQuery.js").Wait();
                    
                    /*Import documents with stored procedure */
                        //ImportDocument("Xavi", "Company", @"./Scripts/ImportDocument.js").Wait();
                    /* User-defined functions */
                        //getTotalFunding("TotalFunding", "Xavi", "Company").Wait();
                    /* Triggers */
                        addCreatedData("Xavi", "Company").Wait();
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

        /*
            Simple stored procedure query
         */
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

        /*
            Import documents with stored procedure
         */

        static async Task ImportDocument(string databaseId, string collectionId, string scriptPath)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));

            var selfLink = collection.Resource.SelfLink;

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

            string testJson = File.ReadAllText(@".\Data\test.json");
            var parameters = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(testJson)};

            var response = await client.ExecuteStoredProcedureAsync<string>(storedProcedure.SelfLink, 
                                                                            new RequestOptions { PartitionKey = new PartitionKey("advertising")},
                                                                            parameters);
            Console.WriteLine("The response is {0}", response.Response);
        }
        /* User-defined functions */
        static async Task getTotalFunding(string udfId, string databaseId, string collectionId)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));

            var collectionSelfLink = collection.Resource.SelfLink;

            var udfFileName = string.Format(@"Scripts\{0}.js", udfId);
            var udf = new UserDefinedFunction 
                            {
                                Id = udfId,
                                Body = File.ReadAllText(udfFileName)
                            };
            await tryDeleteUDF(collectionSelfLink, udf.Id);

            await client.CreateUserDefinedFunctionAsync(collectionSelfLink, udf);
            var results = client.CreateDocumentQuery<dynamic>(collectionSelfLink, 
                            "Select r.name as Name, udf.TotalFunding(r) as TotalFunding from root r where r.name='Cisco'",
                            new FeedOptions { EnableCrossPartitionQuery = true});
            foreach (var result in results) {
                Console.WriteLine("The result is {0}", result);
            }
        }

        private static async Task tryDeleteUDF(string collectionSelfLink, string udfId)
        {
            UserDefinedFunction udf = client.CreateUserDefinedFunctionQuery(collectionSelfLink).Where(x => x.Id == udfId).AsEnumerable().FirstOrDefault();

            if (udf != null) 
            {
                await client.DeleteUserDefinedFunctionAsync(udf.SelfLink);
            }
        }

        /* Triggers */

        static async Task addCreatedData(string databaseId, string collectionId)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));

            var collectionLink = collection.Resource.SelfLink;

            string triggerId = "AddCreateDate";
            string body = File.ReadAllText(@"Scripts\AddCreatedDate.js");

            Trigger trigger = new Trigger
                                {
                                    Id = triggerId,
                                    Body = body,
                                    TriggerOperation = TriggerOperation.Create,
                                    TriggerType = TriggerType.Pre
                                };

            await tryDeleteTrigger(collectionLink, trigger.Id);

            await client.CreateTriggerAsync(collectionLink, trigger);

            var requestOptions = new RequestOptions
                    {
                        PreTriggerInclude = new List<string> { triggerId },
                        PartitionKey = new PartitionKey("Company")
                    };
            await client.CreateDocumentAsync(   collectionLink,
                                                new {
                                                        id = Guid.NewGuid().ToString(),
                                                        category_code = "Company",
                                                        homepage_url ="http//xavi.com",
                                                        name = "Xavi"
                                                    },
                                                requestOptions);
            var results = client.CreateDocumentQuery<Document>( collectionLink,
                                                                "Select * from root r where r.category_code ='Company'",
                                                                new FeedOptions { EnableCrossPartitionQuery = true });

            foreach (var result in results)
            {
                Console.WriteLine("{0}", result);
            }
        }
        private static async Task tryDeleteTrigger(string collectionLink, string triggerId)
        {
            Trigger trigger = client.CreateTriggerQuery(collectionLink).Where(x => x.Id == triggerId).AsEnumerable().FirstOrDefault();

            if (trigger != null)
            {
                await client.DeleteTriggerAsync(trigger.SelfLink);
            }
        }
        #endregion Data_Manipulation
    }
}
