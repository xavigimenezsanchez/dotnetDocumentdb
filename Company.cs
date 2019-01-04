using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dotnetDocumentdb {
    public class Company
    {
        public string Id { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("homepage_url")]
        public string HomepageUrl { get; set; }
        [JsonProperty("crunchbase_url")]
        public string CrunchBaseUrl { get; set; }
        [JsonProperty("category_code")]
        public string Category_Code { get; set; }
    }
}
