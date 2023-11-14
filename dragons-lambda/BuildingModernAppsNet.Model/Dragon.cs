using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace BuildingModernAppsNet.Model
{
    /// <summary>
    /// POCO with some attributes to match the format the data is in.
    /// </summary>
    public class Dragon
    {
#pragma warning disable CS1591
        [JsonPropertyName("description_str")]
        public string Description { get; set; }

        [JsonPropertyName("dragon_name_str")]
        public string DragonName { get; set; }

        [JsonPropertyName("family_str")]
        public string Family { get; set; }

        [JsonPropertyName("location_city_str")]
        public string LocationCity { get; set; }

        [JsonPropertyName("location_neighborhood_str")]
        public string LocationNeighborhood { get; set; }

        [JsonPropertyName("location_state_str")]
        public string LocationStateStr { get; set; }
#pragma warning restore CS1591
    }
}
