"""
This module stores pre-validated, static JSON data for the "Advanced Research Laboratory"
hypotheses. In a real-world scenario, this data would be the result of complex,
long-running data fusion and analysis pipelines. For this demo, we use static
snapshots to ensure sub-second response times.
"""

WEATHER_MOOD_HYPOTHESIS = {
    "hypothesis": "Customer mood, reflected in review ratings, is correlated with weather conditions. Bad weather leads to worse reviews.",
    "data_fusion_map": {
        "latitude": [36.17, 34.05, 40.71],
        "longitude": [-115.14, -118.24, -74.00],
        "city": ["Las Vegas", "Los Angeles", "New York"],
        "avg_rating_sunny": [3.8, 3.9, 3.7],
        "avg_rating_rainy": [3.5, 3.6, 3.4],
    },
    "actionable_recommendation": "On rainy days, businesses could offer discounts or special promotions to mitigate potentially lower customer satisfaction.",
    "external_data_source": "https://www.ncdc.noaa.gov/cdo-web/",
}

CURSED_STOREFRONTS_ANALYSIS = {
    "hypothesis": "Locations with high business turnover (i.e., 'cursed storefronts') can be identified by analyzing business closures and correlating with external data like walkability scores.",
    "data_fusion_map": {
        "latitude": [36.11, 34.02, 40.75],
        "longitude": [-115.27, -118.49, -73.98],
        "address": ["4321 W Flamingo Rd", "123 Santa Monica Pier", "1501 Broadway"],
        "business_churn_rate": [0.8, 0.5, 0.6],
        "walk_score": [45, 92, 99],
    },
    "actionable_recommendation": "High-churn locations, even with high walk scores, may indicate underlying issues (e.g., rent, crime). Investors should perform deeper due diligence.",
    "external_data_source": "https://www.walkscore.com/",
}

REVIEW_MANIPULATION_SYNDICATE = {
    "hypothesis": "Coordinated groups of users posting similar reviews across a specific set of businesses can indicate review manipulation or fraud.",
    "actionable_recommendation": "Flag user accounts with highly overlapping review patterns for manual investigation. Implement stricter velocity checks on new reviews for the affected businesses.",
    "external_data_source": "https://www.fakespot.com/",
}

OPEN_WORLD_DATA_SAFARI = {
    "hypothesis": "Integrating demographic data can reveal untapped market opportunities. For example, areas with a high density of young professionals might favor trendy cafes over traditional restaurants.",
    "actionable_recommendation": "Use demographic data to tailor marketing campaigns and business offerings to the specific needs and preferences of the local population.",
    "external_data_source": "https://www.census.gov/data/developers.html",
}
