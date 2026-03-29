"""Editable schema definitions for the Yelp Text-to-SQL project.

This file is the single place where you can maintain table names, columns,
short descriptions, and join keys for prompt building.

Live schema verification guide:
1. Run `DESCRIBE business`, `DESCRIBE rating`, `DESCRIBE users`, and `DESCRIBE checkin`
   in your real Hive or Spark SQL environment.
2. Keep only exact table names and exact column names that truly exist.
3. Remove any column below that is not present in your real table version.
4. Fill or update the TODO comments instead of guessing.
5. Confirm that the join keys match your actual ETL output.
"""

SCHEMA_VERIFICATION_CHECKLIST = [
    "Confirm the table names are exactly: business, rating, users, checkin.",
    "Confirm every column name with DESCRIBE or SHOW COLUMNS in your live backend.",
    "Remove any column that does not exist in your real table version.",
    "Confirm that the business table contains a postal_code column with an underscore.",
    "Confirm that business.categories is an array of strings.",
    "Confirm that business.attributes and business.hours are object-like fields in your backend representation.",
    "Confirm that checkin.date is stored as a comma-separated list of timestamps.",
    "Confirm the join keys match your actual Hive or Spark tables.",
    "Add real sample values to SAMPLE_VALUE_HINTS only after verifying them from your own data.",
]

TABLE_SCHEMAS = {
    "business": {
        "description": "Business listing information from the Yelp dataset. Each row represents one merchant or storefront location.",
        "join_keys": [
            "business.business_id = rating.business_id",
            "business.business_id = checkin.business_id",
        ],
        "verification_todos": [
            "Run DESCRIBE business and confirm each listed column exists.",
            "Confirm that postal_code uses an underscore in the live backend.",
            "Confirm that categories is an array of strings rather than one flattened string.",
            "Confirm how attributes and hours are represented in your Hive or Spark backend.",
        ],
        "columns": [
            {"name": "business_id", "description": "Unique business identifier."},
            {"name": "name", "description": "Business name. Questions about merchants or restaurant names usually refer to this field."},
            {"name": "address", "description": "Full business address."},
            {"name": "city", "description": "City where the business is located."},
            {"name": "state", "description": "2-letter state code."},
            {"name": "postal_code", "description": "Postal or ZIP code for the business location."},
            {"name": "latitude", "description": "Latitude coordinate."},
            {"name": "longitude", "description": "Longitude coordinate."},
            {"name": "stars", "description": "Average Yelp star rating for the business, rounded to half-stars. Use this for business-level ratings, not individual review ratings."},
            {"name": "review_count", "description": "Number of reviews received by the business. This is a business-level aggregate, not the number of reviews written by a user."},
            {"name": "is_open", "description": "Open status flag stored as 0 or 1."},
            {"name": "attributes", "description": "Business attributes object from Yelp."},
            {"name": "categories", "description": "Array of category strings such as Restaurants, Pizza, or Italian. Cuisine and restaurant-type questions usually use this field."},
            {"name": "hours", "description": "Business hours object from Yelp."},
        ],
    },
    "rating": {
        "description": "Review records written for businesses. This table contains reviews but is named rating in the live backend.",
        "join_keys": [
            "rating.business_id = business.business_id",
            "rating.user_id = users.user_id",
        ],
        "verification_todos": [
            "Run DESCRIBE rating and confirm that review_id, user_id, business_id, and date use these exact names.",
            "Confirm that date is stored in YYYY-MM-DD string format.",
            "Confirm that text remains available for review-text and NLP-style questions.",
        ],
        "columns": [
            {"name": "review_id", "description": "Unique review identifier."},
            {"name": "user_id", "description": "Reviewer identifier."},
            {"name": "business_id", "description": "Reviewed business identifier."},
            {"name": "stars", "description": "Review-level star rating from 1 to 5. Use this for rating counts, distributions, or five-star review totals."},
            {"name": "date", "description": "Review date stored as YYYY-MM-DD. Use this for yearly, monthly, weekly, or trend analyses of reviews."},
            {"name": "text", "description": "The actual review text content."},
            {"name": "useful", "description": "Count of useful votes received by the review."},
            {"name": "funny", "description": "Count of funny votes received by the review."},
            {"name": "cool", "description": "Count of cool votes received by the review."},
        ],
    },
    "users": {
        "description": "User profile information from the Yelp dataset. Each row represents one Yelp user account.",
        "join_keys": [
            "users.user_id = rating.user_id",
        ],
        "verification_todos": [
            "Run DESCRIBE users and confirm the table is named users in the live backend.",
            "Confirm that friends and elite are array-like fields.",
            "Confirm the available compliment_* columns exactly match the live table.",
            "Confirm that yelping_since is stored in YYYY-MM-DD string format.",
        ],
        "columns": [
            {"name": "user_id", "description": "Unique user identifier."},
            {"name": "name", "description": "User's first name."},
            {"name": "review_count", "description": "Number of reviews written by the user. Use this for top reviewers or reviewer activity rankings."},
            {"name": "yelping_since", "description": "Date when the user joined Yelp. Use this for yearly user-growth or cohort analyses."},
            {"name": "friends", "description": "Array of friend user_ids."},
            {"name": "useful", "description": "Useful vote count for the user."},
            {"name": "funny", "description": "Funny vote count for the user."},
            {"name": "cool", "description": "Cool vote count for the user."},
            {"name": "fans", "description": "Number of fans. Use this for popular-user questions."},
            {"name": "elite", "description": "Array of elite years for the user."},
            {"name": "average_stars", "description": "User's average star rating across the reviews they have written."},
            {"name": "compliment_hot", "description": "Hot compliments count."},
            {"name": "compliment_more", "description": "More compliments count."},
            {"name": "compliment_profile", "description": "Profile compliments count."},
            {"name": "compliment_cute", "description": "Cute compliments count."},
            {"name": "compliment_list", "description": "List compliments count."},
            {"name": "compliment_note", "description": "Note compliments count."},
            {"name": "compliment_plain", "description": "Plain compliments count."},
            {"name": "compliment_cool", "description": "Cool compliments count."},
            {"name": "compliment_photos", "description": "Photo compliments count."},
        ],
    },
    "checkin": {
        "description": "Business check-in data from the Yelp dataset. The date field is a comma-separated list of check-in timestamps.",
        "join_keys": [
            "checkin.business_id = business.business_id",
        ],
        "verification_todos": [
            "Run DESCRIBE checkin and confirm business_id and date are the only required columns.",
            "Confirm that date is still the raw comma-separated timestamp list.",
            "If you later explode check-ins into separate rows, update this schema before expecting time-based SQL to work directly.",
        ],
        "columns": [
            {"name": "business_id", "description": "Business identifier."},
            {"name": "date", "description": "Comma-separated list of check-in timestamps for the business."},
        ],
    },
}

SAMPLE_VALUE_HINTS = """
Business vocabulary:
- merchant, business, storefront, and restaurant usually refer to rows in the business table.
- review means one row in the rating table, because the live review table is named rating.
- reviewer or user means one row in the users table.

Field semantics:
- business.stars is the average rating of a business.
- rating.stars is the rating inside one individual review.
- business.review_count is the number of reviews received by one business.
- users.review_count is the number of reviews written by one user.
- users.elite is used for elite-user questions.
- users.fans is used for popular-user questions.
- business.postal_code is the postal or ZIP code column.
- business.categories is an array of strings, not one flat string.
- checkin.date is a comma-separated list of timestamps rather than one clean date column.

Common cuisine and category hints from the project brief:
- The broad restaurant category is "Restaurants".
- Common cuisine examples include American, Mexican, Italian, Japanese, Chinese, Thai, Mediterranean, French, Vietnamese, Greek, Indian, Korean, Hawaiian, African, Spanish, and Middle Eastern.
- Cuisine or restaurant-type questions often rely on membership checks against business.categories.

Common analysis mappings from the project brief:
- "top cities with the most merchants" usually means count businesses grouped by business.city.
- "top states with the most merchants" usually means count businesses grouped by business.state.
- "highest-rated businesses" usually means order by business.stars.
- "five-star reviews" usually means rating.stars = 5.
- "reviews per year" usually means extract year from rating.date.
- "users joining each year" usually means extract year from users.yelping_since.
- "top reviewers" usually means order users by users.review_count.
- "check-in" questions should use the checkin table, but raw checkin.date may need string parsing or preprocessing before time-based aggregation.

Scope caution:
- Some coursework ideas mention NLP, weather, census, or other external data. Only answer those directly when the needed fields or datasets actually exist in the live backend.
"""


def get_table_schemas() -> dict[str, dict[str, object]]:
    """Return the raw editable schema structure."""
    return TABLE_SCHEMAS


def get_schema_verification_checklist() -> list[str]:
    """Return the manual checklist for verifying the real backend schema."""
    return SCHEMA_VERIFICATION_CHECKLIST


def _build_schema_text() -> str:
    """Convert the editable schema dictionary into prompt-friendly text."""
    sections: list[str] = []

    for table_name, table_info in TABLE_SCHEMAS.items():
        lines = [
            f"Table: {table_name}",
            f"Description: {table_info['description']}",
            "Join keys:",
        ]

        for join_key in table_info["join_keys"]:
            lines.append(f"- {join_key}")

        lines.append("Columns:")
        for column in table_info["columns"]:
            column_name = str(column["name"])
            if " " in column_name and not column_name.startswith("`"):
                column_name = f"`{column_name}`"
            lines.append(f"- {column_name}: {column['description']}")

        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def get_schema_text() -> str:
    """Return schema text for prompt building."""
    return _build_schema_text().strip()


def get_sample_value_hints() -> str:
    """Return optional data hints for prompt building."""
    return SAMPLE_VALUE_HINTS.strip()
