from __future__ import annotations

import os
from dataclasses import dataclass

from yelp_text_to_sql.config import ensure_environment_loaded, load_config
from yelp_text_to_sql.schema_definitions import (
    get_sample_value_hints,
    get_table_schemas,
)


@dataclass
class PromptBundle:
    system_prompt: str
    user_prompt: str
    schema_loaded: bool


@dataclass(frozen=True)
class GoldenQueryTemplate:
    key: str
    user_intent: str
    sql: str
    explanation: str


_US_STATE_CODES: tuple[str, ...] = (
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
)

_CUISINE_CATEGORIES: tuple[str, ...] = (
    "American",
    "Mexican",
    "Italian",
    "Japanese",
    "Chinese",
    "Thai",
    "Mediterranean",
    "French",
    "Vietnamese",
    "Greek",
    "Indian",
    "Korean",
    "Hawaiian",
    "African",
    "Spanish",
    "Middle Eastern",
)


_GOLDEN_QUERY_TEMPLATES: tuple[GoldenQueryTemplate, ...] = (
    GoldenQueryTemplate(
        key="reviews_per_year",
        user_intent="Count the number of reviews per year",
        sql=(
            "SELECT\n"
            "  YEAR(TO_DATE(date)) AS review_year,\n"
            "  COUNT(*) AS review_count\n"
            "FROM rating\n"
            "WHERE date IS NOT NULL\n"
            "GROUP BY YEAR(TO_DATE(date))\n"
            "ORDER BY review_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This groups rating rows by year and counts how many reviews happened in each one. "
            "It gives you a clean year-by-year trend line."
        ),
    ),
    GoldenQueryTemplate(
        key="cool_review_votes",
        user_intent="Count the number of cool review votes",
        sql=(
            "SELECT\n"
            "  COALESCE(SUM(cool), 0) AS total_cool_review_votes\n"
            "FROM rating"
        ),
        explanation=(
            "This adds up every cool vote stored on the rating table. "
            "It returns one total for all cool review votes combined."
        ),
    ),
    GoldenQueryTemplate(
        key="users_ranked_by_reviews_per_year",
        user_intent="Rank users by the total reviews per year (with names)",
        sql=(
            "WITH yearly_user_reviews AS (\n"
            "  SELECT\n"
            "    YEAR(TO_DATE(r.date)) AS review_year,\n"
            "    r.user_id,\n"
            "    u.name,\n"
            "    COUNT(*) AS total_reviews\n"
            "  FROM rating r\n"
            "  JOIN users u\n"
            "    ON r.user_id = u.user_id\n"
            "  WHERE r.date IS NOT NULL\n"
            "  GROUP BY YEAR(TO_DATE(r.date)), r.user_id, u.name\n"
            "),\n"
            "ranked_users AS (\n"
            "  SELECT\n"
            "    review_year,\n"
            "    user_id,\n"
            "    name,\n"
            "    total_reviews,\n"
            "    DENSE_RANK() OVER (\n"
            "      PARTITION BY review_year\n"
            "      ORDER BY total_reviews DESC, name ASC\n"
            "    ) AS review_rank\n"
            "  FROM yearly_user_reviews\n"
            ")\n"
            "SELECT\n"
            "  review_year,\n"
            "  review_rank,\n"
            "  user_id,\n"
            "  name,\n"
            "  total_reviews\n"
            "FROM ranked_users\n"
            "ORDER BY review_year DESC, review_rank ASC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This counts how many reviews each user wrote in each year and then ranks them inside that year. "
            "You get names, totals, and yearly rank in one result."
        ),
    ),
    GoldenQueryTemplate(
        key="top_words_all_reviews",
        user_intent="Extract the Top 20 words from all reviews",
        sql=(
            "WITH cleaned_reviews AS (\n"
            "  SELECT LOWER(REGEXP_REPLACE(text, '[^a-zA-Z0-9 ]', ' ')) AS clean_text\n"
            "  FROM rating\n"
            "  WHERE text IS NOT NULL AND TRIM(text) <> ''\n"
            "),\n"
            "tokenized_words AS (\n"
            "  SELECT EXPLODE(SPLIT(clean_text, '\\\\s+')) AS word\n"
            "  FROM cleaned_reviews\n"
            "),\n"
            "filtered_words AS (\n"
            "  SELECT word\n"
            "  FROM tokenized_words\n"
            "  WHERE word <> ''\n"
            "    AND LENGTH(word) >= 3\n"
            "    AND word NOT IN (\n"
            "      'the', 'and', 'for', 'that', 'with', 'this', 'was', 'are', 'you', 'but',\n"
            "      'have', 'had', 'not', 'all', 'our', 'out', 'just', 'from', 'very', 'too'\n"
            "    )\n"
            ")\n"
            "SELECT\n"
            "  word,\n"
            "  COUNT(*) AS word_count\n"
            "FROM filtered_words\n"
            "GROUP BY word\n"
            "ORDER BY word_count DESC, word ASC\n"
            "LIMIT 20"
        ),
        explanation=(
            "This cleans the review text, splits it into words, removes tiny common filler words, and counts what is left. "
            "It returns the 20 most frequent words across all reviews."
        ),
    ),
    GoldenQueryTemplate(
        key="top_words_positive_reviews",
        user_intent="Extract the Top 10 words from positive reviews",
        sql=(
            "WITH cleaned_reviews AS (\n"
            "  SELECT LOWER(REGEXP_REPLACE(text, '[^a-zA-Z0-9 ]', ' ')) AS clean_text\n"
            "  FROM rating\n"
            "  WHERE stars >= 4 AND text IS NOT NULL AND TRIM(text) <> ''\n"
            "),\n"
            "tokenized_words AS (\n"
            "  SELECT EXPLODE(SPLIT(clean_text, '\\\\s+')) AS word\n"
            "  FROM cleaned_reviews\n"
            "),\n"
            "filtered_words AS (\n"
            "  SELECT word\n"
            "  FROM tokenized_words\n"
            "  WHERE word <> ''\n"
            "    AND LENGTH(word) >= 3\n"
            "    AND word NOT IN (\n"
            "      'the', 'and', 'for', 'that', 'with', 'this', 'was', 'are', 'you', 'but',\n"
            "      'have', 'had', 'not', 'all', 'our', 'out', 'just', 'from', 'very', 'too'\n"
            "    )\n"
            ")\n"
            "SELECT\n"
            "  word,\n"
            "  COUNT(*) AS word_count\n"
            "FROM filtered_words\n"
            "GROUP BY word\n"
            "ORDER BY word_count DESC, word ASC\n"
            "LIMIT 10"
        ),
        explanation=(
            "This only looks at high-star reviews before cleaning and counting the words. "
            "It shows the 10 words that appear most often in positive feedback."
        ),
    ),
    GoldenQueryTemplate(
        key="top_words_negative_reviews",
        user_intent="Extract the Top 10 words from negative reviews",
        sql=(
            "WITH cleaned_reviews AS (\n"
            "  SELECT LOWER(REGEXP_REPLACE(text, '[^a-zA-Z0-9 ]', ' ')) AS clean_text\n"
            "  FROM rating\n"
            "  WHERE stars <= 2 AND text IS NOT NULL AND TRIM(text) <> ''\n"
            "),\n"
            "tokenized_words AS (\n"
            "  SELECT EXPLODE(SPLIT(clean_text, '\\\\s+')) AS word\n"
            "  FROM cleaned_reviews\n"
            "),\n"
            "filtered_words AS (\n"
            "  SELECT word\n"
            "  FROM tokenized_words\n"
            "  WHERE word <> ''\n"
            "    AND LENGTH(word) >= 3\n"
            "    AND word NOT IN (\n"
            "      'the', 'and', 'for', 'that', 'with', 'this', 'was', 'are', 'you', 'but',\n"
            "      'have', 'had', 'not', 'all', 'our', 'out', 'just', 'from', 'very', 'too'\n"
            "    )\n"
            ")\n"
            "SELECT\n"
            "  word,\n"
            "  COUNT(*) AS word_count\n"
            "FROM filtered_words\n"
            "GROUP BY word\n"
            "ORDER BY word_count DESC, word ASC\n"
            "LIMIT 10"
        ),
        explanation=(
            "This keeps only low-star reviews before turning the text into words and counting them. "
            "It surfaces the 10 words that show up most often in negative feedback."
        ),
    ),
    GoldenQueryTemplate(
        key="average_review_count_per_rating_tier",
        user_intent="Calculate the average review count per rating tier (1 to 5)",
        sql=(
            "WITH business_rating_tiers AS (\n"
            "  SELECT\n"
            "    CAST(ROUND(stars) AS INT) AS rating_tier,\n"
            "    review_count\n"
            "  FROM business\n"
            "  WHERE stars IS NOT NULL\n"
            "    AND review_count IS NOT NULL\n"
            "    AND stars BETWEEN 1 AND 5\n"
            ")\n"
            "SELECT\n"
            "  rating_tier,\n"
            "  AVG(review_count) AS average_review_count\n"
            "FROM business_rating_tiers\n"
            "WHERE rating_tier BETWEEN 1 AND 5\n"
            "GROUP BY rating_tier\n"
            "ORDER BY rating_tier ASC\n"
            "LIMIT 5"
        ),
        explanation=(
            "This groups businesses into star tiers from 1 to 5 and averages their review counts inside each tier. "
            "It shows whether higher-rated businesses tend to have more reviews."
        ),
    ),
    GoldenQueryTemplate(
        key="top_businesses_low_star_reviews",
        user_intent="Extract the top 15 businesses with 1-star and 2-star reviews",
        sql=(
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  COUNT(*) AS low_star_review_count,\n"
            "  AVG(r.stars) AS average_low_star_rating\n"
            "FROM rating r\n"
            "JOIN business b\n"
            "  ON r.business_id = b.business_id\n"
            "WHERE r.stars IN (1, 2)\n"
            "GROUP BY b.business_id, b.name, b.city, b.state\n"
            "ORDER BY low_star_review_count DESC, average_low_star_rating ASC, b.name ASC\n"
            "LIMIT 15"
        ),
        explanation=(
            "This finds businesses that collected the most 1-star and 2-star reviews. "
            "It ranks them by how many low ratings they received."
        ),
    ),
    GoldenQueryTemplate(
        key="reviews_with_positive_keywords",
        user_intent="Identify reviews with certain positive keywords",
        sql=(
            "SELECT\n"
            "  review_id,\n"
            "  business_id,\n"
            "  user_id,\n"
            "  date,\n"
            "  stars,\n"
            "  REGEXP_EXTRACT(LOWER(text), '\\\\b(amazing|excellent|great|love|friendly|delicious|perfect)\\\\b', 1) AS matched_keyword,\n"
            "  text\n"
            "FROM rating\n"
            "WHERE text IS NOT NULL\n"
            "  AND LOWER(text) RLIKE '\\\\b(amazing|excellent|great|love|friendly|delicious|perfect)\\\\b'\n"
            "ORDER BY date DESC, stars DESC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This looks for reviews whose text contains one of the built-in positive keywords. "
            "It returns the matching review plus the exact keyword that was found."
        ),
    ),
    GoldenQueryTemplate(
        key="frequent_chinese_restaurants",
        user_intent="Rank most frequent Chinese restaurants",
        sql=(
            "SELECT\n"
            "  business_id,\n"
            "  name,\n"
            "  city,\n"
            "  state,\n"
            "  stars,\n"
            "  review_count\n"
            "FROM business\n"
            "WHERE array_contains(categories, 'Chinese')\n"
            "  AND array_contains(categories, 'Restaurants')\n"
            "  AND review_count > 50\n"
            "ORDER BY review_count DESC, stars DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This keeps only Chinese restaurants with a meaningful number of reviews and then ranks them. "
            "The ordering favors the busiest places first, then stronger ratings."
        ),
    ),
    GoldenQueryTemplate(
        key="turnaround_merchants",
        user_intent="Find turnaround merchants",
        sql=(
            "WITH merchant_rating_windows AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    AVG(CASE WHEN TO_DATE(date) >= DATE_SUB(CURRENT_DATE(), 365) THEN CAST(stars AS DOUBLE) END) AS recent_avg_stars,\n"
            "    AVG(CASE WHEN TO_DATE(date) < DATE_SUB(CURRENT_DATE(), 365) THEN CAST(stars AS DOUBLE) END) AS historical_avg_stars,\n"
            "    COUNT(CASE WHEN TO_DATE(date) >= DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) AS recent_review_count,\n"
            "    COUNT(CASE WHEN TO_DATE(date) < DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) AS historical_review_count\n"
            "  FROM rating\n"
            "  WHERE date IS NOT NULL\n"
            "  GROUP BY business_id\n"
            ")\n"
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  m.recent_avg_stars,\n"
            "  m.historical_avg_stars,\n"
            "  m.recent_avg_stars - m.historical_avg_stars AS rating_improvement\n"
            "FROM merchant_rating_windows m\n"
            "JOIN business b\n"
            "  ON m.business_id = b.business_id\n"
            "WHERE m.recent_review_count > 0\n"
            "  AND m.historical_review_count > 0\n"
            "  AND m.recent_avg_stars - m.historical_avg_stars >= 1.0\n"
            "ORDER BY rating_improvement DESC, m.recent_avg_stars DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This compares each business's recent average rating from the last 12 months to its older historical average. "
            "It returns businesses that improved by at least one full star."
        ),
    ),
    GoldenQueryTemplate(
        key="category_synergy",
        user_intent="Find category synergy pairs",
        sql=(
            "WITH exploded_categories AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    EXPLODE(categories) AS category\n"
            "  FROM business\n"
            "  WHERE categories IS NOT NULL\n"
            "    AND SIZE(categories) > 1\n"
            "),\n"
            "paired_categories AS (\n"
            "  SELECT\n"
            "    left_side.category AS category_1,\n"
            "    right_side.category AS category_2\n"
            "  FROM exploded_categories left_side\n"
            "  JOIN exploded_categories right_side\n"
            "    ON left_side.business_id = right_side.business_id\n"
            "   AND left_side.category < right_side.category\n"
            ")\n"
            "SELECT\n"
            "  category_1,\n"
            "  category_2,\n"
            "  COUNT(*) AS cooccurrence_count\n"
            "FROM paired_categories\n"
            "GROUP BY category_1, category_2\n"
            "ORDER BY cooccurrence_count DESC, category_1 ASC, category_2 ASC\n"
            "LIMIT 10"
        ),
        explanation=(
            "This explodes each business category list into single categories and pairs them inside the same business. "
            "It finds the top category combinations that show up together most often."
        ),
    ),
    GoldenQueryTemplate(
        key="polarizing_businesses",
        user_intent="Find polarizing businesses",
        sql=(
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  COUNT(r.stars) AS rating_count,\n"
            "  STDDEV(CAST(r.stars AS DOUBLE)) AS rating_stddev\n"
            "FROM rating r\n"
            "JOIN business b\n"
            "  ON r.business_id = b.business_id\n"
            "GROUP BY b.business_id, b.name, b.city, b.state\n"
            "HAVING COUNT(r.stars) > 50\n"
            "ORDER BY rating_stddev DESC, rating_count DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This measures how spread out each business's ratings are after keeping only businesses with lots of reviews. "
            "A higher standard deviation means the business is more polarizing."
        ),
    ),
    GoldenQueryTemplate(
        key="users_joining_each_year",
        user_intent="Analyze the number of users joining each year",
        sql=(
            "SELECT\n"
            "  YEAR(TO_DATE(yelping_since)) AS join_year,\n"
            "  COUNT(*) AS new_user_count\n"
            "FROM users\n"
            "WHERE yelping_since IS NOT NULL\n"
            "GROUP BY YEAR(TO_DATE(yelping_since))\n"
            "ORDER BY join_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This groups users by the year they first joined Yelp. "
            "It shows how many new users appeared in each year."
        ),
    ),
    GoldenQueryTemplate(
        key="top_reviewers_by_review_count",
        user_intent="Identify top reviewers based on review_count",
        sql=(
            "SELECT\n"
            "  user_id,\n"
            "  name,\n"
            "  review_count,\n"
            "  average_stars,\n"
            "  fans\n"
            "FROM users\n"
            "WHERE review_count IS NOT NULL\n"
            "ORDER BY review_count DESC, fans DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This ranks users by the review_count field already stored on their profile. "
            "It returns the most active reviewers first."
        ),
    ),
    GoldenQueryTemplate(
        key="popular_users_by_fans",
        user_intent="Identify the most popular users based on fans",
        sql=(
            "SELECT\n"
            "  user_id,\n"
            "  name,\n"
            "  fans,\n"
            "  review_count,\n"
            "  average_stars\n"
            "FROM users\n"
            "WHERE fans IS NOT NULL\n"
            "ORDER BY fans DESC, review_count DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This sorts users by their fan count and then breaks ties with review activity. "
            "It finds the most popular users first."
        ),
    ),
    GoldenQueryTemplate(
        key="elite_ratio_each_year",
        user_intent="Calculate the ratio of elite users to regular users each year",
        sql=(
            "WITH yearly_user_cohorts AS (\n"
            "  SELECT\n"
            "    YEAR(TO_DATE(yelping_since)) AS join_year,\n"
            "    CASE WHEN elite IS NOT NULL AND SIZE(elite) > 0 THEN 1 ELSE 0 END AS is_elite_user\n"
            "  FROM users\n"
            "  WHERE yelping_since IS NOT NULL\n"
            ")\n"
            "SELECT\n"
            "  join_year,\n"
            "  SUM(is_elite_user) AS elite_user_count,\n"
            "  COUNT(*) - SUM(is_elite_user) AS regular_user_count,\n"
            "  CASE\n"
            "    WHEN COUNT(*) - SUM(is_elite_user) = 0 THEN NULL\n"
            "    ELSE CAST(SUM(is_elite_user) AS DOUBLE) / CAST(COUNT(*) - SUM(is_elite_user) AS DOUBLE)\n"
            "  END AS elite_to_regular_ratio\n"
            "FROM yearly_user_cohorts\n"
            "GROUP BY join_year\n"
            "ORDER BY join_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This groups users by join year and marks whether each one ever became elite. "
            "It then compares elite users to regular users inside each year."
        ),
    ),
    GoldenQueryTemplate(
        key="silent_user_proportion_each_year",
        user_intent="Display the proportion of total users and silent users (who haven't written reviews) each year",
        sql=(
            "WITH yearly_users AS (\n"
            "  SELECT\n"
            "    YEAR(TO_DATE(yelping_since)) AS join_year,\n"
            "    CASE WHEN COALESCE(review_count, 0) = 0 THEN 1 ELSE 0 END AS is_silent_user\n"
            "  FROM users\n"
            "  WHERE yelping_since IS NOT NULL\n"
            ")\n"
            "SELECT\n"
            "  join_year,\n"
            "  COUNT(*) AS total_users,\n"
            "  SUM(is_silent_user) AS silent_users,\n"
            "  CAST(SUM(is_silent_user) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) AS silent_user_ratio\n"
            "FROM yearly_users\n"
            "GROUP BY join_year\n"
            "ORDER BY join_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This groups users by join year and flags the ones who never wrote a review. "
            "It shows both the counts and the silent-user share for each year."
        ),
    ),
    GoldenQueryTemplate(
        key="yearly_user_review_elite_tip_checkin_stats",
        user_intent="Compute the yearly statistics of new users, number of reviews, elite users, tips, and check-ins.",
        sql=(
            "WITH new_users_by_year AS (\n"
            "  SELECT\n"
            "    YEAR(TO_DATE(yelping_since)) AS activity_year,\n"
            "    COUNT(*) AS new_user_count\n"
            "  FROM users\n"
            "  WHERE yelping_since IS NOT NULL\n"
            "  GROUP BY YEAR(TO_DATE(yelping_since))\n"
            "),\n"
            "reviews_by_year AS (\n"
            "  SELECT\n"
            "    YEAR(TO_DATE(date)) AS activity_year,\n"
            "    COUNT(*) AS review_count\n"
            "  FROM rating\n"
            "  WHERE date IS NOT NULL\n"
            "  GROUP BY YEAR(TO_DATE(date))\n"
            "),\n"
            "elite_users_by_year AS (\n"
            "  SELECT\n"
            "    CAST(elite_year AS INT) AS activity_year,\n"
            "    COUNT(DISTINCT user_id) AS elite_user_count\n"
            "  FROM users\n"
            "  LATERAL VIEW EXPLODE(elite) exploded_elite AS elite_year\n"
            "  WHERE elite IS NOT NULL\n"
            "    AND elite_year RLIKE '^[0-9]{4}$'\n"
            "  GROUP BY CAST(elite_year AS INT)\n"
            "),\n"
            "checkins_by_year AS (\n"
            "  SELECT\n"
            "    YEAR(TO_TIMESTAMP(TRIM(checkin_ts))) AS activity_year,\n"
            "    COUNT(*) AS checkin_count\n"
            "  FROM checkin\n"
            "  LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "  WHERE date IS NOT NULL\n"
            "    AND TRIM(checkin_ts) <> ''\n"
            "  GROUP BY YEAR(TO_TIMESTAMP(TRIM(checkin_ts)))\n"
            "),\n"
            "all_years AS (\n"
            "  SELECT activity_year FROM new_users_by_year\n"
            "  UNION\n"
            "  SELECT activity_year FROM reviews_by_year\n"
            "  UNION\n"
            "  SELECT activity_year FROM elite_users_by_year\n"
            "  UNION\n"
            "  SELECT activity_year FROM checkins_by_year\n"
            ")\n"
            "SELECT\n"
            "  y.activity_year,\n"
            "  COALESCE(n.new_user_count, 0) AS new_user_count,\n"
            "  COALESCE(r.review_count, 0) AS review_count,\n"
            "  COALESCE(e.elite_user_count, 0) AS elite_user_count,\n"
            "  CAST(NULL AS BIGINT) AS tip_count_unavailable,\n"
            "  COALESCE(c.checkin_count, 0) AS checkin_count\n"
            "FROM all_years y\n"
            "LEFT JOIN new_users_by_year n ON y.activity_year = n.activity_year\n"
            "LEFT JOIN reviews_by_year r ON y.activity_year = r.activity_year\n"
            "LEFT JOIN elite_users_by_year e ON y.activity_year = e.activity_year\n"
            "LEFT JOIN checkins_by_year c ON y.activity_year = c.activity_year\n"
            "ORDER BY y.activity_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This combines yearly new-user, review, elite-user, and check-in counts into one timeline. "
            "The tips column is intentionally null because the live schema does not include a tips table."
        ),
    ),
    GoldenQueryTemplate(
        key="early_adopters_tastemakers",
        user_intent="Identify early adopters (tastemakers) who wrote one of the first 5 reviews for restaurants that eventually achieved a 4.5+ star average.",
        sql=(
            "WITH high_average_restaurants AS (\n"
            "  SELECT\n"
            "    b.business_id,\n"
            "    b.name AS business_name,\n"
            "    b.city,\n"
            "    b.state,\n"
            "    AVG(CAST(r.stars AS DOUBLE)) AS business_avg_stars\n"
            "  FROM business b\n"
            "  JOIN rating r\n"
            "    ON b.business_id = r.business_id\n"
            "  WHERE array_contains(b.categories, 'Restaurants')\n"
            "    AND r.date IS NOT NULL\n"
            "  GROUP BY b.business_id, b.name, b.city, b.state\n"
            "  HAVING AVG(CAST(r.stars AS DOUBLE)) >= 4.5\n"
            "),\n"
            "ranked_reviews AS (\n"
            "  SELECT\n"
            "    r.business_id,\n"
            "    r.user_id,\n"
            "    r.review_id,\n"
            "    r.date,\n"
            "    r.stars,\n"
            "    ROW_NUMBER() OVER (\n"
            "      PARTITION BY r.business_id\n"
            "      ORDER BY TO_DATE(r.date) ASC, r.review_id ASC\n"
            "    ) AS review_sequence\n"
            "  FROM rating r\n"
            "  JOIN high_average_restaurants h\n"
            "    ON r.business_id = h.business_id\n"
            "  WHERE r.date IS NOT NULL\n"
            ")\n"
            "SELECT\n"
            "  h.business_id,\n"
            "  h.business_name,\n"
            "  h.city,\n"
            "  h.state,\n"
            "  h.business_avg_stars,\n"
            "  rr.review_sequence,\n"
            "  rr.review_id,\n"
            "  rr.date,\n"
            "  rr.stars,\n"
            "  u.user_id,\n"
            "  u.name,\n"
            "  u.review_count,\n"
            "  u.average_stars\n"
            "FROM ranked_reviews rr\n"
            "JOIN high_average_restaurants h\n"
            "  ON rr.business_id = h.business_id\n"
            "JOIN users u\n"
            "  ON rr.user_id = u.user_id\n"
            "WHERE rr.review_sequence <= 5\n"
            "ORDER BY h.business_avg_stars DESC, rr.business_id ASC, rr.review_sequence ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This first finds restaurants that ultimately reached a 4.5-star average, then ranks their earliest reviews. "
            "It returns the users who were among the first five reviewers for those successful restaurants."
        ),
    ),
    GoldenQueryTemplate(
        key="user_rating_evolution_first_vs_third_year",
        user_intent="Analyze user rating evolution (first year vs third year on the platform).",
        sql=(
            "WITH user_year_windows AS (\n"
            "  SELECT\n"
            "    user_id,\n"
            "    name,\n"
            "    YEAR(TO_DATE(yelping_since)) AS first_year,\n"
            "    YEAR(TO_DATE(yelping_since)) + 2 AS third_year\n"
            "  FROM users\n"
            "  WHERE yelping_since IS NOT NULL\n"
            "),\n"
            "user_rating_evolution AS (\n"
            "  SELECT\n"
            "    u.user_id,\n"
            "    u.name,\n"
            "    AVG(CASE WHEN YEAR(TO_DATE(r.date)) = u.first_year THEN CAST(r.stars AS DOUBLE) END) AS first_year_avg_stars,\n"
            "    AVG(CASE WHEN YEAR(TO_DATE(r.date)) = u.third_year THEN CAST(r.stars AS DOUBLE) END) AS third_year_avg_stars,\n"
            "    COUNT(CASE WHEN YEAR(TO_DATE(r.date)) = u.first_year THEN 1 END) AS first_year_review_count,\n"
            "    COUNT(CASE WHEN YEAR(TO_DATE(r.date)) = u.third_year THEN 1 END) AS third_year_review_count\n"
            "  FROM user_year_windows u\n"
            "  JOIN rating r\n"
            "    ON u.user_id = r.user_id\n"
            "  WHERE r.date IS NOT NULL\n"
            "  GROUP BY u.user_id, u.name\n"
            ")\n"
            "SELECT\n"
            "  user_id,\n"
            "  name,\n"
            "  first_year_avg_stars,\n"
            "  third_year_avg_stars,\n"
            "  third_year_avg_stars - first_year_avg_stars AS rating_change,\n"
            "  first_year_review_count,\n"
            "  third_year_review_count\n"
            "FROM user_rating_evolution\n"
            "WHERE first_year_review_count > 0\n"
            "  AND third_year_review_count > 0\n"
            "ORDER BY ABS(third_year_avg_stars - first_year_avg_stars) DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This compares each user's average review rating from their first year on the platform to their third year. "
            "It highlights how much their scoring behavior changed over time."
        ),
    ),
    GoldenQueryTemplate(
        key="user_dining_diversity",
        user_intent="Segment users by dining diversity (count distinct cuisine categories for adventurous eaters).",
        sql=(
            "WITH cuisine_reviews AS (\n"
            "  SELECT\n"
            "    r.user_id,\n"
            "    exploded_category AS cuisine_category\n"
            "  FROM rating r\n"
            "  JOIN business b\n"
            "    ON r.business_id = b.business_id\n"
            "  LATERAL VIEW EXPLODE(b.categories) exploded_categories AS exploded_category\n"
            "  WHERE array_contains(b.categories, 'Restaurants')\n"
            "    AND exploded_category IN ('American', 'Mexican', 'Italian', 'Japanese', 'Chinese', 'Thai', 'Mediterranean', 'French', 'Vietnamese', 'Greek', 'Indian', 'Korean', 'Hawaiian', 'African', 'Spanish', 'Middle Eastern')\n"
            "),\n"
            "user_cuisine_diversity AS (\n"
            "  SELECT\n"
            "    u.user_id,\n"
            "    u.name,\n"
            "    COUNT(DISTINCT cr.cuisine_category) AS distinct_cuisine_count,\n"
            "    COUNT(*) AS cuisine_review_rows\n"
            "  FROM cuisine_reviews cr\n"
            "  JOIN users u\n"
            "    ON cr.user_id = u.user_id\n"
            "  GROUP BY u.user_id, u.name\n"
            ")\n"
            "SELECT\n"
            "  user_id,\n"
            "  name,\n"
            "  distinct_cuisine_count,\n"
            "  cuisine_review_rows,\n"
            "  CASE\n"
            "    WHEN distinct_cuisine_count >= 8 THEN 'Highly Adventurous'\n"
            "    WHEN distinct_cuisine_count >= 4 THEN 'Moderately Adventurous'\n"
            "    ELSE 'Focused'\n"
            "  END AS dining_diversity_segment\n"
            "FROM user_cuisine_diversity\n"
            "ORDER BY distinct_cuisine_count DESC, cuisine_review_rows DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This counts how many distinct cuisine categories each user has reviewed across restaurant businesses. "
            "It then labels each user by how adventurous their dining pattern looks."
        ),
    ),
    GoldenQueryTemplate(
        key="elite_status_impact",
        user_intent="Identify elite status impact (average word count and useful votes BEFORE vs AFTER becoming elite).",
        sql=(
            "WITH first_elite_year AS (\n"
            "  SELECT\n"
            "    user_id,\n"
            "    MIN(CAST(elite_year AS INT)) AS first_elite_year\n"
            "  FROM users\n"
            "  LATERAL VIEW EXPLODE(elite) exploded_elite AS elite_year\n"
            "  WHERE elite IS NOT NULL\n"
            "    AND elite_year RLIKE '^[0-9]{4}$'\n"
            "  GROUP BY user_id\n"
            "),\n"
            "review_features AS (\n"
            "  SELECT\n"
            "    user_id,\n"
            "    TO_DATE(date) AS review_date,\n"
            "    SIZE(SPLIT(TRIM(text), '\\\\s+')) AS word_count,\n"
            "    COALESCE(useful, 0) AS useful_votes\n"
            "  FROM rating\n"
            "  WHERE date IS NOT NULL\n"
            "    AND text IS NOT NULL\n"
            "    AND TRIM(text) <> ''\n"
            "),\n"
            "before_after_metrics AS (\n"
            "  SELECT\n"
            "    e.user_id,\n"
            "    u.name,\n"
            "    e.first_elite_year,\n"
            "    AVG(CASE WHEN YEAR(r.review_date) < e.first_elite_year THEN CAST(r.word_count AS DOUBLE) END) AS avg_word_count_before_elite,\n"
            "    AVG(CASE WHEN YEAR(r.review_date) >= e.first_elite_year THEN CAST(r.word_count AS DOUBLE) END) AS avg_word_count_after_elite,\n"
            "    AVG(CASE WHEN YEAR(r.review_date) < e.first_elite_year THEN CAST(r.useful_votes AS DOUBLE) END) AS avg_useful_votes_before_elite,\n"
            "    AVG(CASE WHEN YEAR(r.review_date) >= e.first_elite_year THEN CAST(r.useful_votes AS DOUBLE) END) AS avg_useful_votes_after_elite,\n"
            "    COUNT(CASE WHEN YEAR(r.review_date) < e.first_elite_year THEN 1 END) AS reviews_before_elite,\n"
            "    COUNT(CASE WHEN YEAR(r.review_date) >= e.first_elite_year THEN 1 END) AS reviews_after_elite\n"
            "  FROM first_elite_year e\n"
            "  JOIN review_features r\n"
            "    ON e.user_id = r.user_id\n"
            "  JOIN users u\n"
            "    ON e.user_id = u.user_id\n"
            "  GROUP BY e.user_id, u.name, e.first_elite_year\n"
            ")\n"
            "SELECT\n"
            "  user_id,\n"
            "  name,\n"
            "  first_elite_year,\n"
            "  avg_word_count_before_elite,\n"
            "  avg_word_count_after_elite,\n"
            "  avg_word_count_after_elite - avg_word_count_before_elite AS word_count_lift,\n"
            "  avg_useful_votes_before_elite,\n"
            "  avg_useful_votes_after_elite,\n"
            "  avg_useful_votes_after_elite - avg_useful_votes_before_elite AS useful_vote_lift,\n"
            "  reviews_before_elite,\n"
            "  reviews_after_elite\n"
            "FROM before_after_metrics\n"
            "WHERE reviews_before_elite > 0\n"
            "  AND reviews_after_elite > 0\n"
            "ORDER BY useful_vote_lift DESC, word_count_lift DESC, name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This finds the first elite year for each user and compares their review writing before and after that point. "
            "It measures whether word count and useful votes improved after becoming elite."
        ),
    ),
    GoldenQueryTemplate(
        key="rating_distribution",
        user_intent="Analyze the distribution of ratings (1-5 stars)",
        sql=(
            "SELECT\n"
            "  stars AS rating_value,\n"
            "  COUNT(*) AS rating_frequency\n"
            "FROM rating\n"
            "WHERE stars BETWEEN 1 AND 5\n"
            "GROUP BY stars\n"
            "ORDER BY rating_value ASC\n"
            "LIMIT 5"
        ),
        explanation=(
            "This counts how many reviews fall into each star level from 1 to 5. "
            "It gives a simple distribution of rating values."
        ),
    ),
    GoldenQueryTemplate(
        key="weekly_rating_frequency",
        user_intent="Analyze the weekly rating frequency (Monday to Sunday)",
        sql=(
            "SELECT\n"
            "  CASE DAYOFWEEK(TO_DATE(date))\n"
            "    WHEN 2 THEN 'Monday'\n"
            "    WHEN 3 THEN 'Tuesday'\n"
            "    WHEN 4 THEN 'Wednesday'\n"
            "    WHEN 5 THEN 'Thursday'\n"
            "    WHEN 6 THEN 'Friday'\n"
            "    WHEN 7 THEN 'Saturday'\n"
            "    WHEN 1 THEN 'Sunday'\n"
            "  END AS weekday_name,\n"
            "  CASE DAYOFWEEK(TO_DATE(date))\n"
            "    WHEN 2 THEN 1\n"
            "    WHEN 3 THEN 2\n"
            "    WHEN 4 THEN 3\n"
            "    WHEN 5 THEN 4\n"
            "    WHEN 6 THEN 5\n"
            "    WHEN 7 THEN 6\n"
            "    WHEN 1 THEN 7\n"
            "  END AS weekday_order,\n"
            "  COUNT(*) AS rating_frequency\n"
            "FROM rating\n"
            "WHERE date IS NOT NULL\n"
            "GROUP BY DAYOFWEEK(TO_DATE(date))\n"
            "ORDER BY weekday_order ASC\n"
            "LIMIT 7"
        ),
        explanation=(
            "This turns each review date into a day of the week and counts how many ratings happened on each day. "
            "The results are ordered from Monday through Sunday."
        ),
    ),
    GoldenQueryTemplate(
        key="top_businesses_five_star_ratings",
        user_intent="Identify the top businesses with the most five-star ratings",
        sql=(
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  COUNT(*) AS five_star_rating_count\n"
            "FROM rating r\n"
            "JOIN business b\n"
            "  ON r.business_id = b.business_id\n"
            "WHERE r.stars = 5\n"
            "GROUP BY b.business_id, b.name, b.city, b.state\n"
            "ORDER BY five_star_rating_count DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This keeps only 5-star reviews, joins them to businesses, and counts how many each business received. "
            "It ranks the businesses with the strongest five-star volume first."
        ),
    ),
    GoldenQueryTemplate(
        key="top_cities_highest_ratings",
        user_intent="Identify the top 10 cities with the highest ratings",
        sql=(
            "SELECT\n"
            "  b.city,\n"
            "  COUNT(*) AS rating_count,\n"
            "  AVG(CAST(r.stars AS DOUBLE)) AS average_rating\n"
            "FROM rating r\n"
            "JOIN business b\n"
            "  ON r.business_id = b.business_id\n"
            "WHERE b.city IS NOT NULL\n"
            "GROUP BY b.city\n"
            "ORDER BY average_rating DESC, rating_count DESC, b.city ASC\n"
            "LIMIT 10"
        ),
        explanation=(
            "This joins reviews to cities and averages the review stars inside each city. "
            "It returns the 10 cities with the strongest average ratings."
        ),
    ),
    GoldenQueryTemplate(
        key="merchant_rating_differential_vs_city",
        user_intent="Calculate the rating differential: Compare each merchant's average rating against the city average",
        sql=(
            "WITH merchant_average_ratings AS (\n"
            "  SELECT\n"
            "    b.business_id,\n"
            "    b.name,\n"
            "    b.city,\n"
            "    b.state,\n"
            "    AVG(CAST(r.stars AS DOUBLE)) AS merchant_avg_rating,\n"
            "    COUNT(*) AS merchant_rating_count\n"
            "  FROM rating r\n"
            "  JOIN business b\n"
            "    ON r.business_id = b.business_id\n"
            "  WHERE b.city IS NOT NULL\n"
            "  GROUP BY b.business_id, b.name, b.city, b.state\n"
            "),\n"
            "city_average_ratings AS (\n"
            "  SELECT\n"
            "    city,\n"
            "    AVG(merchant_avg_rating) AS city_avg_rating\n"
            "  FROM merchant_average_ratings\n"
            "  GROUP BY city\n"
            ")\n"
            "SELECT\n"
            "  m.business_id,\n"
            "  m.name,\n"
            "  m.city,\n"
            "  m.state,\n"
            "  m.merchant_avg_rating,\n"
            "  c.city_avg_rating,\n"
            "  m.merchant_avg_rating - c.city_avg_rating AS rating_differential,\n"
            "  m.merchant_rating_count\n"
            "FROM merchant_average_ratings m\n"
            "JOIN city_average_ratings c\n"
            "  ON m.city = c.city\n"
            "ORDER BY rating_differential DESC, m.merchant_rating_count DESC, m.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This calculates each merchant's own average rating and compares it to the average merchant rating in the same city. "
            "A larger positive differential means the merchant is outperforming its city baseline."
        ),
    ),
    GoldenQueryTemplate(
        key="nightlife_weekend_vs_weekday_satisfaction",
        user_intent="Compare weekend vs. weekday satisfaction for the Nightlife category",
        sql=(
            "SELECT\n"
            "  CASE\n"
            "    WHEN DAYOFWEEK(TO_DATE(r.date)) IN (1, 7) THEN 'Weekend'\n"
            "    ELSE 'Weekday'\n"
            "  END AS day_segment,\n"
            "  COUNT(*) AS rating_count,\n"
            "  AVG(CAST(r.stars AS DOUBLE)) AS average_rating\n"
            "FROM rating r\n"
            "JOIN business b\n"
            "  ON r.business_id = b.business_id\n"
            "WHERE r.date IS NOT NULL\n"
            "  AND array_contains(b.categories, 'Nightlife')\n"
            "GROUP BY CASE\n"
            "  WHEN DAYOFWEEK(TO_DATE(r.date)) IN (1, 7) THEN 'Weekend'\n"
            "  ELSE 'Weekday'\n"
            "END\n"
            "ORDER BY day_segment ASC\n"
            "LIMIT 2"
        ),
        explanation=(
            "This splits Nightlife reviews into weekend and weekday groups based on the review date. "
            "It compares both review volume and average satisfaction across those two segments."
        ),
    ),
    GoldenQueryTemplate(
        key="checkins_per_year",
        user_intent="Count the number of check-ins per year",
        sql=(
            "SELECT\n"
            "  YEAR(TO_TIMESTAMP(TRIM(checkin_ts))) AS checkin_year,\n"
            "  COUNT(*) AS checkin_count\n"
            "FROM checkin\n"
            "LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "WHERE date IS NOT NULL\n"
            "  AND TRIM(checkin_ts) <> ''\n"
            "GROUP BY YEAR(TO_TIMESTAMP(TRIM(checkin_ts)))\n"
            "ORDER BY checkin_year ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This breaks the raw check-in timestamp string into one row per check-in and groups them by year. "
            "It returns the yearly check-in trend."
        ),
    ),
    GoldenQueryTemplate(
        key="checkins_per_hour",
        user_intent="Count the number of check-ins per hour within a 24-hour period",
        sql=(
            "SELECT\n"
            "  HOUR(TO_TIMESTAMP(TRIM(checkin_ts))) AS checkin_hour,\n"
            "  COUNT(*) AS checkin_count\n"
            "FROM checkin\n"
            "LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "WHERE date IS NOT NULL\n"
            "  AND TRIM(checkin_ts) <> ''\n"
            "GROUP BY HOUR(TO_TIMESTAMP(TRIM(checkin_ts)))\n"
            "ORDER BY checkin_hour ASC\n"
            "LIMIT 24"
        ),
        explanation=(
            "This explodes the check-in timestamps and extracts the hour of day from each one. "
            "It shows how check-ins are distributed across the 24-hour clock."
        ),
    ),
    GoldenQueryTemplate(
        key="most_popular_city_for_checkins",
        user_intent="Identify the most popular city for check-ins",
        sql=(
            "SELECT\n"
            "  b.city,\n"
            "  COUNT(*) AS checkin_count\n"
            "FROM checkin c\n"
            "JOIN business b\n"
            "  ON c.business_id = b.business_id\n"
            "LATERAL VIEW EXPLODE(SPLIT(c.date, ',')) exploded_checkins AS checkin_ts\n"
            "WHERE c.date IS NOT NULL\n"
            "  AND TRIM(checkin_ts) <> ''\n"
            "  AND b.city IS NOT NULL\n"
            "GROUP BY b.city\n"
            "ORDER BY checkin_count DESC, b.city ASC\n"
            "LIMIT 1"
        ),
        explanation=(
            "This joins exploded check-ins to business cities and counts how many happened in each city. "
            "It returns the single city with the most check-ins."
        ),
    ),
    GoldenQueryTemplate(
        key="business_ranked_by_checkins",
        user_intent="Rank all businesses based on check-in counts",
        sql=(
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  COUNT(*) AS checkin_count\n"
            "FROM checkin c\n"
            "JOIN business b\n"
            "  ON c.business_id = b.business_id\n"
            "LATERAL VIEW EXPLODE(SPLIT(c.date, ',')) exploded_checkins AS checkin_ts\n"
            "WHERE c.date IS NOT NULL\n"
            "  AND TRIM(checkin_ts) <> ''\n"
            "GROUP BY b.business_id, b.name, b.city, b.state\n"
            "ORDER BY checkin_count DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This counts total exploded check-ins for every business. "
            "It ranks businesses from the busiest check-in locations down."
        ),
    ),
    GoldenQueryTemplate(
        key="mom_checkin_growth_top_restaurants",
        user_intent="Calculate the month-over-month (MoM) check-in growth rate for the top 50 restaurants",
        sql=(
            "WITH restaurant_checkins AS (\n"
            "  SELECT\n"
            "    b.business_id,\n"
            "    b.name,\n"
            "    DATE_TRUNC('month', TO_TIMESTAMP(TRIM(checkin_ts))) AS activity_month\n"
            "  FROM checkin c\n"
            "  JOIN business b\n"
            "    ON c.business_id = b.business_id\n"
            "  LATERAL VIEW EXPLODE(SPLIT(c.date, ',')) exploded_checkins AS checkin_ts\n"
            "  WHERE c.date IS NOT NULL\n"
            "    AND TRIM(checkin_ts) <> ''\n"
            "    AND array_contains(b.categories, 'Restaurants')\n"
            "),\n"
            "top_restaurants AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    name,\n"
            "    COUNT(*) AS total_checkins\n"
            "  FROM restaurant_checkins\n"
            "  GROUP BY business_id, name\n"
            "  ORDER BY total_checkins DESC, name ASC\n"
            "  LIMIT 50\n"
            "),\n"
            "monthly_checkins AS (\n"
            "  SELECT\n"
            "    rc.business_id,\n"
            "    tr.name,\n"
            "    rc.activity_month,\n"
            "    COUNT(*) AS monthly_checkin_count\n"
            "  FROM restaurant_checkins rc\n"
            "  JOIN top_restaurants tr\n"
            "    ON rc.business_id = tr.business_id\n"
            "  GROUP BY rc.business_id, tr.name, rc.activity_month\n"
            ")\n"
            "SELECT\n"
            "  business_id,\n"
            "  name,\n"
            "  activity_month,\n"
            "  monthly_checkin_count,\n"
            "  LAG(monthly_checkin_count) OVER (PARTITION BY business_id ORDER BY activity_month) AS previous_month_checkins,\n"
            "  CASE\n"
            "    WHEN LAG(monthly_checkin_count) OVER (PARTITION BY business_id ORDER BY activity_month) IS NULL THEN NULL\n"
            "    WHEN LAG(monthly_checkin_count) OVER (PARTITION BY business_id ORDER BY activity_month) = 0 THEN NULL\n"
            "    ELSE ((monthly_checkin_count - LAG(monthly_checkin_count) OVER (PARTITION BY business_id ORDER BY activity_month)) * 100.0)\n"
            "         / LAG(monthly_checkin_count) OVER (PARTITION BY business_id ORDER BY activity_month)\n"
            "  END AS mom_checkin_growth_rate\n"
            "FROM monthly_checkins\n"
            "ORDER BY name ASC, activity_month ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This keeps the 50 restaurants with the highest total check-ins, then tracks their check-ins month by month. "
            "It compares each month to the previous one to calculate MoM growth."
        ),
    ),
    GoldenQueryTemplate(
        key="review_seasonality_by_cuisine",
        user_intent="Analyze review seasonality by cuisine (which month has highest volume for seasonal categories like Ice Cream vs. Soup)",
        sql=(
            "WITH seasonal_cuisine_reviews AS (\n"
            "  SELECT\n"
            "    exploded_category AS cuisine_category,\n"
            "    MONTH(TO_DATE(r.date)) AS review_month\n"
            "  FROM rating r\n"
            "  JOIN business b\n"
            "    ON r.business_id = b.business_id\n"
            "  LATERAL VIEW EXPLODE(b.categories) exploded_categories AS exploded_category\n"
            "  WHERE r.date IS NOT NULL\n"
            "    AND exploded_category IN ('Ice Cream', 'Soup')\n"
            "),\n"
            "monthly_cuisine_volume AS (\n"
            "  SELECT\n"
            "    cuisine_category,\n"
            "    review_month,\n"
            "    COUNT(*) AS review_volume\n"
            "  FROM seasonal_cuisine_reviews\n"
            "  GROUP BY cuisine_category, review_month\n"
            "),\n"
            "ranked_months AS (\n"
            "  SELECT\n"
            "    cuisine_category,\n"
            "    review_month,\n"
            "    review_volume,\n"
            "    DENSE_RANK() OVER (\n"
            "      PARTITION BY cuisine_category\n"
            "      ORDER BY review_volume DESC, review_month ASC\n"
            "    ) AS month_rank\n"
            "  FROM monthly_cuisine_volume\n"
            ")\n"
            "SELECT\n"
            "  cuisine_category,\n"
            "  review_month,\n"
            "  review_volume\n"
            "FROM ranked_months\n"
            "WHERE month_rank = 1\n"
            "ORDER BY cuisine_category ASC\n"
            "LIMIT 10"
        ),
        explanation=(
            "This compares review volume by month for the selected seasonal categories. "
            "It returns the strongest month for each cuisine category."
        ),
    ),
    GoldenQueryTemplate(
        key="top_merchants_combined_metrics",
        user_intent="Identify the top 5 merchants based on combined metrics of rating frequency, average rating, and check-in frequency",
        sql=(
            "WITH rating_metrics AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    COUNT(*) AS rating_frequency,\n"
            "    AVG(CAST(stars AS DOUBLE)) AS average_rating\n"
            "  FROM rating\n"
            "  GROUP BY business_id\n"
            "),\n"
            "checkin_metrics AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    COUNT(*) AS checkin_frequency\n"
            "  FROM checkin\n"
            "  LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "  WHERE date IS NOT NULL\n"
            "    AND TRIM(checkin_ts) <> ''\n"
            "  GROUP BY business_id\n"
            "),\n"
            "combined_metrics AS (\n"
            "  SELECT\n"
            "    b.business_id,\n"
            "    b.name,\n"
            "    b.city,\n"
            "    b.state,\n"
            "    COALESCE(r.rating_frequency, 0) AS rating_frequency,\n"
            "    COALESCE(r.average_rating, 0.0) AS average_rating,\n"
            "    COALESCE(c.checkin_frequency, 0) AS checkin_frequency\n"
            "  FROM business b\n"
            "  LEFT JOIN rating_metrics r ON b.business_id = r.business_id\n"
            "  LEFT JOIN checkin_metrics c ON b.business_id = c.business_id\n"
            "),\n"
            "ranked_metrics AS (\n"
            "  SELECT\n"
            "    *,\n"
            "    DENSE_RANK() OVER (ORDER BY rating_frequency DESC, name ASC) AS rating_frequency_rank,\n"
            "    DENSE_RANK() OVER (ORDER BY average_rating DESC, name ASC) AS average_rating_rank,\n"
            "    DENSE_RANK() OVER (ORDER BY checkin_frequency DESC, name ASC) AS checkin_frequency_rank\n"
            "  FROM combined_metrics\n"
            ")\n"
            "SELECT\n"
            "  business_id,\n"
            "  name,\n"
            "  city,\n"
            "  state,\n"
            "  rating_frequency,\n"
            "  average_rating,\n"
            "  checkin_frequency,\n"
            "  rating_frequency_rank + average_rating_rank + checkin_frequency_rank AS combined_rank_score\n"
            "FROM ranked_metrics\n"
            "ORDER BY combined_rank_score ASC, average_rating DESC, name ASC\n"
            "LIMIT 5"
        ),
        explanation=(
            "This combines review volume, average rating, and total check-ins into one merchant scorecard. "
            "It ranks merchants using the sum of their component metric ranks."
        ),
    ),
    GoldenQueryTemplate(
        key="review_conversion_rate_top_checkedin_businesses",
        user_intent="Calculate the review conversion rate: ratio of total check-ins to total reviews for the top 100 checked-in businesses",
        sql=(
            "WITH checkin_counts AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    COUNT(*) AS total_checkins\n"
            "  FROM checkin\n"
            "  LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "  WHERE date IS NOT NULL\n"
            "    AND TRIM(checkin_ts) <> ''\n"
            "  GROUP BY business_id\n"
            "),\n"
            "review_counts AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    COUNT(*) AS total_reviews\n"
            "  FROM rating\n"
            "  GROUP BY business_id\n"
            "),\n"
            "top_checkedin_businesses AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    total_checkins\n"
            "  FROM checkin_counts\n"
            "  ORDER BY total_checkins DESC\n"
            "  LIMIT 100\n"
            ")\n"
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  t.total_checkins,\n"
            "  COALESCE(r.total_reviews, 0) AS total_reviews,\n"
            "  CASE\n"
            "    WHEN COALESCE(r.total_reviews, 0) = 0 THEN NULL\n"
            "    ELSE CAST(t.total_checkins AS DOUBLE) / CAST(r.total_reviews AS DOUBLE)\n"
            "  END AS checkin_to_review_ratio\n"
            "FROM top_checkedin_businesses t\n"
            "JOIN business b\n"
            "  ON t.business_id = b.business_id\n"
            "LEFT JOIN review_counts r\n"
            "  ON t.business_id = r.business_id\n"
            "ORDER BY t.total_checkins DESC, checkin_to_review_ratio DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This takes the 100 businesses with the most check-ins and compares their check-in volume to their review volume. "
            "It measures how many check-ins happen for each review."
        ),
    ),
    GoldenQueryTemplate(
        key="post_review_checkin_dropoff",
        user_intent="Analyze post-review check-in drop-off: percentage drop in check-ins following a sudden spike in 1-star reviews",
        sql=(
            "WITH monthly_one_star_reviews AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    DATE_TRUNC('month', TO_DATE(date)) AS activity_month,\n"
            "    COUNT(*) AS one_star_review_count\n"
            "  FROM rating\n"
            "  WHERE date IS NOT NULL\n"
            "    AND stars = 1\n"
            "  GROUP BY business_id, DATE_TRUNC('month', TO_DATE(date))\n"
            "),\n"
            "monthly_checkins AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    DATE_TRUNC('month', TO_TIMESTAMP(TRIM(checkin_ts))) AS activity_month,\n"
            "    COUNT(*) AS checkin_count\n"
            "  FROM checkin\n"
            "  LATERAL VIEW EXPLODE(SPLIT(date, ',')) exploded_checkins AS checkin_ts\n"
            "  WHERE date IS NOT NULL\n"
            "    AND TRIM(checkin_ts) <> ''\n"
            "  GROUP BY business_id, DATE_TRUNC('month', TO_TIMESTAMP(TRIM(checkin_ts)))\n"
            "),\n"
            "review_spikes AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    activity_month,\n"
            "    one_star_review_count,\n"
            "    LAG(one_star_review_count) OVER (PARTITION BY business_id ORDER BY activity_month) AS previous_one_star_review_count\n"
            "  FROM monthly_one_star_reviews\n"
            "),\n"
            "spike_months AS (\n"
            "  SELECT\n"
            "    business_id,\n"
            "    activity_month,\n"
            "    one_star_review_count,\n"
            "    previous_one_star_review_count\n"
            "  FROM review_spikes\n"
            "  WHERE one_star_review_count >= 5\n"
            "    AND one_star_review_count > COALESCE(previous_one_star_review_count, 0)\n"
            "),\n"
            "dropoff_metrics AS (\n"
            "  SELECT\n"
            "    s.business_id,\n"
            "    s.activity_month AS spike_month,\n"
            "    current_month.checkin_count AS spike_month_checkins,\n"
            "    next_month.checkin_count AS next_month_checkins,\n"
            "    CASE\n"
            "      WHEN current_month.checkin_count IS NULL OR current_month.checkin_count = 0 THEN NULL\n"
            "      ELSE ((current_month.checkin_count - COALESCE(next_month.checkin_count, 0)) * 100.0)\n"
            "           / current_month.checkin_count\n"
            "    END AS checkin_drop_percent\n"
            "  FROM spike_months s\n"
            "  LEFT JOIN monthly_checkins current_month\n"
            "    ON s.business_id = current_month.business_id\n"
            "   AND s.activity_month = current_month.activity_month\n"
            "  LEFT JOIN monthly_checkins next_month\n"
            "    ON s.business_id = next_month.business_id\n"
            "   AND next_month.activity_month = ADD_MONTHS(s.activity_month, 1)\n"
            ")\n"
            "SELECT\n"
            "  b.business_id,\n"
            "  b.name,\n"
            "  b.city,\n"
            "  b.state,\n"
            "  d.spike_month,\n"
            "  d.spike_month_checkins,\n"
            "  d.next_month_checkins,\n"
            "  d.checkin_drop_percent\n"
            "FROM dropoff_metrics d\n"
            "JOIN business b\n"
            "  ON d.business_id = b.business_id\n"
            "WHERE d.checkin_drop_percent IS NOT NULL\n"
            "ORDER BY d.checkin_drop_percent DESC, d.spike_month DESC, b.name ASC\n"
            "LIMIT 100"
        ),
        explanation=(
            "This finds months where 1-star reviews suddenly spike, then compares check-ins in that month to the following month. "
            "It measures how sharply check-in activity drops after the negative-review surge."
        ),
    ),
)


def get_golden_query_templates() -> tuple[GoldenQueryTemplate, ...]:
    """Return the canonical Spark-friendly Yelp query templates."""
    return _GOLDEN_QUERY_TEMPLATES


def _get_sql_dialect_label() -> str:
    """Return a simple dialect hint based on the configured SQL engine."""
    ensure_environment_loaded()
    engine = os.getenv("YELP_SQL_ENGINE", "").strip().lower()

    if engine == "hive":
        return "Hive SQL"

    if engine == "spark":
        return "Spark SQL"

    return "SQL compatible with your configured database"


def _build_business_glossary_text() -> str:
    """Return Yelp-specific vocabulary and field interpretation notes."""
    glossary_items = [
        "merchant, business, storefront, and restaurant usually map to rows in the business table",
        "business-level ratings use business.stars, while review-level ratings use rating.stars",
        "business.review_count is received reviews for one business, while users.review_count is written reviews for one user",
        "the review table is named rating in this backend, so review-level questions usually map to rating",
        "popular users usually map to users.fans, and elite-user questions usually map to users.elite",
        "review trends and yearly review metrics usually come from rating.date",
        "user-join or cohort questions usually come from users.yelping_since",
        "business.postal_code is the postal or ZIP code column for a business",
        "cuisine or restaurant-type questions usually rely on business.categories, which is stored here as an array of strings and often implies the Restaurants category",
        "business.attributes and business.hours are object-like fields and should not be flattened by guesswork",
        "check-in questions should use the checkin table, but checkin.date is stored as a comma-separated list of timestamps rather than one normalized time column",
    ]
    return "\n".join(f"- {item}" for item in glossary_items)


def _build_analysis_playbook_text() -> str:
    """Return PDF-derived question families that improve SQL grounding."""
    playbook_items = [
        "business analysis: most common merchants, top cities or states by merchant count, average ratings by merchant, top categories, top businesses by five-star reviews",
        "user analysis: users joining each year, top reviewers by review_count, popular users by fans, elite-user exploration, and compliment signals when needed",
        "review analysis: reviews per year, useful/funny/cool totals, word-frequency or pain-point analysis when rating.text is available",
        "rating analysis: rating distributions, top cities by rating, business versus cuisine rating comparisons",
        "check-in analysis: top cities or businesses by check-in volume, while deeper time analysis may require parsing the raw checkin.date string first",
        "comprehensive analysis: combine business ratings, review activity, and check-in frequency only when the required fields are present",
    ]
    return "\n".join(f"- {item}" for item in playbook_items)


def _build_scope_guardrails_text() -> str:
    """Return extra guardrails for PDF tasks that may exceed the live schema."""
    guardrail_items = [
        "The course brief includes advanced NLP tasks that require review text processing beyond plain aggregation. Only generate direct SQL when the request can be answered from the available tables and fields.",
        "The course brief also includes data-enrichment tasks using weather, census, transit, or other external datasets. Do not invent external tables or join paths unless the user has explicitly loaded them into the backend.",
        "When a question mentions cuisines or categories, respect the real storage format of business.categories as an array in the live backend instead of guessing unsupported string functions.",
        "When a question refers to reviews, remember that the underlying table name is rating.",
    ]
    return "\n".join(f"- {item}" for item in guardrail_items)


def _build_dataset_quirks_text() -> str:
    """Return hard dataset-specific SQL rules for the verified Yelp tables."""
    quirk_items = [
        "The business.categories field is an array of strings. When filtering by category, use an array membership check such as array_contains(business.categories, 'Pizza') and do not invent or join to a separate category table.",
        "If the user asks for the best, top-rated, or highest-rated businesses, consider both business.stars and business.review_count. Add business.review_count > 50 before ranking by stars unless the user explicitly asks for a tiny or brand-new sample.",
        "business.is_open uses 1 for open and 0 for closed.",
        "Use COUNT(*) when counting rows such as reviews or businesses. Use COUNT(DISTINCT some_id) only when the user is explicitly asking for unique entities.",
        "The business postal-code column is named postal_code with an underscore.",
        "The review table is named rating, not review.",
        "When searching for a business name like Starbucks, use a LIKE filter on business.name rather than assuming exact equality.",
    ]
    return "\n".join(f"- {item}" for item in quirk_items)


def _build_project_definitions_text() -> str:
    """Return strict business definitions the model must obey for this project."""
    us_state_list = ", ".join(f"'{state_code}'" for state_code in _US_STATE_CODES)
    cuisine_list = ", ".join(_CUISINE_CATEGORIES)
    definition_items = [
        (
            "Merchant Location (U.S. only): when the user explicitly asks about cities, states, or merchants in the U.S., "
            f"filter business rows with WHERE state IN ({us_state_list}). Do not assume the whole database is U.S.-only."
        ),
        (
            "Restaurant categories: the parent restaurant category is exactly 'Restaurants'. "
            f"The strict cuisine categories are {cuisine_list}. "
            "If the user asks to count or compare restaurant types, keep only businesses that have BOTH 'Restaurants' and at least one of those cuisine categories in business.categories."
        ),
        (
            "Turnaround Merchants definition: use the rating table, split dates into date >= DATE_SUB(CURRENT_DATE(), 365) versus older rows, "
            "compute recent and historical AVG(stars) per business_id, and keep businesses where recent_avg - historical_avg >= 1.0."
        ),
        (
            "Category Synergy definition: explode the business.categories array, self-join the exploded categories for the same business_id, keep only cat1 < cat2 to avoid duplicates, then GROUP BY cat1 and cat2, ORDER BY the co-occurrence count DESC, and LIMIT 10."
        ),
        (
            "Polarizing Businesses definition: compute STDDEV(stars) per business_id on the rating table, keep only businesses with COUNT(stars) > 50, then order by the rating standard deviation DESC."
        ),
        (
            "Tips data note: the current live schema does not include a tips table. If a multi-metric yearly request asks for tips, keep the query schema-safe instead of inventing a missing source."
        ),
        (
            "Basic business analysis rules: 'most common merchants' means GROUP BY business.name and ORDER BY COUNT(*) DESC; "
            "'states or cities with most merchants' means GROUP BY state or city and ORDER BY COUNT(*) DESC; "
            "'merchants with most five-star reviews' means JOIN business with rating, filter rating.stars = 5, GROUP BY the business, count the matching ratings, and sort descending."
        ),
    ]
    return "\n".join(f"- {item}" for item in definition_items)


def _build_few_shot_examples_text() -> str:
    """Return compact high-value few-shot examples for SQL generation."""
    examples = [
        (
            'User: "What are the top 5 pizza places in Las Vegas?"',
            "SQL:\n"
            "SELECT name, stars, review_count, address\n"
            "FROM business\n"
            "WHERE city = 'Las Vegas' AND array_contains(categories, 'Pizza') AND review_count > 50\n"
            "ORDER BY stars DESC, review_count DESC\n"
            "LIMIT 5;",
        ),
        (
            'User: "How many users have given more than 100 reviews?"',
            "SQL:\n"
            "SELECT COUNT(*) AS high_review_users\n"
            "FROM users\n"
            "WHERE review_count > 100;",
        ),
        (
            'User: "Show me the 10 most recent reviews for Starbucks."',
            "SQL:\n"
            "SELECT r.date, r.stars, r.text\n"
            "FROM rating r\n"
            "JOIN business b ON r.business_id = b.business_id\n"
            "WHERE b.name LIKE '%Starbucks%'\n"
            "ORDER BY r.date DESC\n"
            "LIMIT 10;",
        ),
    ]
    return "\n\n".join(f"{question}\n{sql}" for question, sql in examples)


def _build_golden_query_cheat_sheet_text() -> str:
    """Return the strict golden Spark SQL templates for known assignment intents."""
    sections = [
        "If the user's question matches or is heavily related to one of the intents below, return the exact SQL block for that example with no changes.",
    ]

    for index, template in enumerate(get_golden_query_templates(), start=1):
        sections.append(
            "\n".join(
                [
                    f"[EXAMPLE {index}]",
                    f'USER INTENT: "{template.user_intent}"',
                    "GOLDEN CODE:",
                    template.sql,
                ]
            )
        )

    return "\n\n".join(sections)


_CORE_YELP_SCHEMA: dict[str, tuple[str, ...]] = {
    "business": (
        "business_id",
        "name",
        "address",
        "city",
        "state",
        "postal_code",
        "latitude",
        "longitude",
        "stars",
        "review_count",
        "is_open",
        "attributes",
        "categories",
        "hours",
    ),
    "rating": (
        "review_id",
        "user_id",
        "business_id",
        "stars",
        "date",
        "text",
        "useful",
        "funny",
        "cool",
    ),
    "users": (
        "user_id",
        "name",
        "review_count",
        "yelping_since",
        "friends",
        "useful",
        "funny",
        "cool",
        "fans",
        "elite",
        "average_stars",
    ),
    "checkin": (
        "business_id",
        "date",
    ),
}


def build_schema_prompt_text() -> str:
    """Build the exact core Yelp schema block required by the SQL prompt."""
    sections: list[str] = []
    for table_name, columns in _CORE_YELP_SCHEMA.items():
        sections.append(f"TABLE: {table_name} ({', '.join(columns)})")
    return "\n".join(sections)


def build_system_prompt() -> str:
    """Build the strict core Text-to-SQL system prompt used on every LLM call."""
    config = load_config()
    sql_dialect = _get_sql_dialect_label()
    schema_prompt_text = build_schema_prompt_text()
    sample_value_hints = get_sample_value_hints()
    business_glossary = _build_business_glossary_text()
    analysis_playbook = _build_analysis_playbook_text()
    scope_guardrails = _build_scope_guardrails_text()
    dataset_quirks = _build_dataset_quirks_text()
    project_definitions = _build_project_definitions_text()
    few_shot_examples = _build_few_shot_examples_text()
    golden_query_cheat_sheet = _build_golden_query_cheat_sheet_text()

    prompt_sections = [
        "You are an elite PySpark and Spark SQL Data Engineer.",
        "Your only job is to translate the user's question into one correct executable SQL query.",
        f"Write exactly one {sql_dialect} query.",
        "Full Yelp schema:",
        schema_prompt_text,
        "Strict output rules:",
        "Output ONLY raw executable SQL. No markdown, no backtick wrappers, no explanation text.",
        "ALWAYS include a LIMIT clause (default LIMIT 20) unless the user explicitly asks for all results.",
        "The review table is named rating, NOT review. Never use FROM review.",
        "Filter early with WHERE before applying GROUP BY or JOIN.",
        "Use exact table names and exact column names from the provided schema.",
        "Do not invent tables, columns, join keys, filters, or business rules.",
        "Never wrap the final answer in JSON.",
        "Critical dataset quirks you must obey:",
        dataset_quirks,
        "Critical project definitions you must memorize:",
        project_definitions,
        "Performance rules are mandatory:",
        "- Default to LIMIT 20 for ordinary result sets.",
        "- Filter early with WHERE clauses before large JOIN, GROUP BY, or ORDER BY operations whenever the question provides a filter.",
        "- Never use SELECT * inside subqueries or CTEs; project only the columns you actually need.",
        "- Prefer the smallest necessary column set in every SELECT.",
        "- Avoid unnecessary joins when one table can answer the question directly.",
        "- If the user asks for rankings such as top/bottom, include an ORDER BY plus a tight LIMIT.",
        "Golden Query Cheat Sheet:",
        golden_query_cheat_sheet,
        "Business glossary and dataset semantics:",
        business_glossary,
        "Common question families from the project brief:",
        analysis_playbook,
        "Scope guardrails:",
        scope_guardrails,
        "Few-shot training examples:",
        few_shot_examples,
        f"Value hints:\n{sample_value_hints or 'No sample value hints have been added yet.'}",
    ]

    return "\n\n".join(prompt_sections).strip()


def build_text_to_sql_system_prompt() -> str:
    """Backward-compatible alias for the core SQL system prompt."""
    return build_system_prompt()


def build_user_question_prompt(question: str) -> str:
    """Build a simple user prompt for the text-to-SQL request."""
    clean_question = question.strip()
    return f"Generate SQL for this question:\n{clean_question}"


def build_prompt_bundle(question: str) -> PromptBundle:
    """Build a schema-aware prompt bundle for text-to-SQL generation."""
    system_prompt = build_system_prompt()
    schema_loaded = True

    return PromptBundle(
        system_prompt=system_prompt,
        user_prompt=build_user_question_prompt(question),
        schema_loaded=schema_loaded,
    )


# Advanced Research Lab prompts - stub implementations
WEATHER_MOOD_HYPOTHESIS = "Explore the correlation between business categories and customer sentiment across seasons"
CURSED_STOREFRONTS_ANALYSIS = "Identify businesses with unusually low ratings compared to their review volume"
REVIEW_MANIPULATION_SYNDICATE = "Detect patterns that might indicate review manipulation or fake reviews"
OPEN_WORLD_DATA_SAFARI = "Discover unexpected insights and correlations in the Yelp dataset"
PRESENTATION_GOLDEN_PROMPTS = (
    "Show the first 5 businesses",
    "Count the number of reviews",
    "Count the number of reviews per year",
    "Show the top 10 cities by number of businesses",
    "Show the top 10 users by review count",
)

# Query hint constants for chat mode detection
DATA_QUERY_HINTS = (
    "sql", "query", "table", "data", "count", "average", "sum", "max", "min",
    "group", "where", "filter", "sort", "order", "join", "business", "review",
    "user", "restaurant", "count", "aggregate", "statistics"
)

GENERAL_CHAT_HINTS = (
    "hello", "hi", "how", "what", "why", "where", "who", "explain", "tell me",
    "about", "information", "help", "thanks", "please", "question"
)

# Prompt chip details - mapping of prompt names to (meta text, description)
PROMPT_CHIP_DETAILS = {
    "The Weather-Mood Hypothesis": ("Research", "Investigate seasonal sentiment patterns"),
    "The Cursed Storefronts Analysis": ("Research", "Detect rating anomalies"),
    "The Review Manipulation Syndicate": ("Research", "Find suspicious review patterns"),
    "Open-World Data Safari": ("Exploration", "Discover unexpected insights"),
}
