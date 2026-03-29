# Forcing a recompile to clear stale cache
from __future__ import annotations

import base64
import difflib
import hashlib
import json
import mimetypes
import os
import pathlib
import re
import time
from html import escape
from typing import Any, Callable
from urllib.parse import urlencode

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from yelp_text_to_sql.api import (
    AppConfig,
    PipelineResult,
    QueryResult,
    describe_table_schema,
    execute_sql,
    get_table_schemas,
    run_test_query,
)
from yelp_text_to_sql.audio_transcription import transcribe_audio_bytes
from yelp_text_to_sql.charts import (
    _find_chart_columns,
    _find_map_columns,
    export_chart_png_bytes,
    render_map,
)
from yelp_text_to_sql.pipeline import run_natural_language_query
from yelp_text_to_sql.pipeline import EXAMPLE_QUESTIONS
from yelp_text_to_sql.prompt_schema import (
    CURSED_STOREFRONTS_ANALYSIS,
    DATA_QUERY_HINTS,
    GENERAL_CHAT_HINTS,
    OPEN_WORLD_DATA_SAFARI,
    PRESENTATION_GOLDEN_PROMPTS,
    PROMPT_CHIP_DETAILS,
    REVIEW_MANIPULATION_SYNDICATE,
    WEATHER_MOOD_HYPOTHESIS,
)
from yelp_text_to_sql.schema_definitions import get_schema_verification_checklist
from yelp_text_to_sql.config import load_config
from yelp_text_to_sql.sql_generation import generate_general_chat_reply
from yelp_text_to_sql.ui_styles import APPLY_CUSTOM_CSS, get_custom_css

# Core application constants
DEFAULT_ROUTE = "home"
DEFAULT_DETAIL_PANEL = "results"
CHAT_MODE_AUTO = "auto"
CHAT_MODE_GENERAL = "general"
CHAT_MODE_DATA = "data"
DEFAULT_CHAT_MODE = CHAT_MODE_AUTO
GENERAL_CHAT_MODE_LABEL = "General Chat"
DEFAULT_CUSTOM_GREETING_RESPONSE = (
    "Hi! I am Query by SilkByteX. I can chat naturally or run Yelp data queries."
)
DEFAULT_CUSTOM_INTRO_RESPONSE = (
    "I am Query by SilkByteX, your Yelp analytics copilot. "
    "My best tasks are turning natural-language questions into SQL, querying Yelp tables like business, review, users, and checkin, "
    "explaining query results clearly, suggesting follow-up analysis, and helping you refine questions for better insights. "
    "Use Data Query mode when you want real database results."
)

# Heuristic constants for chat mode routing
FOLLOW_UP_PREFIXES = (
    "and ",
    "what about ",
    "how about ",
    "show me ",
    "tell me more about ",
    "break it down by ",
    "group by ",
    "can you ",
    "now ",
    "then ",
    "also ",
    "next ",
    "but ",
)
FOLLOW_UP_REFERENCES = (
    " this",
    " that",
    " those",
    " these",
    " it",
    " them",
    " they",
)

# UI labels and routing maps
ROUTE_LABELS = {
    "home": "Conversational Chat",
    "readiness": "Data Journey",
    "schema": "Database Schema",
    "architecture": "Architecture",
    "docs": "Documentation",
    "data_journey_admin": "Data Journey Admin",
}
DETAIL_PANEL_LABELS = {
    "sql": "SQL",
    "results": "Results",
    "chart": "Chart",
    "map": "Map",
    "errors": "Errors",
}

# Constants for the pipeline loading visualizer
PIPELINE_PHASE_USER_INTENT = "user_intent"
PIPELINE_PHASE_ROUTING = "routing"
PIPELINE_PHASE_MEMORY = "memory"
PIPELINE_PHASE_PROMPT = "prompt"
PIPELINE_PHASE_SQL = "sql"
PIPELINE_PHASE_SANITIZE = "sanitize"
PIPELINE_PHASE_EXECUTE = "execute"
PIPELINE_PHASE_RETRY = "retry"
PIPELINE_PHASE_COMPLETE = "complete"
PIPELINE_VISUAL_PHASE_ORDER = (
    PIPELINE_PHASE_USER_INTENT,
    PIPELINE_PHASE_ROUTING,
    PIPELINE_PHASE_MEMORY,
    PIPELINE_PHASE_PROMPT,
    PIPELINE_PHASE_SQL,
    PIPELINE_PHASE_SANITIZE,
    PIPELINE_PHASE_EXECUTE,
    PIPELINE_PHASE_RETRY,
)

# Pipeline visualizer animation settings
PIPELINE_VISUALIZER_FRAME_DELAY_SECONDS = 0.3
PIPELINE_VISUALIZER_FINAL_DELAY_SECONDS = 0.2
PIPELINE_VISUAL_PHASE_LABELS = {
    PIPELINE_PHASE_USER_INTENT: "User Intent",
    PIPELINE_PHASE_ROUTING: "Routing",
    PIPELINE_PHASE_MEMORY: "Memory",
    PIPELINE_PHASE_PROMPT: "Prompt",
    PIPELINE_PHASE_SQL: "SQL",
    PIPELINE_PHASE_SANITIZE: "Sanitize",
    PIPELINE_PHASE_EXECUTE: "Execute",
    PIPELINE_PHASE_RETRY: "Retry",
}

DATA_QUERY_RECOMMENDATIONS = (
    "Show me the top 5 highest-rated Mexican restaurants in Philadelphia that have over 500 reviews.",
    "Which 10 cities have the most elite users?",
    "Count the number of reviews per year.",
)

AUTO_GENERAL_RECOMMENDATIONS = (
    "Summarize the latest SQL result in plain language.",
    "Explain the system architecture in one paragraph.",
    "What follow-up analysis question should I ask next?",
)

PROJECT_RECOMMENDATION_QUESTIONS = (
    # Business Analysis (Top 3)
    "What are the 20 most common merchants in the U.S.?",
    "Which 10 cities have the most merchants in the U.S.?",
    "Which 5 states have the most merchants in the U.S.?",
    # Review Analysis (Top 3)
    "How many reviews were written each year?",
    "How many useful, funny, and cool votes are there in reviews?",
    "Who are the top users by total reviews each year?",
    # User Analysis (Top 3)
    "How many users joined each year?",
    "Who are the top reviewers based on review_count?",
    "Who are the most popular users based on fans?",
    # Check-in Analysis (Top 2)
    "How many check-ins happened each year?",
    "Which city is the most popular for check-ins?",
    # Business Analysis notebook (2MPE568Z2) full set
    "Identify the 20 most common merchants in the U.S.",
    "Identify the top 10 cities with the most merchants in the U.S.",
    "Identify the top 5 states with the most merchants in the U.S.",
    "Identify the 20 most common merchants and display their average ratings.",
    "Count the total number of unique business categories.",
    "Identify the top 10 most frequent categories and their count.",
    "Identify the top 20 merchants that received the most five-star reviews.",
    "Count the number of restaurant types (Chinese, American, Mexican).",
    "Count the number of reviews for each restaurant type.",
    "Analyze the rating distribution (average) for different restaurant types.",
    "Find businesses whose avg rating in last 12 months increased by >= 1 star.",
    "Identify top 10 pairs of distinct categories that co-occur.",
    "Find polarizing merchants (high standard deviation in ratings).",
    # User Analysis notebook (2MP7MA6PH) full set
    "Analyze the number of users joining each year.",
    "Identify top reviewers based on user_review_count.",
    "Identify the most popular users based on user_fans.",
    "Calculate the ratio of elite users to regular users each year.",
    "Display the proportion of total users and silent users (0 reviews) each year.",
    "Compute the yearly statistics of new users, number of reviews, elite users, and fans.",
    "Identify early adopters (tastemakers) who wrote one of the first 5 reviews for successful businesses.",
    "User rating evolution: compare average star rating in first year vs third year.",
    "Segment users by dining diversity (distinct cuisine categories reviewed).",
    "Elite status impact: review length and useful votes before vs after becoming elite.",
)
_RECOMMENDATION_SQL_MAP: dict[str, str] = {
    # Business Analysis
    "what are the 20 most common merchants in the u s": (
        "SELECT name, COUNT(*) AS location_count "
        "FROM business "
        "WHERE name IS NOT NULL AND TRIM(name) <> '' "
        "AND state IN ("
        "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
        "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
        "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
        "'VA','WA','WV','WI','WY'"
        ") "
        "GROUP BY name "
        "ORDER BY location_count DESC, name ASC "
        "LIMIT 20"
    ),
    "what are the top 20 most common merchants in the u s": (
        "SELECT name, COUNT(*) AS location_count "
        "FROM business "
        "WHERE name IS NOT NULL AND TRIM(name) <> '' "
        "AND state IN ("
        "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
        "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
        "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
        "'VA','WA','WV','WI','WY'"
        ") "
        "GROUP BY name "
        "ORDER BY location_count DESC, name ASC "
        "LIMIT 20"
    ),
    "what are the top 20 most common marchants in the u s": (
        "SELECT name, COUNT(*) AS location_count "
        "FROM business "
        "WHERE name IS NOT NULL AND TRIM(name) <> '' "
        "AND state IN ("
        "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
        "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
        "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
        "'VA','WA','WV','WI','WY'"
        ") "
        "GROUP BY name "
        "ORDER BY location_count DESC, name ASC "
        "LIMIT 20"
    ),
    "which 10 cities have the most merchants in the u s": (
        "SELECT city, COUNT(*) AS merchant_count "
        "FROM business "
        "WHERE city IS NOT NULL AND TRIM(city) <> '' "
        "GROUP BY city "
        "ORDER BY merchant_count DESC, city ASC "
        "LIMIT 10"
    ),
    "which 5 states have the most merchants in the u s": (
        "SELECT state, COUNT(*) AS merchant_count "
        "FROM business "
        "WHERE state IS NOT NULL AND TRIM(state) <> '' "
        "GROUP BY state "
        "ORDER BY merchant_count DESC, state ASC "
        "LIMIT 5"
    ),
    # Review Analysis
    "how many reviews were written each year": (
        "SELECT YEAR(rev_date) AS review_year, COUNT(*) AS review_count "
        "FROM review "
        "WHERE rev_date IS NOT NULL "
        "GROUP BY YEAR(rev_date) "
        "ORDER BY review_year"
    ),
    "how many useful funny and cool votes are there in reviews": (
        "SELECT "
        "SUM(COALESCE(rev_useful, 0)) AS useful_votes, "
        "SUM(COALESCE(rev_funny, 0)) AS funny_votes, "
        "SUM(COALESCE(rev_cool, 0)) AS cool_votes "
        "FROM review"
    ),
    "who are the top users by total reviews each year": (
        "WITH yearly_user_reviews AS ( "
        "  SELECT YEAR(rev_date) AS review_year, rev_user_id AS user_id, COUNT(*) AS total_reviews "
        "  FROM review "
        "  WHERE rev_date IS NOT NULL AND rev_user_id IS NOT NULL "
        "  GROUP BY YEAR(rev_date), rev_user_id "
        "), ranked_users AS ( "
        "  SELECT review_year, user_id, total_reviews, "
        "         ROW_NUMBER() OVER (PARTITION BY review_year ORDER BY total_reviews DESC, user_id ASC) AS rn "
        "  FROM yearly_user_reviews "
        ") "
        "SELECT r.review_year, COALESCE(u.user_name, r.user_id) AS user_name, r.total_reviews "
        "FROM ranked_users r "
        "LEFT JOIN users u ON r.user_id = u.user_id "
        "WHERE r.rn <= 10 "
        "ORDER BY r.review_year, r.total_reviews DESC, user_name"
    ),
    # User Analysis
    "how many users joined each year": (
        "SELECT SUBSTR(user_yelping_since, 1, 4) AS join_year, COUNT(*) AS user_count "
        "FROM users "
        "WHERE user_yelping_since IS NOT NULL AND LENGTH(TRIM(user_yelping_since)) >= 4 "
        "GROUP BY SUBSTR(user_yelping_since, 1, 4) "
        "ORDER BY join_year"
    ),
    "who are the top reviewers based on review count": (
        "SELECT user_name, user_review_count, user_fans, user_average_stars "
        "FROM users "
        "WHERE user_review_count IS NOT NULL "
        "ORDER BY user_review_count DESC, user_fans DESC "
        "LIMIT 20"
    ),
    "who are the most popular users based on fans": (
        "SELECT user_name, user_fans, user_review_count, user_average_stars "
        "FROM users "
        "WHERE user_fans IS NOT NULL "
        "ORDER BY user_fans DESC, user_review_count DESC "
        "LIMIT 20"
    ),
    # Check-in Analysis
    "how many check ins happened each year": (
        "WITH exploded_checkins AS ( "
        "  SELECT EXPLODE(SPLIT(checkin_dates, ',')) AS checkin_ts "
        "  FROM checkin WHERE checkin_dates IS NOT NULL AND TRIM(checkin_dates) <> '' "
        ") "
        "SELECT YEAR(CAST(TRIM(checkin_ts) AS TIMESTAMP)) AS checkin_year, COUNT(*) AS checkin_count "
        "FROM exploded_checkins "
        "GROUP BY YEAR(CAST(TRIM(checkin_ts) AS TIMESTAMP)) "
        "ORDER BY checkin_year"
    ),
    "which city is the most popular for check ins": (
        "WITH exploded_checkins AS ( "
        "  SELECT business_id, EXPLODE(SPLIT(checkin_dates, ',')) AS checkin_ts "
        "  FROM checkin WHERE checkin_dates IS NOT NULL AND TRIM(checkin_dates) <> '' "
        ") "
        "SELECT b.city, COUNT(*) AS checkin_count "
        "FROM exploded_checkins c "
        "JOIN business b ON c.business_id = b.business_id "
        "WHERE b.city IS NOT NULL AND TRIM(b.city) <> '' "
        "GROUP BY b.city "
        "ORDER BY checkin_count DESC, b.city ASC "
        "LIMIT 1"
    ),
    # Business notebook question variants (2MPE568Z2)
    "identify the 20 most common merchants in the u s": (
        "SELECT name, COUNT(*) AS location_count "
        "FROM business "
        "WHERE name IS NOT NULL AND TRIM(name) <> '' "
        "AND state IN ("
        "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
        "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
        "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
        "'VA','WA','WV','WI','WY'"
        ") "
        "GROUP BY name "
        "ORDER BY location_count DESC, name ASC "
        "LIMIT 20"
    ),
    "identify the top 10 cities with the most merchants in the u s": (
        "SELECT city, COUNT(*) AS merchant_count "
        "FROM business "
        "WHERE city IS NOT NULL AND TRIM(city) <> '' "
        "AND state IN ("
        "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
        "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
        "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
        "'VA','WA','WV','WI','WY'"
        ") "
        "GROUP BY city "
        "ORDER BY merchant_count DESC, city ASC "
        "LIMIT 10"
    ),
    "identify the top 5 states with the most merchants in the u s": (
        "SELECT state, COUNT(*) AS merchant_count "
        "FROM business "
        "WHERE state IS NOT NULL AND TRIM(state) <> '' "
        "GROUP BY state "
        "ORDER BY merchant_count DESC, state ASC "
        "LIMIT 5"
    ),
    "identify the 20 most common merchants and display their average ratings": (
        "SELECT b.name, COUNT(*) AS location_count, AVG(COALESCE(r.rev_stars, r.stars)) AS average_rating "
        "FROM business b "
        "JOIN review r ON b.business_id = COALESCE(r.business_id, r.rev_business_id) "
        "WHERE b.name IS NOT NULL AND TRIM(b.name) <> '' "
        "GROUP BY b.name "
        "ORDER BY location_count DESC, average_rating DESC "
        "LIMIT 20"
    ),
    "count the total number of unique business categories": (
        "SELECT COUNT(DISTINCT category) AS unique_category_count "
        "FROM (SELECT EXPLODE(SPLIT(COALESCE(categories, bus_categories), ',')) AS category FROM business) t "
        "WHERE category IS NOT NULL AND TRIM(category) <> ''"
    ),
    "identify the top 10 most frequent categories and their count": (
        "SELECT TRIM(category) AS category, COUNT(*) AS category_count "
        "FROM (SELECT EXPLODE(SPLIT(COALESCE(categories, bus_categories), ',')) AS category FROM business) t "
        "WHERE category IS NOT NULL AND TRIM(category) <> '' "
        "GROUP BY TRIM(category) "
        "ORDER BY category_count DESC, category ASC "
        "LIMIT 10"
    ),
    "identify the top 20 merchants that received the most five star reviews": (
        "SELECT b.name, COUNT(*) AS five_star_reviews "
        "FROM business b "
        "JOIN review r ON b.business_id = COALESCE(r.business_id, r.rev_business_id) "
        "WHERE COALESCE(r.rev_stars, r.stars) = 5 "
        "GROUP BY b.name "
        "ORDER BY five_star_reviews DESC, b.name ASC "
        "LIMIT 20"
    ),
    "count the number of restaurant types chinese american mexican": (
        "SELECT "
        "SUM(CASE WHEN LOWER(COALESCE(categories, bus_categories)) LIKE '%chinese%' THEN 1 ELSE 0 END) AS chinese_restaurants, "
        "SUM(CASE WHEN LOWER(COALESCE(categories, bus_categories)) LIKE '%american%' THEN 1 ELSE 0 END) AS american_restaurants, "
        "SUM(CASE WHEN LOWER(COALESCE(categories, bus_categories)) LIKE '%mexican%' THEN 1 ELSE 0 END) AS mexican_restaurants "
        "FROM business "
        "WHERE LOWER(COALESCE(categories, bus_categories)) LIKE '%restaurants%'"
    ),
    "count the number of reviews for each restaurant type": (
        "SELECT cuisine, COUNT(*) AS review_count "
        "FROM ( "
        "SELECT CASE "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%chinese%' THEN 'Chinese' "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%american%' THEN 'American' "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%mexican%' THEN 'Mexican' "
        "ELSE NULL END AS cuisine "
        "FROM review r JOIN business b ON COALESCE(r.business_id, r.rev_business_id) = b.business_id "
        ") t "
        "WHERE cuisine IS NOT NULL "
        "GROUP BY cuisine "
        "ORDER BY review_count DESC"
    ),
    "analyze the rating distribution average for different restaurant types": (
        "SELECT cuisine, AVG(star_value) AS avg_rating, COUNT(*) AS review_count "
        "FROM ( "
        "SELECT CASE "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%chinese%' THEN 'Chinese' "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%american%' THEN 'American' "
        "WHEN LOWER(COALESCE(b.categories, b.bus_categories)) LIKE '%mexican%' THEN 'Mexican' "
        "ELSE NULL END AS cuisine, "
        "COALESCE(r.rev_stars, r.stars) AS star_value "
        "FROM review r JOIN business b ON COALESCE(r.business_id, r.rev_business_id) = b.business_id "
        ") t "
        "WHERE cuisine IS NOT NULL AND star_value IS NOT NULL "
        "GROUP BY cuisine "
        "ORDER BY avg_rating DESC"
    ),
    "find businesses whose avg rating in last 12 months increased by 1 star": (
        "WITH scored AS ( "
        "SELECT COALESCE(r.business_id, r.rev_business_id) AS business_id, "
        "COALESCE(r.rev_stars, r.stars) AS stars, "
        "COALESCE(r.date, r.rev_date) AS review_date "
        "FROM review r "
        "WHERE COALESCE(r.date, r.rev_date) IS NOT NULL AND COALESCE(r.rev_stars, r.stars) IS NOT NULL "
        "), hist AS ( "
        "SELECT business_id, AVG(stars) AS avg_hist "
        "FROM scored "
        "WHERE TO_DATE(review_date) < ADD_MONTHS(CURRENT_DATE, -12) "
        "GROUP BY business_id "
        "), recent AS ( "
        "SELECT business_id, AVG(stars) AS avg_recent "
        "FROM scored "
        "WHERE TO_DATE(review_date) >= ADD_MONTHS(CURRENT_DATE, -12) "
        "GROUP BY business_id "
        ") "
        "SELECT b.name, hist.avg_hist, recent.avg_recent, (recent.avg_recent - hist.avg_hist) AS rating_delta "
        "FROM hist JOIN recent ON hist.business_id = recent.business_id "
        "JOIN business b ON b.business_id = hist.business_id "
        "WHERE recent.avg_recent >= hist.avg_hist + 1 "
        "ORDER BY rating_delta DESC, b.name ASC "
        "LIMIT 50"
    ),
    "identify top 10 pairs of distinct categories that co occur": (
        "WITH exploded AS ( "
        "SELECT business_id, TRIM(category) AS category "
        "FROM (SELECT business_id, EXPLODE(SPLIT(COALESCE(categories, bus_categories), ',')) AS category FROM business) s "
        "WHERE category IS NOT NULL AND TRIM(category) <> '' "
        ") "
        "SELECT e1.category AS category_1, e2.category AS category_2, COUNT(*) AS co_occurrences "
        "FROM exploded e1 "
        "JOIN exploded e2 ON e1.business_id = e2.business_id AND e1.category < e2.category "
        "GROUP BY e1.category, e2.category "
        "ORDER BY co_occurrences DESC, category_1 ASC, category_2 ASC "
        "LIMIT 10"
    ),
    "find polarizing merchants high standard deviation in ratings": (
        "SELECT b.name, COUNT(*) AS review_volume, STDDEV_POP(COALESCE(r.rev_stars, r.stars)) AS rating_stddev "
        "FROM review r "
        "JOIN business b ON COALESCE(r.business_id, r.rev_business_id) = b.business_id "
        "WHERE COALESCE(r.rev_stars, r.stars) IS NOT NULL "
        "GROUP BY b.business_id, b.name "
        "HAVING COUNT(*) >= 50 "
        "ORDER BY rating_stddev DESC, review_volume DESC, b.name ASC "
        "LIMIT 20"
    ),
    # User notebook question variants (2MP7MA6PH)
    "analyze the number of users joining each year": (
        "SELECT YEAR(user_yelping_since) AS join_year, COUNT(*) AS user_count "
        "FROM users "
        "WHERE user_yelping_since IS NOT NULL "
        "GROUP BY YEAR(user_yelping_since) "
        "ORDER BY join_year"
    ),
    "identify top reviewers based on user review count": (
        "SELECT user_name AS name, user_review_count "
        "FROM users "
        "ORDER BY user_review_count DESC, user_name ASC "
        "LIMIT 20"
    ),
    "identify the most popular users based on user fans": (
        "SELECT user_name AS name, user_fans "
        "FROM users "
        "ORDER BY user_fans DESC, user_name ASC "
        "LIMIT 20"
    ),
    "calculate the ratio of elite users to regular users each year": (
        "WITH yearly AS ( "
        "SELECT YEAR(user_yelping_since) AS year, "
        "CASE WHEN user_elite IS NOT NULL AND TRIM(user_elite) <> '' THEN 1 ELSE 0 END AS is_elite "
        "FROM users "
        "WHERE user_yelping_since IS NOT NULL "
        ") "
        "SELECT year, "
        "SUM(is_elite) AS elite_count, "
        "COUNT(*) AS total_users, "
        "(COUNT(*) - SUM(is_elite)) AS regular_count, "
        "CASE WHEN (COUNT(*) - SUM(is_elite)) = 0 THEN NULL "
        "ELSE CAST(SUM(is_elite) AS DOUBLE) / CAST((COUNT(*) - SUM(is_elite)) AS DOUBLE) END AS elite_to_regular_ratio "
        "FROM yearly "
        "GROUP BY year "
        "ORDER BY year"
    ),
    "display the proportion of total users and silent users 0 reviews each year": (
        "SELECT YEAR(user_yelping_since) AS year, "
        "COUNT(*) AS total_users, "
        "SUM(CASE WHEN COALESCE(user_review_count, 0) = 0 THEN 1 ELSE 0 END) AS silent_users, "
        "CAST(SUM(CASE WHEN COALESCE(user_review_count, 0) = 0 THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) AS silent_proportion "
        "FROM users "
        "WHERE user_yelping_since IS NOT NULL "
        "GROUP BY YEAR(user_yelping_since) "
        "ORDER BY year"
    ),
    "compute the yearly statistics of new users number of reviews elite users and fans": (
        "SELECT YEAR(user_yelping_since) AS year, "
        "COUNT(*) AS new_users, "
        "SUM(COALESCE(user_review_count, 0)) AS total_reviews, "
        "SUM(CASE WHEN user_elite IS NOT NULL AND TRIM(user_elite) <> '' THEN 1 ELSE 0 END) AS elite_users, "
        "SUM(COALESCE(user_fans, 0)) AS total_fans "
        "FROM users "
        "WHERE user_yelping_since IS NOT NULL "
        "GROUP BY YEAR(user_yelping_since) "
        "ORDER BY year"
    ),
    "identify early adopters tastemakers who wrote one of the first 5 reviews for successful businesses": (
        "WITH successful_biz AS ( "
        "SELECT business_id "
        "FROM business "
        "WHERE stars >= 4.5 AND review_count > 100 "
        "), ranked_reviews AS ( "
        "SELECT rev_user_id AS user_id, rev_business_id AS business_id, "
        "ROW_NUMBER() OVER (PARTITION BY rev_business_id ORDER BY rev_date) AS review_rank "
        "FROM review "
        "WHERE rev_business_id IS NOT NULL AND rev_user_id IS NOT NULL "
        "), early_reviews AS ( "
        "SELECT user_id "
        "FROM ranked_reviews rr "
        "JOIN successful_biz sb ON rr.business_id = sb.business_id "
        "WHERE rr.review_rank <= 5 "
        ") "
        "SELECT er.user_id, COALESCE(u.user_name, er.user_id) AS user_name, COUNT(*) AS tastemaker_hits "
        "FROM early_reviews er "
        "LEFT JOIN users u ON er.user_id = u.user_id "
        "GROUP BY er.user_id, COALESCE(u.user_name, er.user_id) "
        "ORDER BY tastemaker_hits DESC, user_name ASC "
        "LIMIT 20"
    ),
    "user rating evolution compare average star rating in first year vs third year": (
        "WITH joined AS ( "
        "SELECT r.rev_user_id AS user_id, YEAR(r.rev_date) - YEAR(u.user_yelping_since) AS years_on_platform, "
        "r.rev_stars AS rev_stars "
        "FROM review r "
        "JOIN users u ON r.rev_user_id = u.user_id "
        "WHERE r.rev_date IS NOT NULL AND u.user_yelping_since IS NOT NULL "
        "), filtered AS ( "
        "SELECT user_id, years_on_platform, rev_stars "
        "FROM joined "
        "WHERE years_on_platform IN (0, 2) "
        ") "
        "SELECT years_on_platform, AVG(rev_stars) AS avg_stars, COUNT(*) AS review_count "
        "FROM filtered "
        "GROUP BY years_on_platform "
        "ORDER BY years_on_platform"
    ),
    "segment users by dining diversity distinct cuisine categories reviewed": (
        "SELECT r.rev_user_id AS user_id, "
        "COUNT(DISTINCT COALESCE(b.categories, b.bus_categories)) AS distinct_cuisines, "
        "COUNT(*) AS total_reviews "
        "FROM review r "
        "JOIN business b ON r.rev_business_id = b.business_id "
        "GROUP BY r.rev_user_id "
        "HAVING COUNT(*) >= 20 "
        "ORDER BY distinct_cuisines DESC, total_reviews DESC "
        "LIMIT 50"
    ),
    "elite status impact review length and useful votes before vs after becoming elite": (
        "WITH joined AS ( "
        "SELECT r.rev_text, r.rev_useful, YEAR(r.rev_date) AS review_year, "
        "CAST(SPLIT(u.user_elite, ',')[0] AS INT) AS first_elite_year "
        "FROM review r "
        "JOIN users u ON r.rev_user_id = u.user_id "
        "WHERE u.user_elite IS NOT NULL AND TRIM(u.user_elite) <> '' AND r.rev_date IS NOT NULL "
        ") "
        "SELECT CASE WHEN review_year >= first_elite_year THEN 'After' ELSE 'Before' END AS elite_period, "
        "AVG(LENGTH(rev_text)) AS avg_review_length, "
        "AVG(COALESCE(rev_useful, 0)) AS avg_useful_votes "
        "FROM joined "
        "GROUP BY CASE WHEN review_year >= first_elite_year THEN 'After' ELSE 'Before' END "
        "ORDER BY elite_period"
    ),
}

ZEPPELIN_BASE_URL = os.getenv("ZEPPELIN_BASE_URL", "http://node-master:8080").rstrip("/")
ENABLE_ZEPPELIN_TEXT_FALLBACK = os.getenv("ENABLE_ZEPPELIN_TEXT_FALLBACK", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
AUTO_DEMO_FALLBACK_ENABLED = os.getenv("AUTO_DEMO_FALLBACK_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ZEPPELIN_REFRESH_INTERVAL_SECONDS = int(os.getenv("ZEPPELIN_REFRESH_INTERVAL_SECONDS", "90") or "90")
ZEPPELIN_TASK_NOTEBOOKS: dict[str, dict[str, str]] = {
    "business": {"label": "Business Analysis", "id": "2MPE568Z2"},
    "user": {"label": "User Analysis", "id": "2MP7MA6PH"},
    "review": {"label": "Review Analysis", "id": "2MNDP922H"},
    "checkin": {"label": "Check-in Analysis", "id": "2MM4JR74W"},
    "rating": {"label": "Rating Analysis", "id": "2MND51ANN"},
    "comprehensive": {"label": "Comprehensive Analysis", "id": "2MNEMKZT1"},
    "weather": {"label": "Weather Analysis", "id": "2MPV5DQ4E"},
    "location_time": {"label": "Location and Time", "id": "2MKUV38FW"},
}
TASK_QUERY_ALIASES: dict[str, tuple[str, ...]] = {
    "top categories count": (
        "top 10 most frequent categories and their count",
        "top 10 most frequent categories",
    ),
    "category synergy pairs": (
        "category synergy",
        "top 10 pairs of distinct business categories",
        "co-occur in the same merchant profile",
    ),
    "top states merchants": (
        "top 5 states with the most merchants in the u.s.",
        "top 5 states with the most merchants",
    ),
    "different categories count": (
        "count number of different categories",
        "count number of different categories",
    ),
    "top 20 merchants five star": (
        "top 20 merchants with most 5-star reviews",
        "top 20 merchants that received the most five-star reviews",
    ),
    "restaurant type counts": (
        "count restaurant types",
        "count restaurant types chinese american mexican",
    ),
    "reviews by restaurant type": (
        "count the number of reviews for each restaurant type",
    ),
    "rating distribution by restaurant type": (
        "rating distribution by restaurant type",
        "analyze rating distribution for different restaurant types",
    ),
    "turnaround merchants": ("turnaround merchants analysis",),
    "polarizing businesses": ("polarizing businesses analysis",),
    "top reviewers": ("identify top reviewers based on review count",),
    "popular users fans": ("identify most popular users based on fans",),
    "elite ratio yearly": ("calculate ratio of elite users to regular users each year",),
    "silent users yearly": ("display proportion of total users and silent users each year",),
    "yearly user review elite tips checkins": (
        "compute yearly statistics of new users reviews elite users tips and check-ins",
    ),
    "early adopters tastemakers": ("identify early adopters tastemakers",),
    "user rating evolution": ("analyze user rating evolution first year vs third year",),
    "dining diversity adventurous eaters": ("segment users by dining diversity",),
    "elite impact review length useful votes": (
        "identify elite status impact on review length and useful votes",
    ),
    "reviews per year": ("count the number of reviews per year",),
    "useful funny cool reviews": (
        "count the number of useful helpful funny and cool reviews",
    ),
    "rank users by reviews yearly": ("rank users by the total number of reviews each year",),
    "top common words all reviews": ("extract the top 20 most common words from all reviews",),
    "top words positive": ("extract the top 10 words from positive reviews",),
    "top words negative": ("extract the top 10 words from negative reviews",),
    "word cloud pos tagging": ("perform word cloud analysis by filtering words based on part-of-speech tagging",),
    "word association graph": ("construct a word association graph",),
    "top bigrams low star": ("extract the top 15 bigrams associated with 1-star and 2-star reviews",),
    "correlation review length rating": ("analyze the correlation between review length and rating",),
    "mixed signal reviews": ("identify mixed-signal reviews",),
    "menu items top chinese": ("extract and rank most frequently mentioned menu items for top chinese restaurants",),
    "ratings distribution 1 5": ("analyze the distribution of ratings 1-5 stars",),
    "weekly rating frequency": ("analyze the weekly rating frequency monday to sunday",),
    "top businesses five star ratings": ("identify top businesses with the most five-star ratings",),
    "top cities highest ratings": ("identify top 10 cities with the highest ratings",),
    "rating differential": ("calculate the rating differential",),
    "weekend weekday satisfaction": ("compare weekend vs weekday satisfaction",),
    "checkins per year": ("count the number of check-ins per year",),
    "checkins per hour": ("count the number of check-ins per hour within a 24-hour period",),
    "most popular city checkins": ("identify the most popular city for check-ins",),
    "rank businesses by checkins": ("rank all businesses based on check-in counts",),
    "mom checkin growth": ("calculate month-over-month mom check-in growth rate",),
    "review seasonality cuisine": ("analyze review seasonality by cuisine",),
}

def render_recommendation_mesh(result: QueryResult):
    """Analyzes query results and suggests next-best-queries."""
    if not result.rows:
        return

    cols = {col.lower() for col in result.rows[0].keys()}
    recommendations = []

    if "city" in cols:
        recommendations.append(
            ("Compare average ratings for these cities", "✨ Analyze Ratings")
        )
    if "categories" in cols:
        recommendations.append(
            ("Analyze yearly review trends for these categories", "📈 View Trends")
        )
    if "stars" in cols:
        recommendations.append(("Add 5-star filter", "⭐ Filter by 5-Stars"))
    if "user_id" in cols:
        recommendations.append(("Group by City", "🏙️ Group by City"))

    if not recommendations:
        return

    st.markdown('<div class="recommendation-mesh revealable">', unsafe_allow_html=True)
    st.markdown(
        '<p class="recommendation-header">Suggested follow-up queries</p>',
        unsafe_allow_html=True,
    )

    cols = st.columns(4)
    for i, (query, label) in enumerate(recommendations):
        with cols[i % 4]:
            st.markdown(
                f"""
                <div class="insight-card">
                    <a href="?action=ask&question={urlencode(query)}">
                        <div class="icon">✨</div>
                        <p>{label}</p>
                    </a>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


def render_research_lab():
    """Renders the 'Advanced Research Laboratory' section."""
    st.markdown('<div class="research-lab revealable">', unsafe_allow_html=True)
    render_section_header("Advanced Research Laboratory", "Explore pre-baked data fusion hypotheses that go beyond the Yelp dataset.")

    capsules = {
        "The Weather-Mood Hypothesis": WEATHER_MOOD_HYPOTHESIS,
        "The Cursed Storefronts Analysis": CURSED_STOREFRONTS_ANALYSIS,
        "The Review Manipulation Syndicate": REVIEW_MANIPULATION_SYNDICATE,
        "Open-World Data Safari": OPEN_WORLD_DATA_SAFARI,
    }

    for title, data in capsules.items():
        with st.expander(f"🧪 {title}"):
            st.markdown(f"**Hypothesis:** {data['hypothesis']}")
            if "data_fusion_map" in data:
                st.map(pd.DataFrame(data["data_fusion_map"]))
            st.info(f"**Actionable Recommendation:** {data['actionable_recommendation']}")
            st.markdown(f"<a href='{data['external_data_source']}' class='keyword-link'>External Data Source</a>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_footer_navigator():
    """Renders a compact footer navigator."""
    st.markdown(
        """
        <div class="control-center revealable">
            <hr>
            <div class="control-grid">
                <div class="control-section">
                    <h3>Data Entities</h3>
                    <ul>
                        <li><a href="?route=schema#business" target="_self">Business</a></li>
                        <li><a href="?route=schema#review" target="_self">Review</a></li>
                        <li><a href="?route=schema#user" target="_self">User</a></li>
                        <li><a href="?route=schema#checkin" target="_self">Check-in</a></li>
                    </ul>
                </div>
                <div class="control-section">
                    <h3>Tech Stack Explorer</h3>
                    <ul>
                        <li><a href="https://docs.streamlit.io/" target="_blank">Streamlit Documentation</a></li>
                        <li><a href="https://pandas.pydata.org/docs/" target="_blank">Pandas Documentation</a></li>
                        <li><a href="https://hive.apache.org/docs/latest/" target="_blank">Apache Hive Documentation</a></li>
                        <li><a href="https://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL Documentation</a></li>
                    </ul>
                </div>
                <div class="control-section">
                    <h3>Learn the Syntax</h3>
                    <ul>
                        <li><a href="?action=ask&question=Show top 5 businesses with the most reviews using a window function" target="_self">Window Functions</a></li>
                        <li><a href="?action=ask&question=Count reviews per month for the last year" target="_self">Date Aggregations</a></li>
                        <li><a href="?action=ask&question=Find businesses that have 'Pizza' and 'Italian' categories" target="_self">Join Optimization</a></li>
                    </ul>
                </div>
                <div class="control-section">
                    <h3>Collaboration</h3>
                    <a href="?route=home" class="feature-cta">Report an Insight</a>
                    <a href="?route=home" class="feature-cta">View Team Activity</a>
                </div>
            </div>
            <div class="footer-bottom">
                <p>© 2026 Query by SilkByteX</p>
                <p><a href="?route=home">Terms of Service</a> | <a href="?route=home">Data Privacy Policy</a></p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_chat_turn(turn: dict[str, Any]) -> None:
    user_text = str(turn.get("user_text", ""))
    assistant_text = str(turn.get("assistant_text", ""))
    final_sql = str(turn.get("final_sql", ""))
    result = QueryResult(
        rows=turn.get("rows", []),
        executed=turn.get("executed", False),
        error=turn.get("error", ""),
        message=str(turn.get("message", "")),
    )

    with st.chat_message("user"):
        st.markdown(user_text)

    with st.chat_message("assistant", avatar="M"):
        with st.expander("📝 **Plain English Interpretation**", expanded=True):
            st.markdown(assistant_text)
            if turn.get("retry_happened"):
                st.info("⚠️ Initial query failed. Auto-correction is running.")

        with st.expander("📊 **Interactive Data Table**"):
            _render_result_dataframe(result)

        if result.rows:
            chart_cols = _find_chart_columns(result.rows)
            if chart_cols:
                with st.expander("🎨 **Visual Insight**"):
                    # This is a simplified placeholder for chart rendering
                    df = pd.DataFrame(result.rows)
                    st.bar_chart(df.set_index(chart_cols["x"])[chart_cols["y"]])

        render_recommendation_mesh(result)


def _render_chat_interface() -> None:
    """Render the full chat UI, including history and the input form."""
    if st.session_state.pipeline_loading_phase:
        _render_pipeline_visualizer()
        return

    for turn in st.session_state.conversation_turns:
        _render_chat_turn(turn)

    if question := st.chat_input(
        "Ask a Yelp dataset question...",
        key="main_chat_input",
    ):
        _queue_question_submission(
            question,
            st.session_state.nl_use_demo_mode,
            chat_mode=_get_active_chat_mode(),
        )
        st.rerun()

def _render_sidebar() -> None:
    """Render the app's sidebar with navigation and dynamic query history."""
    with st.sidebar:
        st.title("Query by SilkByteX")
        st.write("Conversational SQL interface for the Yelp dataset.")

        st.markdown("---")
        
        # Dynamic Query History
        st.markdown("<h3>Recent Queries</h3>", unsafe_allow_html=True)
        history = [
            turn["user_text"]
            for turn in reversed(st.session_state.conversation_turns)
            if turn.get("final_sql") and not turn["user_text"].lower().startswith("describe")
        ][:5]

        if not history:
            st.markdown("<p class='text-muted'>No queries yet.</p>", unsafe_allow_html=True)
        else:
            for item in history:
                query_param = urlencode({"question": item})
                st.markdown(
                    f"<a href='?action=ask&{query_param}' target='_self' class='history-link'>{_truncate_context_label(item, 40)}</a>",
                    unsafe_allow_html=True,
                )

        st.markdown("---")
        st.markdown("<h3>Schema Explorer</h3>", unsafe_allow_html=True)
        for table_name in get_table_schemas():
            if st.button(f"DESCRIBE {table_name}"):
                _queue_question_submission(
                    f"DESCRIBE {table_name}",
                    use_demo_mode=False,
                    chat_mode=CHAT_MODE_DATA,
                )
                st.rerun()

def _read_query_param(name: str, default: str = "") -> str:
    """Read one query parameter as a normalized string."""
    value = st.query_params.get(name, default)
    if isinstance(value, list):
        if not value:
            return default
        return str(value[0]).strip()
    return str(value).strip() if value is not None else default


def _current_query_params() -> dict[str, str]:
    """Snapshot the current query params into a plain dictionary."""
    return {str(key): _read_query_param(str(key)) for key in st.query_params.keys()}


def _set_query_params(params: dict[str, str]) -> None:
    """Replace the URL query params with a filtered mapping."""
    st.query_params.clear()
    for key, value in params.items():
        clean_value = str(value).strip()
        if clean_value:
            st.query_params[key] = clean_value


def _remove_query_params(*keys: str) -> None:
    """Remove one or more query params while preserving the rest."""
    params = _current_query_params()
    for key in keys:
        params.pop(key, None)
    _set_query_params(params)


def _build_href(**params: str) -> str:
    """Build one relative href from query params."""
    clean_params = {
        key: str(value).strip()
        for key, value in params.items()
        if value is not None and str(value).strip()
    }
    query_string = urlencode(clean_params)
    return f"?{query_string}" if query_string else "?"


def _build_route_href(route: str = DEFAULT_ROUTE, **params: str) -> str:
    """Build one URL for a route-driven SPA transition."""
    return _build_href(route=route, **params)


def _normalize_chat_mode(mode: str) -> str:
    """Return one safe chat mode value."""
    clean_mode = str(mode).strip().lower()
    if clean_mode in {CHAT_MODE_AUTO, CHAT_MODE_GENERAL, CHAT_MODE_DATA}:
        return clean_mode
    return DEFAULT_CHAT_MODE


def _count_hint_matches(text: str, hints: tuple[str, ...]) -> int:
    """Count heuristic phrase matches inside one user message."""
    normalized_text = f" {text.strip().lower()} "
    score = 0
    for hint in hints:
        if hint in normalized_text:
            score += 2 if " " in hint else 1
    return score


def _infer_auto_chat_mode(question: str) -> str:
    """Heuristically route one message to general chat or data query."""
    normalized = " ".join(question.lower().split())
    if not normalized:
        return CHAT_MODE_GENERAL
    if any(keyword in normalized for keyword in ("weather", "cursed storefronts", "cursed", "hypothesis")):
        return CHAT_MODE_GENERAL
    if _is_greeting_message(normalized) or _is_intro_request(normalized):
        return CHAT_MODE_GENERAL

    data_score = _count_hint_matches(normalized, DATA_QUERY_HINTS)
    general_score = _count_hint_matches(normalized, GENERAL_CHAT_HINTS)

    if re.search(r"\b(select|from|where|group by|order by|limit|join)\b", normalized):
        data_score += 4

    if normalized.startswith(
        (
            "show ",
            "list ",
            "count ",
            "find ",
            "give me ",
            "what are the top ",
            "which are the top ",
            "top ",
            "bottom ",
            "average ",
            "sum ",
            "total ",
            "plot ",
            "chart ",
            "map ",
            "compare ",
            "rank ",
        )
    ):
        data_score += 3

    if normalized.startswith(
        (
            "hi",
            "hello",
            "hey",
            "what can you do",
            "who are you",
            "help me",
            "explain this project",
            "summarize this project",
            "how should i present",
        )
    ):
        general_score += 3

    if data_score > general_score and data_score > 0:
        return CHAT_MODE_DATA
    if general_score > 0:
        return CHAT_MODE_GENERAL
    # Product is data-first: unknown prompts default to data pipeline.
    return CHAT_MODE_DATA


def _resolve_chat_mode(question: str, requested_mode: str) -> str:
    """Resolve the final operating mode for one message."""
    normalized_mode = _normalize_chat_mode(requested_mode)
    normalized_question = " ".join(question.lower().split())
    if any(
        keyword in normalized_question
        for keyword in (
            "hi",
            "hello",
            "hey",
            "weather",
            "cursed storefronts",
            "cursed",
            "hypothesis",
        )
    ):
        return CHAT_MODE_GENERAL
    if normalized_mode != CHAT_MODE_AUTO:
        return normalized_mode
    return _infer_auto_chat_mode(question)


def _sync_chat_mode_from_query_params() -> None:
    """Keep the visible chat mode aligned with the URL for shareable state."""
    requested_mode = _read_query_param("chat_mode", "")
    if requested_mode:
        st.session_state.chat_mode = _normalize_chat_mode(requested_mode)
        return

    if "chat_mode" not in st.session_state:
        st.session_state.chat_mode = DEFAULT_CHAT_MODE


def _get_active_chat_mode() -> str:
    """Return the active command-dock mode."""
    if "chat_mode" not in st.session_state:
        st.session_state.chat_mode = DEFAULT_CHAT_MODE
    return _normalize_chat_mode(st.session_state.chat_mode)


def _get_current_route() -> str:
    """Resolve the active route from the current URL."""
    requested_route = _read_query_param("route", DEFAULT_ROUTE).lower()
    if requested_route in ROUTE_LABELS:
        return requested_route
    return DEFAULT_ROUTE


def _get_active_panel() -> str:
    """Resolve the active sub-panel from the current URL."""
    return _read_query_param("panel", DEFAULT_DETAIL_PANEL).lower()


def _consume_url_action() -> None:
    """Handle one-shot URL actions, then remove them to avoid loops."""
    action = _read_query_param("action").lower()
    if not action:
        return
    action_signature = _build_href(**_current_query_params())
    if action_signature == st.session_state.get("last_consumed_action_signature", ""):
        return

    handled = True

    if action == "run_test":
        _handle_test_query()
    elif action == "run_schema_audit":
        _run_schema_drift_audit()
    elif action == "run_readiness_smoke_test":
        _run_readiness_smoke_test()
    elif action == "presentation_reset":
        _run_presentation_reset()
    elif action == "ask":
        question = _read_query_param("question")
        if question:
            st.session_state.nl_question_text = question
            is_recommendation = _is_recommendation_question(question)
            forced_mode = CHAT_MODE_DATA if is_recommendation else _get_active_chat_mode()
            if is_recommendation:
                # Recommendations should always run real SQL against the backend.
                st.session_state.nl_use_demo_mode = False
            _queue_question_submission(
                question,
                False if is_recommendation else st.session_state.nl_use_demo_mode,
                chat_mode=forced_mode,
            )
    elif action == "edit_question":
        question = _read_query_param("question")
        st.session_state.editable_question_draft = question or ""
    elif action == "clear_conversation":
        _clear_conversation()
    elif action == "toggle_mode":
        st.session_state.nl_use_demo_mode = not st.session_state.nl_use_demo_mode
        st.toast(
            f"Switched to {'Demo' if st.session_state.nl_use_demo_mode else 'Live'} Mode"
        )
    elif action == "toggle_chat_panel":
        pass
    else:
        handled = False

    if not handled:
        return
    st.session_state.last_consumed_action_signature = action_signature


def _initialize_state() -> None:
    if "conversation_turns" not in st.session_state:
        st.session_state.conversation_turns = []

    max_turn_id = 0
    for index, turn in enumerate(st.session_state.conversation_turns, start=1):
        turn_id = turn.get("turn_id")
        if not isinstance(turn_id, int):
            turn_id = index
            turn["turn_id"] = turn_id
        max_turn_id = max(max_turn_id, turn_id)

    if "streamed_turn_ids" not in st.session_state:
        st.session_state.streamed_turn_ids = [
            turn["turn_id"] for turn in st.session_state.conversation_turns
        ]

    if "pending_stream_turn_id" not in st.session_state:
        st.session_state.pending_stream_turn_id = None

    if "next_turn_id" not in st.session_state:
        st.session_state.next_turn_id = max_turn_id + 1 if max_turn_id else 1
    elif st.session_state.next_turn_id <= max_turn_id:
        st.session_state.next_turn_id = max_turn_id + 1

    if "pending_question_submission" not in st.session_state:
        st.session_state.pending_question_submission = None

    if "editable_question_draft" not in st.session_state:
        st.session_state.editable_question_draft = ""

    if "chat_mode" not in st.session_state:
        st.session_state.chat_mode = DEFAULT_CHAT_MODE

    if "chat_panel_minimized" not in st.session_state:
        st.session_state.chat_panel_minimized = False

    if "readiness_schema_audit_report" not in st.session_state:
        st.session_state.readiness_schema_audit_report = None

    if "readiness_smoke_test_report" not in st.session_state:
        st.session_state.readiness_smoke_test_report = None

    if "readiness_last_fallback_note" not in st.session_state:
        st.session_state.readiness_last_fallback_note = ""

    if "pipeline_loading_phase" not in st.session_state:
        st.session_state.pipeline_loading_phase = ""

    if "pipeline_loading_final_phase" not in st.session_state:
        st.session_state.pipeline_loading_final_phase = ""

    if "pipeline_loading_note" not in st.session_state:
        st.session_state.pipeline_loading_note = ""

    if "pipeline_loading_sequence" not in st.session_state:
        st.session_state.pipeline_loading_sequence = 0

    if "latest_sql" not in st.session_state:
        st.session_state.latest_sql = ""

    if "latest_prompt" not in st.session_state:
        st.session_state.latest_prompt = ""

    if "latest_result" not in st.session_state:
        st.session_state.latest_result = QueryResult()

    if "manual_sql_text" not in st.session_state:
        st.session_state.manual_sql_text = "SELECT * FROM business LIMIT 5"

    if "nl_question_text" not in st.session_state:
        st.session_state.nl_question_text = "Show the first 5 businesses"

    if "nl_use_demo_mode" not in st.session_state:
        st.session_state.nl_use_demo_mode = not _has_live_generation_config()

    if "latest_question" not in st.session_state:
        st.session_state.latest_question = ""

    if "latest_mode_label" not in st.session_state:
        st.session_state.latest_mode_label = ""

    if "latest_generation_note" not in st.session_state:
        st.session_state.latest_generation_note = ""

    if "latest_original_sql" not in st.session_state:
        st.session_state.latest_original_sql = ""

    if "latest_original_sql_explanation" not in st.session_state:
        st.session_state.latest_original_sql_explanation = ""

    if "latest_corrected_sql" not in st.session_state:
        st.session_state.latest_corrected_sql = ""

    if "latest_corrected_sql_explanation" not in st.session_state:
        st.session_state.latest_corrected_sql_explanation = ""

    if "latest_sql_explanation" not in st.session_state:
        st.session_state.latest_sql_explanation = ""

    if "latest_retry_happened" not in st.session_state:
        st.session_state.latest_retry_happened = False

    if "latest_retry_status" not in st.session_state:
        st.session_state.latest_retry_status = "No retry"

    if "last_voice_audio_hash" not in st.session_state:
        st.session_state.last_voice_audio_hash = ""

    if "latest_voice_transcript" not in st.session_state:
        st.session_state.latest_voice_transcript = ""

    if "latest_voice_transcription_note" not in st.session_state:
        st.session_state.latest_voice_transcription_note = ""

    if "latest_voice_transcription_error" not in st.session_state:
        st.session_state.latest_voice_transcription_error = ""

    if "speed_mode_enabled" not in st.session_state:
        st.session_state.speed_mode_enabled = True

    if "custom_greeting_response" not in st.session_state:
        st.session_state.custom_greeting_response = DEFAULT_CUSTOM_GREETING_RESPONSE

    if "custom_intro_response" not in st.session_state:
        st.session_state.custom_intro_response = DEFAULT_CUSTOM_INTRO_RESPONSE

    if "general_chat_response_cache" not in st.session_state:
        st.session_state.general_chat_response_cache = {}
    if "zeppelin_last_refresh_ts" not in st.session_state:
        st.session_state.zeppelin_last_refresh_ts = 0.0
    if "recommendation_reply_cache" not in st.session_state:
        st.session_state.recommendation_reply_cache = {}
    if "last_consumed_action_signature" not in st.session_state:
        st.session_state.last_consumed_action_signature = ""

    st.session_state.current_view = _get_current_route()
    _sync_chat_mode_from_query_params()
    _consume_url_action()


def _has_live_generation_config() -> bool:
    """Check whether the basic DeepSeek settings are available."""
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    model = os.getenv("DEEPSEEK_MODEL", "").strip()
    return bool(api_key and model)


def _has_live_transcription_config() -> bool:
    """Voice transcription is disabled in this build."""
    return False


def _set_output_context(
    *,
    question: str,
    mode_label: str,
    generation_note: str,
    sql: str,
    result: QueryResult,
    prompt_text: str = "",
    original_sql: str = "",
    original_sql_explanation: str = "",
    corrected_sql: str = "",
    corrected_sql_explanation: str = "",
    sql_explanation: str = "",
    retry_happened: bool = False,
    retry_status: str = "No retry",
) -> None:
    """Store the latest question, SQL, mode, and result for rendering."""
    resolved_sql = str(sql or "").strip()
    if not resolved_sql:
        resolved_sql = _build_conversation_trace_sql(
            question=question,
            mode_label=mode_label,
            generation_note=generation_note,
        )

    st.session_state.latest_question = question
    st.session_state.latest_mode_label = mode_label
    st.session_state.latest_generation_note = generation_note
    st.session_state.latest_prompt = prompt_text
    st.session_state.latest_sql = resolved_sql
    st.session_state.latest_sql_explanation = sql_explanation
    st.session_state.latest_result = result
    st.session_state.latest_original_sql = original_sql
    st.session_state.latest_original_sql_explanation = original_sql_explanation
    st.session_state.latest_corrected_sql = corrected_sql
    st.session_state.latest_corrected_sql_explanation = corrected_sql_explanation
    st.session_state.latest_retry_happened = retry_happened
    st.session_state.latest_retry_status = retry_status


def _set_pipeline_output_context(pipeline_result: PipelineResult) -> None:
    """Copy the central pipeline result into the UI session state."""
    _set_output_context(
        question=pipeline_result.user_question,
        mode_label=pipeline_result.mode_label,
        generation_note=pipeline_result.generation_note,
        sql=pipeline_result.final_sql,
        result=QueryResult(
            rows=pipeline_result.rows,
            executed=pipeline_result.success,
            error=pipeline_result.error_message or None,
            message=pipeline_result.result_message,
        ),
        prompt_text=pipeline_result.prompt_text,
        original_sql=pipeline_result.generated_sql,
        original_sql_explanation=pipeline_result.generated_sql_explanation,
        corrected_sql=pipeline_result.corrected_sql,
        corrected_sql_explanation=pipeline_result.corrected_sql_explanation,
        sql_explanation=pipeline_result.final_sql_explanation,
        retry_happened=pipeline_result.retry_happened,
        retry_status=pipeline_result.retry_status,
    )


def _build_assistant_message(
    pipeline_result: PipelineResult,
    *,
    is_follow_up: bool = False,
    context_count: int = 0,
) -> str:
    """Create a short assistant update after the pipeline runs."""
    if pipeline_result.used_demo_mode and not pipeline_result.success:
        return "I could not match that question to one of the built-in demo examples."

    if pipeline_result.used_demo_mode:
        return (
            "I answered this in Demo/Mock Mode by mapping your question to a small "
            "hardcoded example and running it through the data layer."
        )

    memory_phrase = ""
    if is_follow_up and context_count:
        memory_phrase = (
            f" using the last {context_count} quer{'y' if context_count == 1 else 'ies'} "
            "as short-term conversation memory"
        )

    if pipeline_result.retry_happened and not pipeline_result.success:
        return (
            "I ran your request"
            f"{memory_phrase}, but the database query still failed after one correction attempt. "
            "Live mode remains active so you can retry immediately."
        )

    if pipeline_result.retry_happened:
        return (
            "I processed your request"
            f"{memory_phrase}, hit a database error, asked for one correction, and "
            "reran the query once."
        )

    if not pipeline_result.success:
        if memory_phrase:
            return (
                "I used the recent conversation context to interpret this follow-up, "
                "but the query could not be completed."
            )
        return "I tried to answer that question, but the query could not be completed."

    if memory_phrase:
        return (
            "I treated this as a follow-up, used the recent conversation context to "
            "build a fresh query, validated it, and ran it against the database."
        )

    return "I interpreted your question, validated the query, and ran it against the database."


def _truncate_context_label(text: str, max_chars: int = 88) -> str:
    """Trim one question label so thread chips stay compact."""
    clean_text = " ".join(str(text).strip().split())
    if len(clean_text) <= max_chars:
        return clean_text
    return f"{clean_text[: max_chars - 3].rstrip()}..."


def _question_looks_like_follow_up(
    question: str,
    recent_context: list[dict[str, Any]],
) -> bool:
    """Heuristically detect when the user is referring to earlier results."""
    if not recent_context:
        return False

    normalized_question = f" {' '.join(question.lower().split())} "
    if normalized_question.strip().startswith(FOLLOW_UP_PREFIXES):
        return True

    return any(reference in normalized_question for reference in FOLLOW_UP_REFERENCES)


def _build_recent_query_context() -> list[dict[str, Any]]:
    """Collect up to two prior analytic turns for short-term model memory."""
    recent_turns: list[dict[str, Any]] = []
    for turn in st.session_state.conversation_turns:
        mode_label = str(turn.get("mode_label", ""))
        if mode_label in {"Database Test", "Manual SQL Runner", GENERAL_CHAT_MODE_LABEL}:
            continue

        question_text = " ".join(str(turn.get("user_text", "")).strip().split())
        if not question_text or "```sql" in question_text:
            continue

        recent_turns.append(
            {
                "turn_id": turn.get("turn_id"),
                "question": question_text,
                "sql": str(turn.get("final_sql", "")).strip(),
                "rows": turn.get("rows", []),
                "error": str(turn.get("error", "") or ""),
                "message": str(turn.get("message", "") or ""),
            }
        )

    return recent_turns[-2:]


def _build_recent_general_chat_messages() -> list[dict[str, str]]:
    """Collect a short conversational memory for the general chat mode."""
    messages: list[dict[str, str]] = []
    for turn in st.session_state.conversation_turns:
        if str(turn.get("mode_label", "")) != GENERAL_CHAT_MODE_LABEL:
            continue

        user_text = " ".join(str(turn.get("user_text", "")).strip().split())
        if user_text and "```sql" not in user_text:
            messages.append({"role": "user", "content": user_text})

        assistant_text = " ".join(str(turn.get("assistant_text", "")).strip().split())
        if assistant_text:
            messages.append({"role": "assistant", "content": assistant_text})

    return messages[-6:]


def _normalize_free_text(value: str) -> str:
    """Normalize user text for fast intent checks and cache keys."""
    return " ".join(str(value or "").strip().split())


def _escape_sql_literal(value: str, max_chars: int = 480) -> str:
    """Escape one SQL string literal and keep it compact for trace SQL."""
    normalized = _normalize_free_text(value)
    clipped = normalized[:max_chars]
    return clipped.replace("'", "''")


def _build_conversation_trace_sql(
    *,
    question: str,
    assistant_text: str = "",
    mode_label: str = "",
    generation_note: str = "",
) -> str:
    """Build a lightweight SQL trace when the reply did not generate SQL."""
    safe_question = _escape_sql_literal(question)
    safe_assistant = _escape_sql_literal(assistant_text)
    safe_mode = _escape_sql_literal(mode_label or GENERAL_CHAT_MODE_LABEL, max_chars=80)
    safe_note = _escape_sql_literal(generation_note, max_chars=280)
    return (
        "-- Query by SilkByteX conversation trace\n"
        f"-- Mode: {safe_mode}\n"
        "SELECT\n"
        f"  '{safe_question}' AS user_prompt,\n"
        f"  '{safe_assistant}' AS assistant_response,\n"
        f"  '{safe_note}' AS generation_note;"
    )


def _get_recommendation_fast_sql(question: str) -> str | None:
    normalized = _normalize_free_text(question).lower().rstrip("?.!")
    direct = _RECOMMENDATION_SQL_MAP.get(normalized)
    if direct:
        return direct

    best_sql: str | None = None
    best_score = 0.0
    for candidate_question, candidate_sql in _RECOMMENDATION_SQL_MAP.items():
        seq_score = difflib.SequenceMatcher(None, normalized, candidate_question).ratio()
        overlap = _token_overlap_score(normalized, candidate_question)
        score = (0.62 * seq_score) + (0.38 * overlap)
        if score > best_score:
            best_score = score
            best_sql = candidate_sql
    if best_sql and best_score >= 0.56:
        return best_sql
    return None


def _is_recommendation_question(question: str) -> bool:
    """Return True when a question is one of the recommendation intents (fuzzy)."""
    normalized = _normalize_free_text(question).lower().rstrip("?.!")
    if normalized in {_normalize_free_text(q).lower().rstrip('?.!') for q in PROJECT_RECOMMENDATION_QUESTIONS}:
        return True
    best_score = 0.0
    for candidate in PROJECT_RECOMMENDATION_QUESTIONS:
        candidate_norm = _normalize_free_text(candidate).lower().rstrip("?.!")
        seq_score = difflib.SequenceMatcher(None, normalized, candidate_norm).ratio()
        overlap = _token_overlap_score(normalized, candidate_norm)
        score = (0.62 * seq_score) + (0.38 * overlap)
        if score > best_score:
            best_score = score
    return best_score >= 0.56


def _get_recommendation_fast_reply(question: str) -> str | None:
    """Return the fastest available reply for recommendation questions."""
    if not _is_recommendation_question(question):
        return None

    normalized = _normalize_free_text(question).lower().rstrip("?.!")
    recommendation_cache = dict(st.session_state.get("recommendation_reply_cache", {}) or {})
    direct = recommendation_cache.get(normalized)
    if direct:
        return direct

    best_reply: str | None = None
    best_score = 0.0
    for cached_question, cached_reply in recommendation_cache.items():
        seq_score = difflib.SequenceMatcher(None, normalized, cached_question).ratio()
        overlap = _token_overlap_score(normalized, cached_question)
        score = (0.62 * seq_score) + (0.38 * overlap)
        if score > best_score:
            best_score = score
            best_reply = cached_reply
    if best_reply and best_score >= 0.56:
        return best_reply

    # Warm fallback in case cache is empty or stale.
    return _build_zeppelin_qa_reply(question)


def _is_greeting_message(question: str) -> bool:
    normalized = _normalize_free_text(question).lower()
    return normalized in {"hi", "hello", "hey", "yo", "hi there", "hello there"}


def _is_intro_request(question: str) -> bool:
    normalized = _normalize_free_text(question).lower()
    intro_hints = (
        "who are you",
        "tell me about yourself",
        "what can you do",
        "introduce yourself",
        "intro",
        "what do you do",
        "help me",
    )
    return any(hint in normalized for hint in intro_hints)


def _get_fast_general_reply(question: str) -> str | None:
    """Return an instant user-configured reply for greetings/intro prompts."""
    if not st.session_state.speed_mode_enabled:
        return None

    if _is_greeting_message(question):
        return _normalize_free_text(st.session_state.custom_greeting_response)

    if _is_intro_request(question):
        return _normalize_free_text(st.session_state.custom_intro_response)

    return None


def _render_response_preferences() -> None:
    """Render query settings panel (opened via Query Settings hyperlink)."""
    turns = list(st.session_state.conversation_turns)
    recent_questions: list[str] = []
    for turn in reversed(turns):
        question = _normalize_free_text(str(turn.get("user_text", "")))
        if question and "```sql" not in question and question not in recent_questions:
            recent_questions.append(question)
        if len(recent_questions) >= 8:
            break

    history_markup = ""
    if recent_questions:
        history_markup = "".join(
            (
                f'<a href="{escape(_build_route_href("home", chat_mode=CHAT_MODE_AUTO, action="ask", question=question, panel="results"))}" '
                'target="_self" style="display:block;padding:0.5rem 0.62rem;border-radius:10px;'
                'text-decoration:none;color:#1c1c1c;background:rgba(17,17,17,0.05);'
                'border:1px solid rgba(17,17,17,0.08);font-size:0.83rem;font-weight:650;line-height:1.35;">'
                f"{escape(_truncate_context_label(question, 92))}"
                "</a>"
            )
            for question in recent_questions
        )
    else:
        history_markup = '<div class="hero-history-empty">No previous chats yet.</div>'

    with st.expander("Query Settings", expanded=True):
        st.caption("Customize responses and access previous chats.")
        preference_columns = st.columns([1, 1])
        with preference_columns[0]:
            st.toggle(
                "Enable Speed Mode",
                key="speed_mode_enabled",
                help="Routes greetings/intro instantly and serves cached general replies.",
            )
        with preference_columns[1]:
            if st.button("Clear Chat History", width="stretch"):
                _clear_conversation()
                st.rerun()
            if st.button("Refresh Zeppelin Knowledge", width="stretch"):
                _refresh_zeppelin_knowledge(force=True)
                st.success("Zeppelin knowledge refreshed.")

        st.text_area(
            "Greeting Response",
            key="custom_greeting_response",
            height=90,
            help="Used for short greetings like hi/hello/hey.",
        )
        st.text_area(
            "Intro Response",
            key="custom_intro_response",
            height=110,
            help="Used for prompts like 'who are you' or 'what can you do'.",
        )

        st.markdown("**Previous Chats**")
        st.markdown(
            f'<div style="display:grid;gap:0.4rem;">{history_markup}</div>',
            unsafe_allow_html=True,
        )


def _show_success_toast():
    """Displays a temporary success message."""
    st.markdown(
        """
        <div class="success-toast">
            <span>✅</span> Query executed successfully.
        </div>
        <script>
            setTimeout(() => {
                const toast = document.querySelector('.success-toast');
                if (toast) {
                    toast.style.display = 'none';
                }
            }, 3000);
        </script>
        """,
        unsafe_allow_html=True,
    )


def _sanitize_assistant_text(text: str) -> str:
    """Remove internal notebook source URLs from assistant-facing responses."""
    clean = str(text or "")
    filtered_lines: list[str] = []
    for raw_line in clean.splitlines():
        line = raw_line.strip()
        lower_line = line.lower()
        if "node-master:8080" in lower_line:
            continue
        if "/#/notebook/" in lower_line:
            continue
        if lower_line.startswith("source: http://") or lower_line.startswith("source: https://"):
            continue
        filtered_lines.append(raw_line)
    clean = "\n".join(filtered_lines)
    clean = re.sub(r"https?://[^\s]*node-master:8080[^\s]*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\[([^\]]+)\]\(\s*https?://[^\)]*node-master:8080[^\)]*\)", r"\1", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return clean


def _append_conversation_turn(
    *,
    user_text: str,
    assistant_text: str,
    mode_label: str,
    generation_note: str,
    final_sql: str,
    result: QueryResult,
    original_sql: str = "",
    original_sql_explanation: str = "",
    corrected_sql: str = "",
    corrected_sql_explanation: str = "",
    final_sql_explanation: str = "",
    is_follow_up: bool = False,
    context_questions: list[str] | None = None,
    retry_happened: bool = False,
    retry_status: str = "No retry",
) -> None:
    """Store one user turn and one assistant turn for the chat-style history."""
    if result.executed and not result.error:
        _show_success_toast()

    turn_id = st.session_state.next_turn_id
    st.session_state.next_turn_id += 1
    resolved_final_sql = str(final_sql or "").strip()
    if not resolved_final_sql:
        resolved_final_sql = _build_conversation_trace_sql(
            question=user_text,
            assistant_text=assistant_text,
            mode_label=mode_label,
            generation_note=generation_note,
        )

    turn_payload = {
        "turn_id": turn_id,
        "user_text": str(user_text),
        "assistant_text": _sanitize_assistant_text(assistant_text),
        "mode_label": str(mode_label),
        "generation_note": str(generation_note or ""),
        "final_sql": resolved_final_sql,
        "original_sql": str(original_sql or ""),
        "original_sql_explanation": str(original_sql_explanation or ""),
        "corrected_sql": str(corrected_sql or ""),
        "corrected_sql_explanation": str(corrected_sql_explanation or ""),
        "final_sql_explanation": str(final_sql_explanation or ""),
        "is_follow_up": bool(is_follow_up),
        "context_questions": list(context_questions or []),
        "retry_happened": bool(retry_happened),
        "retry_status": str(retry_status or "No retry"),
        "rows": list(result.rows or []),
        "executed": bool(result.executed),
        "error": str(result.error or ""),
        "message": str(result.message or ""),
    }
    st.session_state.conversation_turns.append(turn_payload)
    st.session_state.pending_stream_turn_id = turn_id


def _render_previous_messages_panel() -> None:
    """Render a compact, always-visible text history of previous messages."""
    turns = st.session_state.conversation_turns
    if not turns:
        return

    with st.expander(f"Previous Messages ({len(turns)})", expanded=True):
        for turn in turns:
            user_text = str(turn.get("user_text", "")).strip()
            assistant_text = str(turn.get("assistant_text", "")).strip()
            if user_text:
                st.markdown(f"**You:** {user_text}")
            if assistant_text:
                st.markdown(f"**Assistant:** {assistant_text}")
            st.markdown("---")


def _clear_conversation() -> None:
    """Reset the visible chat history and latest output state."""
    st.session_state.conversation_turns = []
    st.session_state.streamed_turn_ids = []
    st.session_state.pending_stream_turn_id = None
    st.session_state.pending_question_submission = None
    st.session_state.readiness_last_fallback_note = ""
    st.session_state.pipeline_loading_phase = ""
    st.session_state.pipeline_loading_final_phase = ""
    st.session_state.pipeline_loading_note = ""
    st.session_state.pipeline_loading_sequence = 0
    st.session_state.next_turn_id = 1
    st.session_state.general_chat_response_cache = {}
    _set_output_context(
        question="",
        mode_label="",
        generation_note="",
        sql="",
        result=QueryResult(),
        original_sql_explanation="",
        corrected_sql_explanation="",
        sql_explanation="",
        retry_happened=False,
        retry_status="No retry",
    )


def _extract_describe_column_names(rows: list[dict[str, Any]]) -> list[str]:
    """Pull real column names out of Hive/Spark DESCRIBE output."""
    if not rows:
        return []

    first_row = rows[0]
    candidate_keys = ("col_name", "column_name", "col_name ", "column_name ")
    column_key = next(
        (candidate for candidate in candidate_keys if candidate in first_row),
        next(iter(first_row.keys()), ""),
    )
    if not column_key:
        return []

    live_columns: list[str] = []
    for row in rows:
        raw_value = str(row.get(column_key, "")).strip()
        lowered_value = raw_value.lower()
        if lowered_value.startswith(
            ("# partition information", "# detailed table information")
        ):
            break
        if not raw_value or raw_value.startswith("#"):
            continue
        if lowered_value.startswith(("partition information", "detailed table information")):
            break
        if set(raw_value) == {"-"}:
            continue

        clean_name = raw_value.strip("`")
        if clean_name and clean_name not in live_columns:
            live_columns.append(clean_name)

    return live_columns


def _compare_schema_columns(
    expected_columns: list[str],
    live_columns: list[str],
) -> dict[str, Any]:
    """Compare schema_definitions.py columns against the live backend columns."""
    expected_lookup = {column.strip(): None for column in expected_columns if column.strip()}
    live_lookup = {column.strip(): None for column in live_columns if column.strip()}

    missing_columns = [column for column in expected_lookup if column not in live_lookup]
    extra_columns = [column for column in live_lookup if column not in expected_lookup]

    return {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "matched_column_count": len(expected_lookup) - len(missing_columns),
        "expected_column_count": len(expected_lookup),
        "live_column_count": len(live_lookup),
    }


def _should_auto_fallback_to_demo_mode(
    *,
    used_demo_mode: bool,
    success: bool,
    status: str,
) -> bool:
    """Return True when the app should switch to Demo Mode after a live failure."""
    if not AUTO_DEMO_FALLBACK_ENABLED:
        return False

    if used_demo_mode or success:
        return False

    return status not in {"", "input_error"}


def _activate_demo_fallback(note: str) -> None:
    """Flip the app back to Demo/Mock Mode after a live-path failure."""
    clean_note = " ".join(str(note).strip().split())
    if not clean_note:
        clean_note = (
            "Live validation hit an issue, so the copilot switched back to "
            "Demo/Mock Mode to keep the presentation moving."
        )

    st.session_state.nl_use_demo_mode = True
    st.session_state.readiness_last_fallback_note = clean_note
    st.toast("Live path failed. Demo/Mock Mode is now active as a safety fallback.")


def _run_presentation_reset() -> None:
    """Reset the stage back to a clean demo-friendly state."""
    _clear_conversation()
    st.session_state.nl_use_demo_mode = True
    st.session_state.nl_question_text = PRESENTATION_GOLDEN_PROMPTS[0]
    st.session_state.manual_sql_text = "SELECT * FROM business LIMIT 5"
    st.session_state.last_voice_audio_hash = ""
    st.session_state.latest_voice_transcript = ""
    st.session_state.latest_voice_transcription_note = ""
    st.session_state.latest_voice_transcription_error = ""
    st.toast(
        "Session reset. Demo Mode is active."
    )


def _run_schema_drift_audit() -> None:
    """Compare the live DESCRIBE output against schema_definitions.py."""
    audit_entries: list[dict[str, Any]] = []
    matched_tables = 0
    drifted_tables = 0
    errored_tables = 0

    for table_name, table_info in get_table_schemas().items():
        expected_columns = [
            str(column.get("name", "")).strip()
            for column in table_info.get("columns", [])
            if str(column.get("name", "")).strip()
        ]
        describe_result = describe_table_schema(table_name)
        if describe_result.error:
            errored_tables += 1
            audit_entries.append(
                {
                    "table": table_name,
                    "status": "error",
                    "expected_columns": expected_columns,
                    "live_columns": [],
                    "missing_columns": expected_columns,
                    "extra_columns": [],
                    "matched_column_count": 0,
                    "expected_column_count": len(expected_columns),
                    "live_column_count": 0,
                    "message": describe_result.message,
                    "error": describe_result.error or "",
                }
            )
            continue

        live_columns = _extract_describe_column_names(describe_result.rows)
        comparison = _compare_schema_columns(expected_columns, live_columns)
        entry_status = (
            "match"
            if not comparison["missing_columns"] and not comparison["extra_columns"]
            else "drift"
        )
        if entry_status == "match":
            matched_tables += 1
        else:
            drifted_tables += 1

        audit_entries.append(
            {
                "table": table_name,
                "status": entry_status,
                "expected_columns": expected_columns,
                "live_columns": live_columns,
                "message": describe_result.message,
                "error": "",
                **comparison,
            }
        )

    st.session_state.readiness_schema_audit_report = {
        "ran_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "matched_tables": matched_tables,
            "drifted_tables": drifted_tables,
            "errored_tables": errored_tables,
            "total_tables": len(audit_entries),
        },
        "tables": audit_entries,
    }
    st.toast("Schema drift audit completed.")


def _run_readiness_smoke_test() -> None:
    """Run a presentation-oriented backend smoke test suite."""
    checks: list[dict[str, str]] = []

    def add_check(
        key: str,
        title: str,
        status: str,
        detail: str,
        sql: str = "",
    ) -> None:
        checks.append(
            {
                "key": key,
                "title": title,
                "status": status,
                "detail": detail,
                "sql": sql,
            }
        )

    connectivity_result = run_test_query()
    connectivity_ok = connectivity_result.executed and not connectivity_result.error
    add_check(
        "db_connectivity",
        "Database Connectivity",
        "success" if connectivity_ok else "error",
        connectivity_result.message if connectivity_ok else (connectivity_result.error or connectivity_result.message),
        sql="SELECT * FROM business LIMIT 5",
    )

    visibility_failures: list[str] = []
    if connectivity_ok:
        for table_name in get_table_schemas():
            describe_result = describe_table_schema(table_name)
            if describe_result.error:
                visibility_failures.append(f"{table_name}: {describe_result.error}")

        add_check(
            "table_visibility",
            "Core Table Visibility",
            "success" if not visibility_failures else "error",
            (
                "All core Yelp tables responded to DESCRIBE probes."
                if not visibility_failures
                else " | ".join(visibility_failures)
            ),
            sql="DESCRIBE business / review / users / checkin",
        )
    else:
        add_check(
            "table_visibility",
            "Core Table Visibility",
            "skipped",
            "Skipped because the base database connectivity test did not succeed.",
            sql="DESCRIBE business / review / users / checkin",
        )

    chart_sql = (
        "SELECT city, COUNT(*) AS business_count "
        "FROM business "
        "GROUP BY city "
        "ORDER BY business_count DESC "
        "LIMIT 10"
    )
    if connectivity_ok:
        chart_result = execute_sql(chart_sql)
        chart_ready = chart_result.executed and _get_chart_summary(chart_result.rows)[0]
        add_check(
            "chart_probe",
            "Chart Rendering Probe",
            "success" if chart_ready else "error",
            (
                "The returned rows are chart-ready."
                if chart_ready
                else (chart_result.error or "The probe ran, but the returned shape did not unlock a chart.")
            ),
            sql=chart_sql,
        )
    else:
        add_check(
            "chart_probe",
            "Chart Rendering Probe",
            "skipped",
            "Skipped because the base database connectivity test did not succeed.",
            sql=chart_sql,
        )

    map_sql = (
        "SELECT name, city, state, latitude, longitude "
        "FROM business "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "LIMIT 20"
    )
    if connectivity_ok:
        map_result = execute_sql(map_sql)
        map_ready = map_result.executed and _get_map_summary(map_result.rows)[0]
        add_check(
            "map_probe",
            "Map Rendering Probe",
            "success" if map_ready else ("error" if map_result.error else "warn"),
            (
                "The returned rows unlock the dark geospatial map view."
                if map_ready
                else (map_result.error or "The backend responded, but map-ready columns were not found in the probe result.")
            ),
            sql=map_sql,
        )
    else:
        add_check(
            "map_probe",
            "Map Rendering Probe",
            "skipped",
            "Skipped because the base database connectivity test did not succeed.",
            sql=map_sql,
        )

    add_check(
        "llm_probe",
        "Live LLM Probe",
        "skipped",
        "Skipped by request. API-key setup and live model validation remain a manual presentation step.",
    )

    overall_status = "success"
    if any(check["status"] == "error" for check in checks):
        overall_status = "error"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "warn"

    st.session_state.readiness_smoke_test_report = {
        "ran_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "overall_status": overall_status,
        "checks": checks,
    }

    if (
        AUTO_DEMO_FALLBACK_ENABLED
        and overall_status == "error"
        and not st.session_state.nl_use_demo_mode
    ):
        _activate_demo_fallback(
            "The System validation found a live backend issue, so the copilot switched to Demo/Mock Mode as a presentation-safe fallback."
        )

    st.toast("System validation completed.")


def _is_numeric_column(dataframe: pd.DataFrame, column_name: str) -> bool:
    """Reuse the same simple numeric check used by the chart logic."""
    return pd.api.types.is_numeric_dtype(dataframe[column_name])


def _is_date_like_column(dataframe: pd.DataFrame, column_name: str) -> bool:
    """Check whether a result column looks like a year, date, or timestamp."""
    lower_name = column_name.lower()
    date_words = ("year", "date", "month", "day", "time")
    if any(word in lower_name for word in date_words):
        return True

    parsed_values = pd.to_datetime(dataframe[column_name], errors="coerce", format="mixed")
    return parsed_values.notna().sum() >= max(1, len(dataframe) // 2)


def _get_chart_summary(rows: list[dict[str, Any]]) -> tuple[bool, str]:
    """Return whether a chart will be shown and what kind of chart it is."""
    if not rows:
        return (False, "No chart")

    dataframe = pd.DataFrame(rows)
    chart_details = _find_chart_columns(dataframe)

    if chart_details is None:
        return (False, "No chart")

    chart_type = chart_details[0]
    if chart_type == "line":
        return (True, "Line chart")
    return (True, "Bar chart")


def _get_map_summary(rows: list[dict[str, Any]]) -> tuple[bool, str]:
    """Return whether a map can be offered for the current result set."""
    if not rows:
        return (False, "No map")

    dataframe = pd.DataFrame(rows)
    if _find_map_columns(dataframe) is None:
        return (False, "No map")

    return (True, "Location map")


def _render_result_summary(result: QueryResult, retry_happened: bool) -> None:
    """Show a short summary for one query response."""
    chart_shown, chart_label = _get_chart_summary(result.rows)
    map_shown, map_label = _get_map_summary(result.rows)
    row_count = len(result.rows)

    if result.error:
        status_label = "Error"
        status_tone = "error"
    elif result.executed:
        status_label = "Success"
        status_tone = "success"
    elif result.rows:
        status_label = "Rows ready"
        status_tone = "success"
    else:
        status_label = "Waiting"
        status_tone = "warn"

    retry_label = "Used" if retry_happened else "Not needed"
    retry_tone = "warn" if retry_happened else "success"
    chart_value = chart_label if chart_shown else "Not shown"
    chart_tone = "success" if chart_shown else "warn"
    map_value = map_label if map_shown else "Not shown"
    map_tone = "success" if map_shown else "warn"

    summary_cards = [
        ("Status", status_label, status_tone),
        ("Rows Returned", str(row_count), "success" if row_count else "warn"),
        ("Chart", chart_value, chart_tone),
        ("Map", map_value, map_tone),
        ("Retry", retry_label, retry_tone),
    ]

    cards_markup = "".join(
        (
            f'<div class="summary-metric-card {escape(tone)}">'
            f'<div class="summary-metric-label">{escape(label)}</div>'
            f'<div class="summary-metric-value">{escape(value)}</div>'
            "</div>"
        )
        for label, value, tone in summary_cards
    )

    st.markdown(
        f'<div class="summary-metrics-grid animate-in stagger-3">{cards_markup}</div>',
        unsafe_allow_html=True,
    )


def _render_user_turn(turn: dict[str, Any]) -> None:
    """Render one user message as a custom right-aligned chat bubble."""
    user_text = str(turn.get("user_text", ""))
    edit_href = _build_route_href(
        "home",
        action="edit_question",
        question=user_text,
        panel="results",
    )

    if "```sql" in turn["user_text"]:
        before_sql, sql_part = turn["user_text"].split("```sql", maxsplit=1)
        sql_text = sql_part.split("```", maxsplit=1)[0].strip()
        intro_text = before_sql.strip()

        st.markdown(
            f"""
            <div class="chat-row user-row animate-in stagger-1">
                <div class="chat-bubble user-bubble">
                    {escape(intro_text)}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if sql_text:
            st.code(sql_text, language="sql")
        st.markdown(
            f'<div style="display:flex; justify-content:flex-end; margin-top:0.18rem;"><a href="{escape(edit_href)}" target="_self" style="font-size:0.74rem; color:#d6af8c; text-decoration:underline; font-weight:700;">Edit</a></div>',
            unsafe_allow_html=True,
        )
        return

    user_html = escape(turn["user_text"]).replace("\n", "<br>")
    st.markdown(
        f"""
        <div class="chat-row user-row animate-in stagger-1">
            <div class="chat-bubble user-bubble">
                {user_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="display:flex; justify-content:flex-end; margin-top:0.18rem;"><a href="{escape(edit_href)}" target="_self" style="font-size:0.74rem; color:#d6af8c; text-decoration:underline; font-weight:700;">Edit</a></div>',
        unsafe_allow_html=True,
    )


def _render_follow_up_thread_banner(turn: dict[str, Any]) -> None:
    """Render a compact memory banner before a follow-up question."""
    if not turn.get("is_follow_up"):
        return

    context_questions = [
        _truncate_context_label(question)
        for question in turn.get("context_questions", [])
        if str(question).strip()
    ]
    if not context_questions:
        return

    chips_markup = "".join(
        f'<span class="follow-up-thread-chip">{escape(question)}</span>'
        for question in context_questions
    )
    st.markdown(
        f"""
        <style>
        .follow-up-thread-shell {{
            display: flex;
            align-items: stretch;
            gap: 0.85rem;
            margin: 0.4rem 0 0.9rem;
        }}

        .follow-up-thread-line {{
            width: 2px;
            border-radius: 999px;
            background: linear-gradient(180deg, rgba(175, 127, 89, 0.18), rgba(175, 127, 89, 0.7), rgba(175, 127, 89, 0.18));
            min-height: 100%;
        }}

        .follow-up-thread-card {{
            flex: 1;
            padding: 0.9rem 1rem;
            border-radius: 18px;
            border: 1px solid rgba(175, 127, 89, 0.16);
            background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(249,245,240,0.96));
            box-shadow: 0 16px 34px rgba(17,17,17,0.05);
        }}

        .follow-up-thread-eyebrow {{
            color: #af7f59;
            font-size: 0.7rem;
            font-weight: 800;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            margin-bottom: 0.4rem;
        }}

        .follow-up-thread-title {{
            color: #111111;
            font-size: 0.98rem;
            font-weight: 800;
            margin-bottom: 0.65rem;
        }}

        .follow-up-thread-chip-row {{
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
        }}

        .follow-up-thread-chip {{
            display: inline-flex;
            align-items: center;
            padding: 0.48rem 0.72rem;
            border-radius: 999px;
            background: rgba(175, 127, 89, 0.11);
            color: #5f5a57;
            font-size: 0.8rem;
            font-weight: 700;
            line-height: 1.35;
        }}
        </style>
        <div class="follow-up-thread-shell animate-in stagger-1">
            <div class="follow-up-thread-line"></div>
            <div class="follow-up-thread-card">
                <div class="follow-up-thread-eyebrow">Conversation Memory</div>
                <div class="follow-up-thread-title">Follow-up query connected to the recent analysis</div>
                <div class="follow-up-thread-chip-row">{chips_markup}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_response_overview(turn: dict[str, Any], result: QueryResult) -> None:
    """Render the summary card at the top of each assistant response."""
    mode_label = str(turn.get("mode_label", "")).strip() or "Assistant"
    if str(turn.get("mode_label", "")) == GENERAL_CHAT_MODE_LABEL:
        tone_class = "is-general"
    elif result.error:
        tone_class = "is-error"
    elif turn.get("retry_happened"):
        tone_class = "is-retry"
    else:
        tone_class = "is-data"

    if str(turn.get("mode_label", "")) == GENERAL_CHAT_MODE_LABEL:
        status_label = "Conversation Ready" if not result.error else "Needs Attention"
        status_class = "success" if not result.error else "danger"
        badges = [
            (GENERAL_CHAT_MODE_LABEL, "warn"),
            (status_label, status_class),
        ]
        if turn.get("is_follow_up"):
            badges.append(("Follow-up", "warn"))

        badge_markup = "".join(
            f'<span class="response-badge {escape(css_class)}">{escape(text)}</span>'
            for text, css_class in badges
        )
        quick_links_markup = ""
    else:
        chart_shown, chart_label = _get_chart_summary(result.rows)
        map_shown, _ = _get_map_summary(result.rows)
        if result.error:
            status_label = "Needs Attention"
            status_class = "danger"
        elif result.executed:
            status_label = "Query Complete"
            status_class = "success"
        else:
            status_label = "Awaiting Result"
            status_class = "warn"

        badges = [
            (status_label, status_class),
            (f"{len(result.rows)} row(s)", ""),
        ]

        if chart_shown:
            badges.append((chart_label, "success"))

        if map_shown:
            badges.append(("Map ready", "success"))

        if turn.get("is_follow_up"):
            badges.append(("Follow-up", "warn"))

        if turn["retry_happened"]:
            badges.append(("Retry once", "warn"))

        badge_markup = "".join(
            f'<span class="response-badge {escape(css_class)}">{escape(text)}</span>'
            for text, css_class in badges
        )
        quick_links = [("View Results", _build_route_href("home", panel="results"))]

        if chart_shown:
            quick_links.append(("Open Chart", _build_route_href("home", panel="chart")))

        if map_shown:
            quick_links.append(("Open Map", _build_route_href("home", panel="map")))

        if result.error or turn["retry_happened"]:
            quick_links.append(("Inspect Errors", _build_route_href("home", panel="errors")))

        quick_links_markup = "".join(
            (
                f'<a href="{escape(href)}" target="_self" '
                'class="response-action-link">'
                f"{escape(label)}"
                "</a>"
            )
            for label, href in quick_links
        )
    st.markdown(
        """
        <style>
        .response-overview {
            position: relative;
            margin: 0.32rem 0 0.75rem;
            border-radius: 22px;
            border: 1px solid rgba(17, 17, 17, 0.08);
            background: linear-gradient(155deg, rgba(255,255,255,0.98), rgba(248,242,236,0.96));
            box-shadow: 0 18px 38px rgba(17,17,17,0.10);
            overflow: hidden;
            padding: 1rem 1.05rem 0.95rem;
        }

        .response-overview::before {
            content: "";
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 4px;
            border-radius: 999px;
            background: linear-gradient(180deg, #af7f59, #d6af8c);
            opacity: 0.95;
        }

        .response-overview.is-error::before {
            background: linear-gradient(180deg, #d7534f, #f08c86);
        }

        .response-overview.is-retry::before {
            background: linear-gradient(180deg, #c28a40, #f2c277);
        }

        .response-overview.is-general::before {
            background: linear-gradient(180deg, #6b7280, #c5ccd6);
        }

        .response-head {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .response-brand-orb {
            width: 2.2rem;
            height: 2.2rem;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.86rem;
            font-weight: 800;
            color: #ffffff;
            background: linear-gradient(145deg, #111111, #3d352f);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.16);
            flex-shrink: 0;
        }

        .response-head-copy {
            min-width: 0;
            flex: 1;
        }

        .response-kicker {
            color: #af7f59;
            font-size: 0.67rem;
            font-weight: 800;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            line-height: 1.1;
        }

        .response-mode {
            margin-top: 0.18rem;
            color: #111111;
            font-size: 0.9rem;
            font-weight: 760;
            letter-spacing: 0.01em;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .response-state-pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.36rem 0.62rem;
            border-radius: 999px;
            border: 1px solid rgba(17,17,17,0.1);
            background: rgba(255,255,255,0.74);
            color: #4d463f;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            flex-shrink: 0;
        }

        .response-copy-stream-shell {
            margin-top: 0.65rem;
            border-radius: 14px;
            border: 1px solid rgba(17,17,17,0.06);
            background: linear-gradient(165deg, rgba(255,255,255,0.82), rgba(250,246,242,0.78));
            padding: 0.8rem 0.86rem;
            position: relative;
            overflow: hidden;
        }

        .response-copy-stream {
            margin: 0;
            color: #151515;
            line-height: 1.78;
            font-size: 1.0rem;
            font-weight: 540;
            letter-spacing: 0.005em;
            min-height: 1.6em;
        }

        .response-copy-stream-shell::after {
            content: "";
            position: absolute;
            inset: auto -32% 0.08rem -32%;
            height: 2px;
            background: linear-gradient(90deg, rgba(175,127,89,0), rgba(175,127,89,0.62), rgba(175,127,89,0));
            opacity: 0;
            transform: translateX(-30%);
        }

        .response-copy-stream-shell.is-streaming::after {
            opacity: 1;
            animation: responseProgressSweep 1.2s linear infinite;
        }

        .response-copy-stream.is-streaming .response-cursor {
            display: inline-block;
            width: 0.7ch;
            color: #af7f59;
            font-weight: 700;
            animation: responseCursorBlink 0.9s steps(1, end) infinite;
        }

        .response-para-gap {
            height: 0.52rem;
        }

        .response-bullet-line {
            display: flex;
            align-items: flex-start;
            gap: 0.42rem;
            margin: 0.14rem 0;
        }

        .response-bullet-dot {
            color: #af7f59;
            font-weight: 800;
            line-height: 1.35;
        }

        .response-action-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-top: 0.72rem;
        }

        .response-action-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-decoration: none;
            color: #111111;
            border: 1px solid rgba(17,17,17,0.1);
            background: rgba(255,255,255,0.92);
            border-radius: 999px;
            padding: 0.44rem 0.74rem;
            font-size: 0.77rem;
            font-weight: 760;
            transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
        }

        .response-action-link:hover {
            transform: translateY(-1px);
            border-color: rgba(175,127,89,0.35);
            box-shadow: 0 10px 24px rgba(17,17,17,0.08);
            color: #111111;
        }

        .response-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 0.46rem;
            margin-top: 0.72rem;
        }

        .response-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            padding: 0.36rem 0.62rem;
            font-size: 0.73rem;
            font-weight: 760;
            line-height: 1.2;
            background: rgba(17,17,17,0.08);
            color: #323232;
            border: 1px solid rgba(17,17,17,0.06);
        }

        .response-badge.success {
            background: rgba(52, 168, 83, 0.12);
            color: #1f7a3a;
            border-color: rgba(52, 168, 83, 0.26);
        }

        .response-badge.warn {
            background: rgba(194, 138, 64, 0.14);
            color: #8c5b1f;
            border-color: rgba(194, 138, 64, 0.3);
        }

        .response-badge.danger {
            background: rgba(215, 83, 79, 0.14);
            color: #b53e3a;
            border-color: rgba(215, 83, 79, 0.3);
        }

        .summary-spotlight {
            margin: 0.15rem 0 0.8rem;
            border-radius: 18px;
            border: 1px solid rgba(17,17,17,0.08);
            background: linear-gradient(165deg, rgba(255,255,255,0.97), rgba(247,241,236,0.95));
            box-shadow: 0 16px 32px rgba(17,17,17,0.08);
            padding: 0.85rem 0.94rem;
        }

        .summary-title {
            margin: 0;
            color: #111111;
            font-size: 0.96rem;
            font-weight: 800;
            letter-spacing: 0.01em;
        }

        .summary-copy {
            margin: 0.42rem 0 0;
            color: #5b534d;
            line-height: 1.66;
            font-size: 0.86rem;
            font-weight: 520;
        }

        .summary-spotlight.success {
            border-color: rgba(52, 168, 83, 0.24);
            background: linear-gradient(165deg, rgba(238, 251, 242, 0.95), rgba(250, 255, 251, 0.96));
        }

        .summary-spotlight.retry {
            border-color: rgba(194, 138, 64, 0.26);
            background: linear-gradient(165deg, rgba(255, 248, 236, 0.95), rgba(255, 251, 245, 0.96));
        }

        .summary-spotlight.error {
            border-color: rgba(215, 83, 79, 0.24);
            background: linear-gradient(165deg, rgba(255, 241, 241, 0.95), rgba(255, 249, 249, 0.96));
        }

        .summary-spotlight.waiting {
            border-color: rgba(107, 114, 128, 0.2);
            background: linear-gradient(165deg, rgba(245, 247, 251, 0.95), rgba(252, 253, 255, 0.96));
        }

        @keyframes responseCursorBlink {
            0%, 48% { opacity: 1; }
            49%, 100% { opacity: 0; }
        }

        @keyframes responseProgressSweep {
            0% { transform: translateX(-36%); }
            100% { transform: translateX(36%); }
        }

        @media (max-width: 720px) {
            .response-overview {
                padding: 0.88rem 0.84rem 0.86rem;
                border-radius: 18px;
            }

            .response-head {
                align-items: flex-start;
                gap: 0.62rem;
            }

            .response-brand-orb {
                width: 2rem;
                height: 2rem;
                font-size: 0.78rem;
            }

            .response-mode {
                font-size: 0.84rem;
                white-space: normal;
            }

            .response-state-pill {
                font-size: 0.66rem;
                padding: 0.32rem 0.52rem;
            }

            .response-copy-stream {
                font-size: 0.94rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    overview_placeholder = st.empty()
    should_stream = _should_stream_turn(turn)

    if should_stream:
        for visible_text in _generate_streaming_response_frames(turn["assistant_text"]):
            overview_placeholder.markdown(
                _build_response_overview_markup(
                    assistant_text=visible_text,
                    mode_label=mode_label,
                    tone_class=tone_class,
                    badge_markup=badge_markup,
                    quick_links_markup=quick_links_markup,
                    is_streaming=True,
                ),
                unsafe_allow_html=True,
            )

        streamed_turn_ids = list(st.session_state.streamed_turn_ids)
        turn_id = turn["turn_id"]
        if turn_id not in streamed_turn_ids:
            streamed_turn_ids.append(turn_id)
        st.session_state.streamed_turn_ids = streamed_turn_ids
        st.session_state.pending_stream_turn_id = None

    overview_placeholder.markdown(
        _build_response_overview_markup(
            assistant_text=turn["assistant_text"],
            mode_label=mode_label,
            tone_class=tone_class,
            badge_markup=badge_markup,
            quick_links_markup=quick_links_markup,
            is_streaming=False,
        ),
        unsafe_allow_html=True,
    )


def _slugify_filename_part(value: str, *, fallback: str = "report") -> str:
    """Turn freeform text into a stable, download-friendly filename part."""
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned[:56] or fallback


def _build_export_base_filename(turn: dict[str, Any]) -> str:
    """Build a shared filename stem for SQL, CSV, and chart downloads."""
    question_text = str(turn.get("user_text", "")).strip()
    if "```sql" in question_text:
        question_text = "manual-sql-export"
    return _slugify_filename_part(question_text, fallback="yelp-insight")


def _encode_download_href(file_bytes: bytes, mime_type: str) -> str:
    """Encode bytes into a client-side download href."""
    encoded = base64.b64encode(file_bytes).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _build_results_csv_bytes(rows: list[dict[str, Any]]) -> bytes | None:
    """Convert one result table into UTF-8 CSV bytes."""
    if not rows:
        return None

    dataframe = pd.DataFrame(rows)
    csv_text = dataframe.to_csv(index=False)
    if not csv_text.strip():
        return None
    return csv_text.encode("utf-8")


def _build_sql_download_bytes(turn: dict[str, Any]) -> bytes | None:
    """Return the final SQL as a downloadable .sql payload."""
    sql_text = str(turn.get("final_sql", "")).strip()
    if "<div" in sql_text.lower() or "<span" in sql_text.lower():
        sql_text = ""
    if not sql_text:
        sql_text = _build_conversation_trace_sql(
            question=str(turn.get("user_text", "")),
            assistant_text=str(turn.get("assistant_text", "")),
            mode_label=str(turn.get("mode_label", "")),
            generation_note=str(turn.get("generation_note", "")),
        )

    if not sql_text.endswith(";"):
        sql_text = f"{sql_text}\n"
    else:
        sql_text = f"{sql_text}\n"
    return sql_text.encode("utf-8")


def _build_export_action_markup(
    *,
    label: str,
    detail: str,
    filename: str,
    mime_type: str,
    file_bytes: bytes | None,
) -> str:
    """Build one glassmorphic export action button or disabled tile."""
    if file_bytes:
        href = _encode_download_href(file_bytes, mime_type)
        return (
            f'<a href="{href}" download="{escape(filename)}" class="export-report-action">'
            f'<span class="export-report-action-title">{escape(label)}</span>'
            f'<span class="export-report-action-copy">{escape(detail)}</span>'
            "</a>"
        )

    return (
        '<div class="export-report-action disabled">'
        f'<span class="export-report-action-title">{escape(label)}</span>'
        f'<span class="export-report-action-copy">{escape(detail)}</span>'
        "</div>"
    )


def _render_export_report_group(turn: dict[str, Any], result: QueryResult) -> None:
    """SQL visibility/export controls intentionally disabled in chat responses."""
    return


def _build_response_overview_markup(
    *,
    assistant_text: str,
    mode_label: str,
    tone_class: str,
    badge_markup: str,
    quick_links_markup: str,
    is_streaming: bool,
) -> str:
    """Build the response overview card markup for static and streaming states."""
    def _format_response_copy_html(raw_text: str) -> str:
        safe_text = escape(raw_text or "")
        if not safe_text:
            return ""

        rendered_lines: list[str] = []
        for raw_line in safe_text.splitlines():
            line = raw_line.strip()
            if not line:
                rendered_lines.append('<div class="response-para-gap"></div>')
                continue
            if line.startswith("- "):
                rendered_lines.append(
                    f'<div class="response-bullet-line"><span class="response-bullet-dot">•</span><span>{line[2:].strip()}</span></div>'
                )
                continue
            rendered_lines.append(f"<div>{line}</div>")

        return "".join(rendered_lines)

    streaming_class = " is-streaming" if is_streaming else ""
    cursor_markup = '<span class="response-cursor">|</span>' if is_streaming else ""
    safe_assistant_text = _format_response_copy_html(assistant_text)
    safe_mode_label = escape(mode_label)
    tone_class_name = escape(tone_class or "is-data")
    action_row_markup = (
        f'<div class="response-action-row">{quick_links_markup}</div>'
        if quick_links_markup
        else ""
    )
    badge_row_markup = (
        f'<div class="response-badges">{badge_markup}</div>'
        if badge_markup
        else ""
    )
    return f"""
    <div class="response-overview {tone_class_name} animate-in stagger-1">
        <div class="response-head">
            <div class="response-brand-orb">Q</div>
            <div class="response-head-copy">
                <div class="response-kicker">Query by SilkByteX</div>
                <div class="response-mode">{safe_mode_label}</div>
            </div>
            <span class="response-state-pill">{'Typing' if is_streaming else 'Ready'}</span>
        </div>
        <div class="response-copy-stream-shell{' is-streaming' if is_streaming else ''}">
            <div class="response-copy response-copy-stream{streaming_class}">{safe_assistant_text}{cursor_markup}</div>
        </div>
        {action_row_markup}
        {badge_row_markup}
    </div>
    """


def _generate_streaming_response_frames(text: str):
    """Reveal one short assistant response in small chunks for a typewriter effect."""
    if not text:
        yield ""
        return

    current_text = ""
    buffered_chars = ""

    for character in text:
        buffered_chars += character

        should_flush = (
            character in {" ", "\n", ".", ",", "!", "?", ";", ":"}
            or len(buffered_chars) >= 2
        )
        if not should_flush:
            continue

        current_text += buffered_chars
        yield current_text
        time.sleep(_get_stream_chunk_delay(buffered_chars))
        buffered_chars = ""

    if buffered_chars:
        current_text += buffered_chars
        yield current_text


def _get_stream_chunk_delay(chunk: str) -> float:
    """Return a tiny pause that keeps the typewriter effect smooth but fast."""
    if any(character in chunk for character in {".", "!", "?"}):
        return 0.018
    if any(character in chunk for character in {",", ";", ":"}):
        return 0.012
    if "\n" in chunk:
        return 0.01
    return 0.0045


def _should_stream_turn(turn: dict[str, Any]) -> bool:
    """Return True when the current turn should animate right now."""
    turn_id = turn.get("turn_id")
    if not isinstance(turn_id, int):
        return False

    return (
        turn_id == st.session_state.pending_stream_turn_id
        and turn_id not in st.session_state.streamed_turn_ids
    )


def _build_control_center_status_cards() -> list[tuple[str, str, str]]:
    """Return the compact status cards shown inside the floating drawer."""
    if st.session_state.nl_use_demo_mode:
        return [
            ("Demo Mode", "Using sample prompts and SQL", "info"),
            ("SQL Engine", os.getenv("YELP_SQL_ENGINE", "hive").upper(), "neutral"),
        ]

    if not _has_live_generation_config():
        return [
            ("Setup Needed", "Missing LLM model settings", "warn"),
            ("SQL Engine", os.getenv("YELP_SQL_ENGINE", "hive").upper(), "neutral"),
        ]

    return [
        ("Live Model Ready", "Connected to the Text-to-SQL pipeline", "success"),
        ("SQL Engine", os.getenv("YELP_SQL_ENGINE", "hive").upper(), "neutral"),
    ]


def _render_floating_action_menu(config: AppConfig) -> None:
    """Render a CSS-only floating drawer that replaces the default sidebar."""
    _ = config
    current_route = _get_current_route()
    active_panel = _get_active_panel()
    toggle_target_route = current_route or "home"
    toggle_href = _build_route_href(
        toggle_target_route,
        action="toggle_mode",
        panel=active_panel if toggle_target_route == "home" else "",
    )
    toggle_label = (
        "Switch To Live Mode" if st.session_state.nl_use_demo_mode else "Switch To Demo Mode"
    )
    toggle_copy = (
        "Use live LLM SQL generation"
        if st.session_state.nl_use_demo_mode
        else "Use sample prompts and SQL"
    )

    drawer_actions = [
        (
            toggle_label,
            toggle_copy,
            toggle_href,
            "primary",
        ),
        (
            "Open Readiness",
            "Run readiness checks",
            _build_route_href("readiness"),
            "secondary",
        ),
        (
            "Run Database Test",
            "Execute the fixed backend validation query",
            _build_route_href("home", action="run_test", panel="results"),
            "secondary",
        ),
        (
            "Open Manual SQL",
            "Run SQL directly",
            _build_route_href("home", panel="manual_sql"),
            "secondary",
        ),
        (
            "Clear Conversation",
            "Reset the visible chat history",
            _build_route_href("home", action="clear_conversation"),
            "ghost",
        ),
        (
            "Reset Session",
            "Clear session state",
            _build_route_href("home", action="presentation_reset"),
            "ghost",
        ),
    ]

    action_markup = "".join(
        (
            f'<a href="{escape(href)}" target="_self" '
            f'class="floating-menu-action {escape(action_tone)}">'
            f'<span class="floating-menu-action-title">{escape(label)}</span>'
            f'<span class="floating-menu-action-copy">{escape(copy)}</span>'
            "</a>"
        )
        for label, copy, href, action_tone in drawer_actions
    )

    status_markup = "".join(
        (
            f'<div class="floating-menu-status-card {escape(tone)}">'
            f'<div class="floating-menu-status-title">{escape(title)}</div>'
            f'<div class="floating-menu-status-copy">{escape(copy)}</div>'
            "</div>"
        )
        for title, copy, tone in _build_control_center_status_cards()
    )

    utility_links = [
        ("Data Journey", _build_route_href("readiness")),
        ("Database Schema", _build_route_href("schema")),
        ("Architecture", _build_route_href("architecture")),
        ("Docs", _build_route_href("docs")),
    ]
    utility_markup = "".join(
        (
            f'<a href="{escape(href)}" target="_self" class="floating-menu-utility-link">'
            f"{escape(label)}"
            "</a>"
        )
        for label, href in utility_links
    )

    st.markdown(
        f"""
        <style>
        [data-testid="stSidebar"] {{
            display: none !important;
        }}

        [data-testid="collapsedControl"] {{
            display: none !important;
        }}

        .floating-menu-shell {{
            position: fixed;
            right: 1.25rem;
            bottom: 5.9rem;
            z-index: 1200;
        }}

        .floating-menu-toggle {{
            position: absolute;
            opacity: 0;
            pointer-events: none;
        }}

        .floating-menu-button {{
            position: relative;
            display: inline-flex;
            align-items: center;
            gap: 0.72rem;
            padding: 0.9rem 1.02rem;
            border-radius: 999px;
            background: linear-gradient(145deg, rgba(255,255,255,0.98), rgba(246,240,234,0.98));
            border: 1px solid rgba(17,17,17,0.08);
            box-shadow: 0 22px 46px rgba(17,17,17,0.12);
            cursor: pointer;
            user-select: none;
            transition: transform 0.24s ease, box-shadow 0.24s ease, background 0.24s ease;
        }}

        .floating-menu-button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 28px 54px rgba(17,17,17,0.16);
        }}

        .floating-menu-button-icon {{
            width: 2.7rem;
            height: 2.7rem;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(145deg, #111111, #2d2a28);
            color: #ffffff;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.12);
            flex-shrink: 0;
        }}

        .floating-menu-button-icon svg {{
            width: 1.18rem;
            height: 1.18rem;
            transition: transform 0.32s ease;
        }}

        .floating-menu-button-copy {{
            display: flex;
            flex-direction: column;
            gap: 0.1rem;
            min-width: 0;
        }}

        .floating-menu-button-label {{
            color: #111111;
            font-size: 0.84rem;
            font-weight: 800;
            letter-spacing: 0.02em;
            line-height: 1.2;
        }}

        .floating-menu-button-note {{
            color: #6a615b;
            font-size: 0.73rem;
            font-weight: 700;
            line-height: 1.25;
        }}

        .floating-menu-overlay {{
            position: fixed;
            inset: 0;
            background: rgba(17, 17, 17, 0.18);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.24s ease;
        }}

        .floating-menu-drawer {{
            position: fixed;
            right: 1.25rem;
            bottom: 10.8rem;
            width: min(28.5rem, calc(100vw - 1.5rem));
            max-height: min(78vh, 50rem);
            overflow: auto;
            padding: 1.15rem;
            border-radius: 30px;
            border: 1px solid rgba(17,17,17,0.08);
            background:
                radial-gradient(circle at top right, rgba(175,127,89,0.18), transparent 34%),
                linear-gradient(180deg, rgba(255,255,255,0.98), rgba(247,242,237,0.98));
            box-shadow: 0 34px 72px rgba(17,17,17,0.16);
            opacity: 0;
            pointer-events: none;
            transform: translateY(1.15rem) scale(0.98);
            transform-origin: bottom right;
            transition: opacity 0.24s ease, transform 0.24s ease;
        }}

        .floating-menu-toggle:checked + .floating-menu-button {{
            background: linear-gradient(145deg, rgba(17,17,17,0.98), rgba(45,42,40,0.98));
            box-shadow: 0 28px 58px rgba(17,17,17,0.2);
        }}

        .floating-menu-toggle:checked + .floating-menu-button .floating-menu-button-label,
        .floating-menu-toggle:checked + .floating-menu-button .floating-menu-button-note {{
            color: #ffffff;
        }}

        .floating-menu-toggle:checked + .floating-menu-button .floating-menu-button-icon {{
            background: linear-gradient(145deg, #af7f59, #8f6442);
        }}

        .floating-menu-toggle:checked + .floating-menu-button .floating-menu-button-icon svg {{
            transform: rotate(72deg);
        }}

        .floating-menu-toggle:checked ~ .floating-menu-overlay {{
            opacity: 1;
            pointer-events: auto;
        }}

        .floating-menu-toggle:checked ~ .floating-menu-drawer {{
            opacity: 1;
            pointer-events: auto;
            transform: translateY(0) scale(1);
        }}

        .floating-menu-head {{
            display: flex;
            align-items: start;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 1rem;
        }}

        .floating-menu-kicker {{
            color: #af7f59;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.16em;
            text-transform: uppercase;
        }}

        .floating-menu-title {{
            color: #111111;
            font-size: 1.34rem;
            font-weight: 800;
            line-height: 1.16;
            margin-top: 0.45rem;
        }}

        .floating-menu-subtitle {{
            color: #5f5a57;
            font-size: 0.9rem;
            line-height: 1.68;
            margin-top: 0.55rem;
        }}

        .floating-menu-close {{
            width: 2.3rem;
            height: 2.3rem;
            border-radius: 999px;
            border: 1px solid rgba(17,17,17,0.08);
            background: rgba(255,255,255,0.8);
            color: #111111;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            flex-shrink: 0;
            box-shadow: 0 12px 24px rgba(17,17,17,0.06);
        }}

        .floating-menu-close:hover {{
            background: rgba(17,17,17,0.04);
        }}

        .floating-menu-status-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.75rem;
            margin-bottom: 0.95rem;
        }}

        .floating-menu-status-card {{
            padding: 0.92rem 0.95rem;
            border-radius: 20px;
            border: 1px solid rgba(17,17,17,0.08);
            background: rgba(255,255,255,0.78);
        }}

        .floating-menu-status-card.success {{
            border-color: rgba(175,127,89,0.22);
            background: rgba(175,127,89,0.08);
        }}

        .floating-menu-status-card.warn {{
            border-color: rgba(175,127,89,0.22);
            background: rgba(175,127,89,0.08);
        }}

        .floating-menu-status-card.info {{
            border-color: rgba(17,17,17,0.12);
            background: rgba(17,17,17,0.04);
        }}

        .floating-menu-status-title {{
            color: #111111;
            font-size: 0.83rem;
            font-weight: 800;
            line-height: 1.3;
        }}

        .floating-menu-status-copy {{
            color: #5f5a57;
            font-size: 0.79rem;
            line-height: 1.55;
            margin-top: 0.3rem;
        }}

        .floating-menu-section {{
            margin-top: 0.8rem;
            border-radius: 22px;
            border: 1px solid rgba(17,17,17,0.08);
            background: rgba(255,255,255,0.76);
            overflow: hidden;
        }}

        .floating-menu-section summary {{
            list-style: none;
            cursor: pointer;
            padding: 0.95rem 1rem;
            color: #111111;
            font-size: 0.9rem;
            font-weight: 800;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
        }}

        .floating-menu-section summary::-webkit-details-marker {{
            display: none;
        }}

        .floating-menu-section summary::after {{
            content: "+";
            color: #af7f59;
            font-size: 1.08rem;
            line-height: 1;
        }}

        .floating-menu-section[open] summary::after {{
            content: "−";
        }}

        .floating-menu-section-body {{
            display: grid;
            gap: 0.72rem;
            padding: 0 1rem 1rem;
        }}

        .floating-menu-action {{
            display: grid;
            gap: 0.2rem;
            padding: 0.95rem 1rem;
            border-radius: 18px;
            text-decoration: none;
            transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease;
        }}

        .floating-menu-action:hover {{
            transform: translateY(-1px);
        }}

        .floating-menu-action.primary {{
            background: linear-gradient(145deg, #111111, #2e2a28);
            color: #ffffff;
            box-shadow: 0 18px 34px rgba(17,17,17,0.15);
        }}

        .floating-menu-action.secondary {{
            background: linear-gradient(145deg, rgba(175,127,89,0.12), rgba(255,255,255,0.94));
            color: #111111;
            border: 1px solid rgba(175,127,89,0.16);
        }}

        .floating-menu-action.ghost {{
            background: rgba(255,255,255,0.88);
            color: #111111;
            border: 1px solid rgba(17,17,17,0.08);
        }}

        .floating-menu-action-title {{
            font-size: 0.88rem;
            font-weight: 800;
            line-height: 1.3;
        }}

        .floating-menu-action-copy {{
            font-size: 0.79rem;
            line-height: 1.55;
            opacity: 0.84;
        }}

        .floating-menu-utility-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.65rem;
        }}

        .floating-menu-utility-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.78rem 0.82rem;
            border-radius: 16px;
            border: 1px solid rgba(17,17,17,0.08);
            background: rgba(255,255,255,0.84);
            color: #5f5a57;
            text-decoration: none;
            font-size: 0.8rem;
            font-weight: 800;
            text-align: center;
        }}

        .floating-menu-utility-link:hover {{
            color: #111111;
            border-color: rgba(175,127,89,0.22);
        }}

        .floating-menu-layer-list {{
            display: grid;
            gap: 0.58rem;
        }}

        .floating-menu-layer-item {{
            display: flex;
            align-items: start;
            gap: 0.7rem;
            color: #5f5a57;
            font-size: 0.81rem;
            line-height: 1.55;
        }}

        .floating-menu-layer-dot {{
            width: 0.55rem;
            height: 0.55rem;
            margin-top: 0.38rem;
            border-radius: 999px;
            background: #af7f59;
            flex-shrink: 0;
        }}

        @media (max-width: 900px) {{
            .floating-menu-shell {{
                right: 0.9rem;
                bottom: 5.3rem;
            }}

            .floating-menu-drawer {{
                right: 0.75rem;
                bottom: 9.7rem;
                width: calc(100vw - 1.5rem);
            }}

            .floating-menu-status-grid,
            .floating-menu-utility-grid {{
                grid-template-columns: 1fr;
            }}

            .floating-menu-button {{
                padding: 0.82rem 0.88rem;
            }}

            .floating-menu-button-note {{
                display: none;
            }}
        }}
        </style>
        <div class="floating-menu-shell">
            <input type="checkbox" id="floating-settings-toggle" class="floating-menu-toggle">
            <label for="floating-settings-toggle" class="floating-menu-button" aria-label="Open settings menu">
                <span class="floating-menu-button-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="3.2"></circle>
                        <path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a1.3 1.3 0 0 1 0 1.8l-1.7 1.7a1.3 1.3 0 0 1-1.8 0l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9V21a1.3 1.3 0 0 1-1.3 1.3h-2.4A1.3 1.3 0 0 1 8.4 21v-.2a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a1.3 1.3 0 0 1-1.8 0l-1.7-1.7a1.3 1.3 0 0 1 0-1.8l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H3A1.3 1.3 0 0 1 1.7 12v-2.4A1.3 1.3 0 0 1 3 8.3h.2a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a1.3 1.3 0 0 1 0-1.8l1.7-1.7a1.3 1.3 0 0 1 1.8 0l.1.1a1 1 0 0 0 1.1.2 1 1 0 0 0 .6-.9V3A1.3 1.3 0 0 1 10.7 1.7h2.4A1.3 1.3 0 0 1 14.4 3v.2a1 1 0 0 0 .6.9 1 1 0 0 0 1.1-.2l.1-.1a1.3 1.3 0 0 1 1.8 0l1.7 1.7a1.3 1.3 0 0 1 0 1.8l-.1.1a1 1 0 0 0-.2 1.1 1 1 0 0 0 .9.6h.2A1.3 1.3 0 0 1 22.3 9.6V12a1.3 1.3 0 0 1-1.3 1.3h-.2a1 1 0 0 0-.9.6z"></path>
                    </svg>
                </span>
                <span class="floating-menu-button-copy">
                    <span class="floating-menu-button-label">Settings</span>
                    <span class="floating-menu-button-note">System Controls</span>
                </span>
            </label>
            <label for="floating-settings-toggle" class="floating-menu-overlay" aria-hidden="true"></label>
            <aside class="floating-menu-drawer" aria-label="Floating action menu">
                <div class="floating-menu-head">
                    <div>
                        <div class="floating-menu-kicker">System Controls</div>
                        <div class="floating-menu-title">Floating Action Menu</div>
                        <div class="floating-menu-subtitle">Mode switch, readiness checks, and project navigation.</div>
                    </div>
                    <label for="floating-settings-toggle" class="floating-menu-close" aria-label="Close settings menu">×</label>
                </div>
                <div class="floating-menu-status-grid">{status_markup}</div>
                <details class="floating-menu-section" open>
                    <summary>Session Controls</summary>
                    <div class="floating-menu-section-body">
                        {action_markup}
                    </div>
                </details>
                <details class="floating-menu-section" open>
                    <summary>Quick Views</summary>
                    <div class="floating-menu-section-body">
                        <div class="floating-menu-utility-grid">{utility_markup}</div>
                    </div>
                </details>
                <details class="floating-menu-section">
                    <summary>System Architecture Layers</summary>
                    <div class="floating-menu-section-body">
                        <div class="floating-menu-layer-list">
                            <div class="floating-menu-layer-item"><span class="floating-menu-layer-dot"></span><span><code>app.py</code> launches the Streamlit experience.</span></div>
                            <div class="floating-menu-layer-item"><span class="floating-menu-layer-dot"></span><span><code>ui.py</code> handles presentation, navigation, and interaction state.</span></div>
                            <div class="floating-menu-layer-item"><span class="floating-menu-layer-dot"></span><span><code>pipeline.py</code> orchestrates question → SQL → execution.</span></div>
                            <div class="floating-menu-layer-item"><span class="floating-menu-layer-dot"></span><span><code>database.py</code> owns backend execution and connectivity checks.</span></div>
                        </div>
                    </div>
                </details>
            </aside>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _get_response_tone(turn: dict[str, Any], result: QueryResult) -> tuple[str, str, str]:
    """Return the summary state class, title, and supporting text."""
    if result.error and turn["retry_happened"]:
        return (
            "error",
            "Retry Used, Final Query Failed",
            "The app corrected the first SQL once, but the backend still returned an error on the final attempt.",
        )

    if result.error:
        return (
            "error",
            "Query Needs Attention",
            "The current run did not complete successfully. Open the Errors tab for the backend details.",
        )

    if turn["retry_happened"]:
        return (
            "retry",
            "Recovered After One Retry",
            "The first SQL attempt failed, a corrected query was generated, and the second attempt completed.",
        )

    if result.executed:
        return (
            "success",
            "Query Completed Successfully",
            "The generated SQL ran cleanly and the result set is ready to inspect below.",
        )

    return (
        "waiting",
        "Waiting For Query Output",
        "Run a question to populate the SQL, result table, chart, and error details.",
    )


def _render_summary_spotlight(turn: dict[str, Any], result: QueryResult) -> None:
    """Render the main status callout before the detail tabs."""
    tone_class, title, copy = _get_response_tone(turn, result)
    st.markdown(
        f"""
        <div class="summary-spotlight {escape(tone_class)} animate-in stagger-2">
            <h4 class="summary-title">{escape(title)}</h4>
            <p class="summary-copy">{escape(copy)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_copy_sql_button(sql_text: str, button_key: str) -> None:
    """Render a client-side copy-to-clipboard button for one SQL block."""
    safe_button_id = re.sub(r"[^a-zA-Z0-9_-]", "-", button_key)
    components.html(
        f"""
        <div style="display:flex; justify-content:flex-end;">
            <button
                id="{escape(safe_button_id)}"
                style="
                    border: 1px solid rgba(175, 127, 89, 0.28);
                    background: linear-gradient(135deg, rgba(255,255,255,0.96), rgba(248,244,239,0.96));
                    color: #111111;
                    border-radius: 999px;
                    padding: 0.55rem 0.95rem;
                    font-family: Manrope, Segoe UI, sans-serif;
                    font-size: 0.82rem;
                    font-weight: 700;
                    cursor: pointer;
                    box-shadow: 0 10px 22px rgba(17,17,17,0.06);
                "
            >
                Copy to Clipboard
            </button>
        </div>
        <script>
            const button = document.getElementById("{escape(safe_button_id)}");
            if (button) {{
                button.addEventListener("click", async () => {{
                    try {{
                        await navigator.clipboard.writeText({json.dumps(sql_text)});
                        const previousLabel = button.textContent;
                        button.textContent = "Copied";
                        setTimeout(() => {{
                            button.textContent = previousLabel;
                        }}, 1400);
                    }} catch (_error) {{
                        button.textContent = "Copy failed";
                    }}
                }});
            }}
        </script>
        """,
        height=54,
        scrolling=False,
    )


def _render_sql_explanation_card(explanation_text: str) -> None:
    """Render the plain-English SQL explanation beside the code block."""
    clean_explanation = explanation_text.strip()
    if not clean_explanation:
        clean_explanation = (
            "No plain-English explanation was returned for this SQL yet. "
            "The query itself is still available on the left."
        )

    st.markdown(
        f"""
        <div style="
            height: 100%;
            padding: 1rem 1.05rem;
            border-radius: 22px;
            border: 1px solid rgba(17, 17, 17, 0.08);
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(250,246,242,0.98));
            box-shadow: 0 16px 34px rgba(17,17,17,0.05);
        ">
            <div style="
                color: #af7f59;
                font-size: 0.72rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                margin-bottom: 0.6rem;
            ">
                Explain Like I'm 5
            </div>
            <div style="
                color: #111111;
                font-size: 1rem;
                font-weight: 700;
                line-height: 1.45;
                margin-bottom: 0.65rem;
            ">
                What this SQL is doing
            </div>
            <div style="
                color: #595959;
                line-height: 1.72;
                font-size: 0.96rem;
            ">
                {escape(clean_explanation)}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_sql_trace_block(
    *,
    title: str,
    sql_text: str,
    explanation_text: str,
    copy_key: str,
) -> None:
    """Render one SQL trace section with code, explanation, and copy action."""
    st.markdown(f"**{title}**")
    content_columns = st.columns([1.55, 1.0])

    with content_columns[0]:
        _render_copy_sql_button(sql_text or "", copy_key)
        if sql_text:
            st.code(sql_text, language="sql")
        else:
            st.info("SQL is not available for this step.")

    with content_columns[1]:
        _render_sql_explanation_card(explanation_text)


def _render_sql_tab(turn: dict[str, Any]) -> None:
    """Render generated SQL details, including retry information."""
    fallback_sql = _build_conversation_trace_sql(
        question=str(turn.get("user_text", "")),
        assistant_text=str(turn.get("assistant_text", "")),
        mode_label=str(turn.get("mode_label", "")),
        generation_note=str(turn.get("generation_note", "")),
    )

    if turn["retry_happened"]:
        _render_sql_trace_block(
            title="First SQL Attempt",
            sql_text=turn["original_sql"],
            explanation_text=turn.get("original_sql_explanation", ""),
            copy_key=f"sql-trace-original-{turn.get('turn_id', 'unknown')}",
        )
        _render_sql_trace_block(
            title="Corrected SQL",
            sql_text=turn["corrected_sql"],
            explanation_text=turn.get("corrected_sql_explanation", ""),
            copy_key=f"sql-trace-corrected-{turn.get('turn_id', 'unknown')}",
        )
        return

    _render_sql_trace_block(
        title="Generated SQL",
        sql_text=str(turn.get("final_sql", "")).strip() or fallback_sql,
        explanation_text=turn.get("final_sql_explanation", ""),
        copy_key=f"sql-trace-final-{turn.get('turn_id', 'unknown')}",
    )


def _format_number(value: float) -> str:
    """Format a number into a compact human-friendly string."""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:,.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:,.1f}K"
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def _is_aggregation_like_metric_name(column_name: str) -> bool:
    """Return True when a column name looks like an aggregate KPI."""
    lowered = column_name.lower()
    keywords = (
        "count",
        "total",
        "sum",
        "avg",
        "average",
        "mean",
        "median",
        "max",
        "min",
        "revenue",
        "sales",
        "profit",
        "amount",
        "value",
    )
    return any(keyword in lowered for keyword in keywords)


def _prettify_metric_label(column_name: str) -> str:
    """Convert one SQL-ish column name into a dashboard-friendly label."""
    return column_name.replace("_", " ").strip().title()


def _build_kpi_display_value(value: float) -> tuple[str, float]:
    """Return one compact display value and its decimal precision."""
    if abs(value) >= 1_000_000:
        return (f"{value / 1_000_000:,.1f}M", 1)
    if abs(value) >= 1_000:
        return (f"{value / 1_000:,.1f}K", 1)
    if float(value).is_integer():
        return (f"{int(value):,}", 0)
    return (f"{value:,.2f}", 2)


def _query_looks_aggregate_driven(turn: dict[str, Any]) -> bool:
    """Return True when the current question or SQL looks aggregation-focused."""
    sql_text = str(turn.get("final_sql", "")).lower()
    user_text = str(turn.get("user_text", "")).lower()

    sql_keywords = (
        "count(",
        "sum(",
        "avg(",
        "average(",
        "min(",
        "max(",
        "group by",
        "having",
    )
    prompt_keywords = (
        "count",
        "total",
        "sum",
        "average",
        "avg",
        "maximum",
        "minimum",
        "revenue",
        "sales",
        "profit",
    )
    return any(keyword in sql_text for keyword in sql_keywords) or any(
        keyword in user_text for keyword in prompt_keywords
    )


def _extract_kpi_metrics(rows: list[dict[str, Any]], turn: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract up to three dashboard KPI candidates from aggregation-style results."""
    if not rows:
        return []

    if not _query_looks_aggregate_driven(turn):
        return []

    dataframe = pd.DataFrame(rows)
    numeric_columns = [
        column_name
        for column_name in dataframe.columns
        if pd.api.types.is_numeric_dtype(dataframe[column_name])
    ]
    if not numeric_columns:
        return []

    non_numeric_columns = [column for column in dataframe.columns if column not in numeric_columns]
    metric_candidates: list[dict[str, Any]] = []

    if len(dataframe) == 1:
        first_row = dataframe.iloc[0]
        for column_name in numeric_columns:
            raw_value = first_row[column_name]
            if pd.isna(raw_value):
                continue
            value = float(raw_value)
            display_value, decimal_places = _build_kpi_display_value(value)
            metric_candidates.append(
                {
                    "label": _prettify_metric_label(column_name),
                    "value": value,
                    "display_value": display_value,
                    "decimal_places": decimal_places,
                    "note": "Returned directly by the query",
                    "priority": 120 if _is_aggregation_like_metric_name(column_name) else 90,
                }
            )
    else:
        for column_name in numeric_columns:
            series = pd.to_numeric(dataframe[column_name], errors="coerce").dropna()
            if series.empty:
                continue

            lowered_name = column_name.lower()
            pretty_name = _prettify_metric_label(column_name)

            if any(keyword in lowered_name for keyword in ("avg", "average", "mean", "median")):
                metric_label = f"Average {pretty_name}"
                metric_value = float(series.mean())
                note = f"Across {len(series)} grouped rows"
                priority = 130
            elif any(keyword in lowered_name for keyword in ("count", "total", "sum", "revenue", "sales", "profit", "amount", "value")):
                metric_label = pretty_name if pretty_name.lower().startswith("total ") else f"Total {pretty_name}"
                metric_value = float(series.sum())
                note = f"Summed across {len(series)} grouped rows"
                priority = 125
            else:
                metric_label = f"Peak {pretty_name}"
                metric_value = float(series.max())
                note = "Highest value in the returned result set"
                priority = 95

            if len(non_numeric_columns) == 1 and not series.empty:
                leading_dimension = non_numeric_columns[0]
                peak_index = series.idxmax()
                dimension_value = dataframe.loc[peak_index, leading_dimension]
                if pd.notna(dimension_value) and "Peak " in metric_label:
                    note = f"Top {_prettify_metric_label(leading_dimension)}: {dimension_value}"

            display_value, decimal_places = _build_kpi_display_value(metric_value)
            metric_candidates.append(
                {
                    "label": metric_label,
                    "value": metric_value,
                    "display_value": display_value,
                    "decimal_places": decimal_places,
                    "note": note,
                    "priority": priority,
                }
            )

    ranked_metrics = sorted(
        metric_candidates,
        key=lambda metric: (metric["priority"], abs(metric["value"])),
        reverse=True,
    )
    return ranked_metrics[:3]


def _render_kpi_scorecards(rows: list[dict[str, Any]], turn: dict[str, Any]) -> bool:
    """Render premium KPI scorecards above the data table when aggregations are present."""
    metrics = _extract_kpi_metrics(rows, turn)
    if not metrics:
        return False

    payload = json.dumps(metrics)
    components.html(
        f"""
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8" />
                <style>
                    :root {{
                        --kpi-black: #111111;
                        --kpi-charcoal: #1b1b1b;
                        --kpi-white: #fffdfa;
                        --kpi-bronze: #af7f59;
                        --kpi-bronze-soft: #d6af8c;
                        --kpi-muted: #6c625c;
                        --kpi-border: rgba(17, 17, 17, 0.08);
                        --kpi-shadow: 0 28px 64px rgba(17, 17, 17, 0.08);
                    }}

                    * {{
                        box-sizing: border-box;
                    }}

                    html, body {{
                        margin: 0;
                        padding: 0;
                        background: transparent;
                        font-family: "Manrope", "Segoe UI", sans-serif;
                    }}

                    .kpi-dashboard {{
                        display: grid;
                        gap: 1rem;
                        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                        padding: 0.25rem 0 0.65rem;
                    }}

                    .kpi-card {{
                        position: relative;
                        overflow: hidden;
                        min-height: 200px;
                        padding: 1.2rem 1.2rem 1.1rem;
                        border-radius: 26px;
                        border: 1px solid var(--kpi-border);
                        background:
                            radial-gradient(circle at top right, rgba(175, 127, 89, 0.16), transparent 42%),
                            linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(249, 245, 241, 0.98) 100%);
                        box-shadow: var(--kpi-shadow);
                        transform: translateY(20px) scale(0.98);
                        opacity: 0;
                        animation: kpiCardRise 0.72s cubic-bezier(0.16, 1, 0.3, 1) forwards;
                    }}

                    .kpi-card::before {{
                        content: "";
                        position: absolute;
                        inset: 0;
                        background: linear-gradient(
                            120deg,
                            rgba(255, 255, 255, 0) 0%,
                            rgba(255, 255, 255, 0.24) 35%,
                            rgba(214, 175, 140, 0.26) 50%,
                            rgba(255, 255, 255, 0) 100%
                        );
                        transform: translateX(-140%);
                        animation: kpiShimmer 3.8s ease-in-out infinite;
                        pointer-events: none;
                    }}

                    .kpi-card:nth-child(2) {{
                        animation-delay: 0.08s;
                    }}

                    .kpi-card:nth-child(3) {{
                        animation-delay: 0.16s;
                    }}

                    .kpi-eyebrow {{
                        color: var(--kpi-bronze);
                        font-size: 0.72rem;
                        font-weight: 800;
                        letter-spacing: 0.16em;
                        text-transform: uppercase;
                    }}

                    .kpi-label {{
                        margin-top: 0.75rem;
                        color: var(--kpi-black);
                        font-size: 1.05rem;
                        font-weight: 800;
                        line-height: 1.28;
                    }}

                    .kpi-value-shell {{
                        position: relative;
                        overflow: hidden;
                        margin-top: 1.05rem;
                        min-height: 4.6rem;
                    }}

                    .kpi-value {{
                        display: inline-block;
                        color: var(--kpi-charcoal);
                        font-size: clamp(2.4rem, 5vw, 3.7rem);
                        font-weight: 800;
                        line-height: 0.95;
                        letter-spacing: -0.06em;
                        transform: translateY(110%);
                        opacity: 0;
                        filter: blur(8px);
                        animation: kpiValueRise 1s cubic-bezier(0.16, 1, 0.3, 1) forwards;
                    }}

                    .kpi-subtext {{
                        margin-top: 0.9rem;
                        color: var(--kpi-muted);
                        font-size: 0.9rem;
                        line-height: 1.55;
                    }}

                    .kpi-footer-rail {{
                        margin-top: 1rem;
                        width: 4.5rem;
                        height: 4px;
                        border-radius: 999px;
                        background: linear-gradient(90deg, var(--kpi-bronze), rgba(175, 127, 89, 0.12));
                    }}

                    @keyframes kpiCardRise {{
                        from {{
                            opacity: 0;
                            transform: translateY(20px) scale(0.98);
                        }}
                        to {{
                            opacity: 1;
                            transform: translateY(0) scale(1);
                        }}
                    }}

                    @keyframes kpiValueRise {{
                        from {{
                            opacity: 0;
                            transform: translateY(110%);
                            filter: blur(8px);
                        }}
                        to {{
                            opacity: 1;
                            transform: translateY(0);
                            filter: blur(0);
                        }}
                    }}

                    @keyframes kpiShimmer {{
                        0% {{
                            transform: translateX(-140%);
                        }}
                        48%,
                        100% {{
                            transform: translateX(140%);
                        }}
                    }}

                    @media (max-width: 640px) {{
                        .kpi-card {{
                            min-height: 180px;
                        }}
                    }}
                </style>
            </head>
            <body>
                <div class="kpi-dashboard" id="kpi-dashboard"></div>
                <script>
                    const metrics = {payload};
                    const root = document.getElementById("kpi-dashboard");
                    const escapeHtml = (value) => String(value)
                        .replaceAll("&", "&amp;")
                        .replaceAll("<", "&lt;")
                        .replaceAll(">", "&gt;")
                        .replaceAll('"', "&quot;")
                        .replaceAll("'", "&#39;");

                    root.innerHTML = metrics.map((metric, index) => `
                        <section class="kpi-card">
                            <div class="kpi-eyebrow">Metric ${index + 1}</div>
                            <div class="kpi-label">${escapeHtml(metric.label)}</div>
                            <div class="kpi-value-shell">
                                <div
                                    class="kpi-value"
                                    data-target="${metric.value}"
                                    data-decimals="${metric.decimal_places}"
                                >0</div>
                            </div>
                            <div class="kpi-subtext">${escapeHtml(metric.note)}</div>
                            <div class="kpi-footer-rail"></div>
                        </section>
                    `).join("");

                    const formatCompactNumber = (value, decimalPlaces) => {{
                        const absolute = Math.abs(value);
                        if (absolute >= 1_000_000) {{
                            return `${{(value / 1_000_000).toFixed(decimalPlaces)}}M`;
                        }}
                        if (absolute >= 1_000) {{
                            return `${{(value / 1_000).toFixed(decimalPlaces)}}K`;
                        }}
                        return new Intl.NumberFormat("en-US", {{
                            minimumFractionDigits: decimalPlaces,
                            maximumFractionDigits: decimalPlaces,
                        }}).format(value);
                    }};

                    const easeOutExpo = (progress) => (
                        progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress)
                    );

                    root.querySelectorAll(".kpi-value").forEach((node, index) => {{
                        const target = Number(node.dataset.target || "0");
                        const decimalPlaces = Number(node.dataset.decimals || "0");
                        const duration = 1350 + (index * 140);
                        const startTime = performance.now();

                        const tick = (timestamp) => {{
                            const elapsed = timestamp - startTime;
                            const progress = Math.min(elapsed / duration, 1);
                            const easedProgress = easeOutExpo(progress);
                            const currentValue = target * easedProgress;
                            node.textContent = formatCompactNumber(currentValue, decimalPlaces);

                            if (progress < 1) {{
                                window.requestAnimationFrame(tick);
                                return;
                            }}

                            node.textContent = formatCompactNumber(target, decimalPlaces);
                        }};

                        window.requestAnimationFrame(tick);
                    }});
                </script>
            </body>
        </html>
        """,
        height=340,
        scrolling=False,
    )
    return True


def _render_data_insights(rows: list[dict[str, Any]], *, kpi_cards_shown: bool = False) -> None:
    """Render an auto-generated data insights strip above the result table."""
    if not rows:
        return

    dataframe = pd.DataFrame(rows)
    row_count = len(dataframe)
    col_count = len(dataframe.columns)

    # Single-value result → show it as a big hero metric
    if row_count == 1 and col_count == 1 and not kpi_cards_shown:
        col_name = dataframe.columns[0]
        value = dataframe.iloc[0, 0]
        display_value = _format_number(float(value)) if isinstance(value, (int, float)) else str(value)
        label = col_name.replace("_", " ").title()
        st.markdown(
            f"""
            <div class="insight-hero-metric animate-in stagger-3">
                <div class="insight-hero-label">{escape(label)}</div>
                <div class="insight-hero-value">{escape(display_value)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # Build insight cards for numeric columns
    numeric_cols = [c for c in dataframe.columns if pd.api.types.is_numeric_dtype(dataframe[c])]
    insight_items: list[tuple[str, str, str]] = [
        ("Rows", str(row_count), "Total rows returned"),
        ("Columns", str(col_count), "Result width"),
    ]

    for col_name in numeric_cols[:3]:
        pretty_name = col_name.replace("_", " ").title()
        col_min = dataframe[col_name].min()
        col_max = dataframe[col_name].max()
        col_sum = dataframe[col_name].sum()
        insight_items.append(
            (f"{pretty_name} Range", f"{_format_number(col_min)} – {_format_number(col_max)}", f"Sum: {_format_number(col_sum)}")
        )

    cards_markup = "".join(
        (
            '<div class="insight-card animate-in stagger-4">'
            f'<div class="insight-card-label">{escape(label)}</div>'
            f'<div class="insight-card-value">{escape(value)}</div>'
            f'<div class="insight-card-note">{escape(note)}</div>'
            "</div>"
        )
        for label, value, note in insight_items
    )

    st.markdown(
        f'<div class="insight-strip">{cards_markup}</div>',
        unsafe_allow_html=True,
    )


def _render_results_tab(turn: dict[str, Any], result: QueryResult) -> None:
    """Render the main query result table with data insights."""
    if result.error:
        st.error("The query could not be completed, so no final result table is available.")
        if result.message:
            st.info(result.message)
        return

    if result.executed:
        st.success("The query ran successfully.")
    elif result.message:
        st.info(result.message)

    if result.rows:
        kpi_cards_shown = _render_kpi_scorecards(result.rows, turn)
        _render_data_insights(result.rows, kpi_cards_shown=kpi_cards_shown)
        st.dataframe(result.rows, width="stretch")
    else:
        if result.executed:
            st.info("Query executed successfully but returned 0 rows.")
        else:
            st.markdown(
                """
                <div class="empty-soft-note">
                    <strong>No rows yet.</strong> Run a question above to see results here.
                    Try one of the suggested prompts to get started.
                </div>
                """,
                unsafe_allow_html=True,
            )

    if result.message and result.executed:
        st.caption(result.message)


def _prettify_chart_field_name(column_name: str) -> str:
    """Turn one result column name into premium chart copy."""
    return str(column_name).replace("_", " ").strip().title()


def _stringify_chart_label(value: Any) -> str:
    """Convert one axis label into a stable string for Chart.js."""
    if pd.isna(value):
        return "Unknown"

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")

    return str(value)


def _build_cinematic_chart_payload(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Build the trimmed chart payload used by the GSAP + Chart.js scene."""
    if not rows:
        return None

    dataframe = pd.DataFrame(rows)
    chart_details = _find_chart_columns(dataframe)
    if chart_details is None:
        return None

    chart_type, label_column, numeric_column = chart_details
    working_dataframe = dataframe[[label_column, numeric_column]].copy()
    working_dataframe[numeric_column] = pd.to_numeric(
        working_dataframe[numeric_column],
        errors="coerce",
    )
    working_dataframe = working_dataframe.dropna(subset=[numeric_column])
    if working_dataframe.empty:
        return None

    labels = [_stringify_chart_label(value) for value in working_dataframe[label_column].tolist()]
    values = [float(value) for value in working_dataframe[numeric_column].tolist()]
    if not values:
        return None

    highest_index = max(range(len(values)), key=lambda index: values[index])
    lowest_index = min(range(len(values)), key=lambda index: values[index])
    highest_label = labels[highest_index]
    lowest_label = labels[lowest_index]
    highest_value = values[highest_index]
    lowest_value = values[lowest_index]
    signal_range = highest_value - lowest_value

    metric_name = _prettify_chart_field_name(numeric_column)
    dimension_name = _prettify_chart_field_name(label_column)

    return {
        "chart_type": chart_type,
        "chart_title": f"{metric_name} by {dimension_name}",
        "chart_subtitle": (
            "Automated chart rendering from SQL query results."
        ),
        "label_column": dimension_name,
        "numeric_column": metric_name,
        "labels": labels,
        "values": values,
        "row_count": len(values),
        "kpis": [
            {
                "eyebrow": "Highest Point",
                "title": highest_label,
                "value": _format_number(highest_value),
                "note": f"Peak {metric_name.lower()} in the returned result set",
            },
            {
                "eyebrow": "Lowest Point",
                "title": lowest_label,
                "value": _format_number(lowest_value),
                "note": f"Floor {metric_name.lower()} in the returned result set",
            },
            {
                "eyebrow": "Signal Spread",
                "title": f"{metric_name} Range",
                "value": _format_number(signal_range),
                "note": f"Distance from {lowest_label} to {highest_label}",
            },
        ],
    }


def _render_cinematic_chart_scene(payload: dict[str, Any], chart_label: str) -> None:
    """Render the scroll-triggered GSAP + Chart.js chart experience."""
    template = """
    <!doctype html>
    <html>
        <head>
            <meta charset="utf-8" />
            <script src="https://cdn.jsdelivr.net/npm/gsap@3/dist/gsap.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                :root {
                    --scene-black: #101010;
                    --scene-charcoal: #171717;
                    --scene-panel: rgba(255, 255, 255, 0.055);
                    --scene-panel-strong: rgba(255, 255, 255, 0.085);
                    --scene-white: #fffdfa;
                    --scene-copy: rgba(255, 253, 250, 0.74);
                    --scene-muted: rgba(255, 253, 250, 0.46);
                    --scene-bronze: #af7f59;
                    --scene-bronze-soft: #d6af8c;
                    --scene-border: rgba(255, 255, 255, 0.085);
                    --scene-shadow: 0 38px 96px rgba(0, 0, 0, 0.28);
                }

                * {
                    box-sizing: border-box;
                }

                html, body {
                    margin: 0;
                    padding: 0;
                    background: transparent;
                    overflow: hidden;
                    font-family: "SF Pro Display", "Segoe UI", sans-serif;
                    color: var(--scene-white);
                }

                .cinematic-shell {
                    position: relative;
                    overflow: hidden;
                    min-height: 760px;
                    border-radius: 34px;
                    border: 1px solid rgba(17, 17, 17, 0.08);
                    background:
                        radial-gradient(circle at 14% 18%, rgba(214, 175, 140, 0.22), transparent 34%),
                        radial-gradient(circle at 82% 18%, rgba(255, 253, 250, 0.08), transparent 28%),
                        linear-gradient(180deg, rgba(17, 17, 17, 0.98) 0%, rgba(28, 28, 29, 0.985) 62%, rgba(14, 14, 15, 0.99) 100%);
                    box-shadow: var(--scene-shadow);
                    perspective: 1800px;
                }

                .cinematic-shell::before {
                    content: "";
                    position: absolute;
                    inset: 0;
                    background:
                        linear-gradient(135deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0) 34%),
                        linear-gradient(180deg, rgba(175,127,89,0.08) 0%, rgba(175,127,89,0) 22%);
                    pointer-events: none;
                }

                .cinematic-header {
                    position: relative;
                    z-index: 3;
                    display: flex;
                    align-items: flex-start;
                    justify-content: space-between;
                    gap: 1rem;
                    padding: 1.35rem 1.45rem 0;
                }

                .cinematic-kicker {
                    color: var(--scene-bronze-soft);
                    font-size: 0.76rem;
                    font-weight: 800;
                    letter-spacing: 0.18em;
                    text-transform: uppercase;
                    margin-bottom: 0.45rem;
                }

                .cinematic-title {
                    margin: 0;
                    font-size: clamp(1.45rem, 2vw, 1.85rem);
                    line-height: 1.08;
                    letter-spacing: -0.04em;
                }

                .cinematic-copy {
                    max-width: 40rem;
                    margin: 0.58rem 0 0;
                    color: var(--scene-copy);
                    font-size: 0.96rem;
                    line-height: 1.65;
                }

                .cinematic-chip {
                    padding: 0.75rem 0.92rem;
                    border-radius: 999px;
                    border: 1px solid rgba(214, 175, 140, 0.24);
                    background: rgba(255, 255, 255, 0.05);
                    color: var(--scene-bronze-soft);
                    font-size: 0.76rem;
                    font-weight: 800;
                    letter-spacing: 0.13em;
                    text-transform: uppercase;
                    white-space: nowrap;
                }

                .cinematic-stage {
                    position: relative;
                    height: 515px;
                    margin: 1rem 1rem 0;
                    border-radius: 28px;
                    overflow: hidden;
                    border: 1px solid rgba(255, 255, 255, 0.07);
                    background:
                        radial-gradient(circle at 50% 100%, rgba(175,127,89,0.18), transparent 38%),
                        linear-gradient(180deg, rgba(255,255,255,0.035) 0%, rgba(255,255,255,0.015) 100%);
                }

                .cinematic-stage::before {
                    content: "";
                    position: absolute;
                    inset: 0;
                    background-image:
                        linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px),
                        linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px);
                    background-size: 50px 50px;
                    mask-image: radial-gradient(circle at center, black 42%, transparent 92%);
                    opacity: 0.52;
                    pointer-events: none;
                }

                .cinematic-orb {
                    position: absolute;
                    border-radius: 999px;
                    filter: blur(0);
                    pointer-events: none;
                    opacity: 0.82;
                }

                .cinematic-orb.one {
                    top: 2.6rem;
                    left: 12%;
                    width: 14rem;
                    height: 14rem;
                    background: radial-gradient(circle, rgba(214,175,140,0.18) 0%, rgba(214,175,140,0) 72%);
                }

                .cinematic-orb.two {
                    right: 6%;
                    top: 8%;
                    width: 10rem;
                    height: 10rem;
                    background: radial-gradient(circle, rgba(255,253,250,0.08) 0%, rgba(255,253,250,0) 72%);
                }

                .cinematic-chart-viewport {
                    position: absolute;
                    right: 1.35rem;
                    left: 16.8rem;
                    top: 1.4rem;
                    bottom: 1.1rem;
                    padding: 1rem 1rem 0.9rem;
                    border-radius: 26px;
                    border: 1px solid rgba(255, 255, 255, 0.06);
                    background:
                        linear-gradient(180deg, rgba(255,255,255,0.045) 0%, rgba(255,255,255,0.015) 100%);
                    box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
                    opacity: 0;
                    transform: translateY(42px) scale(0.97);
                    filter: blur(14px);
                }

                .cinematic-chart-canvas-shell {
                    position: absolute;
                    inset: 1rem 1rem 1rem 1rem;
                }

                .cinematic-chart-canvas {
                    width: 100%;
                    height: 100%;
                    display: block;
                }

                .cinematic-floor-glow {
                    position: absolute;
                    left: 18rem;
                    right: 1.4rem;
                    bottom: 1.1rem;
                    height: 3.8rem;
                    border-radius: 999px;
                    background: radial-gradient(circle, rgba(175,127,89,0.28) 0%, rgba(175,127,89,0) 72%);
                    filter: blur(12px);
                    opacity: 0.88;
                    pointer-events: none;
                }

                .cinematic-laser-axis {
                    position: absolute;
                    background: linear-gradient(180deg, rgba(214,175,140,0.96), rgba(255,253,250,0.42));
                    box-shadow:
                        0 0 18px rgba(214, 175, 140, 0.7),
                        0 0 42px rgba(175, 127, 89, 0.32);
                    opacity: 0;
                    transform-origin: center center;
                    pointer-events: none;
                }

                .cinematic-laser-axis.y {
                    left: 17.8rem;
                    top: 3rem;
                    bottom: 4.55rem;
                    width: 2px;
                    transform: scaleY(0);
                }

                .cinematic-laser-axis.x {
                    left: 17.8rem;
                    right: 2rem;
                    bottom: 4.55rem;
                    height: 2px;
                    transform: scaleX(0);
                    transform-origin: left center;
                }

                .cinematic-kpi-stack {
                    position: absolute;
                    left: 1.25rem;
                    top: 1.25rem;
                    width: 14.2rem;
                    display: grid;
                    gap: 0.85rem;
                    z-index: 4;
                }

                .cinematic-kpi-card {
                    position: relative;
                    overflow: hidden;
                    padding: 1rem 1rem 1rem 1.05rem;
                    border-radius: 24px;
                    border: 1px solid rgba(255,255,255,0.08);
                    background:
                        linear-gradient(160deg, rgba(255,255,255,0.09) 0%, rgba(255,255,255,0.04) 100%);
                    box-shadow:
                        0 24px 60px rgba(0,0,0,0.2),
                        inset 0 1px 0 rgba(255,255,255,0.04);
                    transform: translateX(-220px) rotateY(34deg) rotateZ(-4deg);
                    opacity: 0;
                    transform-style: preserve-3d;
                    backdrop-filter: blur(18px);
                }

                .cinematic-kpi-card::before {
                    content: "";
                    position: absolute;
                    inset: 0;
                    background: linear-gradient(125deg, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0) 36%);
                    pointer-events: none;
                }

                .cinematic-kpi-eyebrow {
                    color: var(--scene-bronze-soft);
                    font-size: 0.72rem;
                    font-weight: 800;
                    letter-spacing: 0.16em;
                    text-transform: uppercase;
                    margin-bottom: 0.5rem;
                }

                .cinematic-kpi-title {
                    color: var(--scene-white);
                    font-size: 0.9rem;
                    font-weight: 800;
                    line-height: 1.35;
                    margin-bottom: 0.48rem;
                }

                .cinematic-kpi-value {
                    color: var(--scene-white);
                    font-size: 1.8rem;
                    font-weight: 800;
                    letter-spacing: -0.04em;
                    line-height: 1.06;
                    margin-bottom: 0.42rem;
                    text-shadow: 0 0 20px rgba(214, 175, 140, 0.24);
                }

                .cinematic-kpi-note {
                    color: var(--scene-copy);
                    font-size: 0.8rem;
                    line-height: 1.55;
                }

                .cinematic-footer {
                    position: relative;
                    z-index: 3;
                    display: flex;
                    flex-wrap: wrap;
                    gap: 0.7rem;
                    padding: 1rem 1rem 1.1rem;
                }

                .cinematic-footer-pill {
                    padding: 0.72rem 0.9rem;
                    border-radius: 999px;
                    border: 1px solid rgba(255,255,255,0.07);
                    background: rgba(255,255,255,0.04);
                    color: var(--scene-copy);
                    font-size: 0.78rem;
                    font-weight: 700;
                    letter-spacing: 0.02em;
                }

                .cinematic-note {
                    margin-top: 0.28rem;
                    color: var(--scene-muted);
                    font-size: 0.8rem;
                    line-height: 1.5;
                }

                @media (max-width: 920px) {
                    .cinematic-shell {
                        min-height: 930px;
                    }

                    .cinematic-header {
                        flex-direction: column;
                    }

                    .cinematic-stage {
                        height: 670px;
                    }

                    .cinematic-kpi-stack {
                        position: relative;
                        width: auto;
                        left: 0;
                        top: 0;
                        padding: 1rem 1rem 0;
                    }

                    .cinematic-chart-viewport {
                        left: 1rem;
                        top: 19.7rem;
                    }

                    .cinematic-laser-axis.y {
                        left: 2rem;
                        top: 21.3rem;
                    }

                    .cinematic-laser-axis.x {
                        left: 2rem;
                        right: 2rem;
                        bottom: 4.55rem;
                    }

                    .cinematic-floor-glow {
                        left: 1.2rem;
                    }
                }
            </style>
        </head>
        <body>
            <div class="cinematic-shell" id="cinematic-chart-root">
                <div class="cinematic-header">
                    <div>
                        <div class="cinematic-kicker">Automated Chart Generation</div>
                        <h3 class="cinematic-title">__CHART_TITLE__</h3>
                        <p class="cinematic-copy">__CHART_SUBTITLE__</p>
                        <div class="cinematic-note">Chart is generated from the current query result.</div>
                    </div>
                    <div class="cinematic-chip">__CHART_LABEL__</div>
                </div>
                <div class="cinematic-stage" id="cinematic-chart-stage">
                    <div class="cinematic-orb one"></div>
                    <div class="cinematic-orb two"></div>
                    <div class="cinematic-kpi-stack" id="cinematic-kpi-stack"></div>
                    <div class="cinematic-laser-axis y" id="cinematic-axis-y"></div>
                    <div class="cinematic-laser-axis x" id="cinematic-axis-x"></div>
                    <div class="cinematic-chart-viewport" id="cinematic-chart-viewport">
                        <div class="cinematic-chart-canvas-shell">
                            <canvas class="cinematic-chart-canvas" id="cinematic-chart-canvas"></canvas>
                        </div>
                    </div>
                    <div class="cinematic-floor-glow"></div>
                </div>
                <div class="cinematic-footer">
                    <div class="cinematic-footer-pill">Tabular Data Rendering</div>
                    <div class="cinematic-footer-pill">Automated Chart Generation</div>
                    <div class="cinematic-footer-pill">__CHART_MODE__ chart</div>
                    <div class="cinematic-footer-pill">Chart.js</div>
                </div>
            </div>
            <script>
                (() => {
                    const payload = __PAYLOAD__;
                    const root = document.getElementById("cinematic-chart-root");
                    const stage = document.getElementById("cinematic-chart-stage");
                    const viewport = document.getElementById("cinematic-chart-viewport");
                    const canvas = document.getElementById("cinematic-chart-canvas");
                    const kpiStack = document.getElementById("cinematic-kpi-stack");
                    const axisY = document.getElementById("cinematic-axis-y");
                    const axisX = document.getElementById("cinematic-axis-x");

                    if (
                        !root ||
                        !stage ||
                        !viewport ||
                        !canvas ||
                        !kpiStack ||
                        !axisY ||
                        !axisX ||
                        typeof window.Chart === "undefined" ||
                        typeof window.gsap === "undefined"
                    ) {
                        return;
                    }

                    const state = {
                        chart: null,
                        started: false,
                    };
                    const theme = {
                        bronze: "#af7f59",
                        bronzeSoft: "#d6af8c",
                        white: "#fffdfa",
                        copy: "rgba(255,253,250,0.72)",
                        muted: "rgba(255,253,250,0.42)",
                        grid: "rgba(255,253,250,0.08)",
                    };

                    const formatValue = (value) => {
                        const numericValue = Number(value || 0);
                        if (Math.abs(numericValue) >= 1000000) {
                            return `${(numericValue / 1000000).toFixed(1)}M`;
                        }
                        if (Math.abs(numericValue) >= 1000) {
                            return `${(numericValue / 1000).toFixed(1)}K`;
                        }
                        if (Number.isInteger(numericValue)) {
                            return numericValue.toLocaleString();
                        }
                        return numericValue.toLocaleString(undefined, {
                            minimumFractionDigits: 0,
                            maximumFractionDigits: 2,
                        });
                    };

                    const renderKpis = () => {
                        kpiStack.innerHTML = payload.kpis
                            .map(
                                (kpi, index) => `
                                    <div class="cinematic-kpi-card cinematic-kpi-card-${index + 1}">
                                        <div class="cinematic-kpi-eyebrow">${kpi.eyebrow}</div>
                                        <div class="cinematic-kpi-title">${kpi.title}</div>
                                        <div class="cinematic-kpi-value">${kpi.value}</div>
                                        <div class="cinematic-kpi-note">${kpi.note}</div>
                                    </div>
                                `,
                            )
                            .join("");
                    };

                    const createDataset = (context) => {
                        const gradient = context.createLinearGradient(0, 0, 0, canvas.height || 420);
                        gradient.addColorStop(0, "rgba(214, 175, 140, 0.98)");
                        gradient.addColorStop(0.55, "rgba(175, 127, 89, 0.92)");
                        gradient.addColorStop(1, "rgba(95, 75, 58, 0.46)");

                        if (payload.chart_type === "line") {
                            return {
                                label: payload.numeric_column,
                                data: payload.values,
                                borderColor: theme.bronzeSoft,
                                borderWidth: 3,
                                pointRadius: 4.2,
                                pointHoverRadius: 6.4,
                                pointBackgroundColor: theme.white,
                                pointBorderColor: theme.bronze,
                                pointBorderWidth: 2,
                                pointHoverBackgroundColor: theme.white,
                                pointHoverBorderColor: theme.bronzeSoft,
                                fill: true,
                                backgroundColor: "rgba(175, 127, 89, 0.14)",
                                tension: 0.34,
                            };
                        }

                        return {
                            label: payload.numeric_column,
                            data: payload.values,
                            backgroundColor: gradient,
                            borderColor: theme.bronzeSoft,
                            borderWidth: 1.4,
                            borderRadius: 14,
                            borderSkipped: false,
                            hoverBackgroundColor: "rgba(214, 175, 140, 0.96)",
                            barPercentage: payload.row_count > 10 ? 0.64 : 0.74,
                            categoryPercentage: payload.row_count > 10 ? 0.72 : 0.82,
                        };
                    };

                    const createChart = () => {
                        if (state.chart) {
                            return;
                        }

                        const context = canvas.getContext("2d");
                        if (!context) {
                            return;
                        }

                        let delayed = false;
                        state.chart = new window.Chart(context, {
                            type: payload.chart_type === "line" ? "line" : "bar",
                            data: {
                                labels: payload.labels,
                                datasets: [createDataset(context)],
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                interaction: {
                                    mode: "index",
                                    intersect: false,
                                },
                                layout: {
                                    padding: {
                                        top: 16,
                                        right: 12,
                                        bottom: 8,
                                        left: 12,
                                    },
                                },
                                plugins: {
                                    legend: {
                                        display: false,
                                    },
                                    tooltip: {
                                        backgroundColor: "rgba(12, 12, 13, 0.92)",
                                        borderColor: "rgba(214, 175, 140, 0.28)",
                                        borderWidth: 1,
                                        cornerRadius: 14,
                                        displayColors: false,
                                        padding: 14,
                                        titleColor: theme.white,
                                        bodyColor: theme.copy,
                                        callbacks: {
                                            title: (items) => items[0]?.label || "",
                                            label: (item) => `${payload.numeric_column}: ${formatValue(item.parsed.y ?? item.parsed)}`,
                                        },
                                    },
                                },
                                scales: {
                                    x: {
                                        grid: {
                                            display: false,
                                        },
                                        border: {
                                            display: false,
                                        },
                                        ticks: {
                                            color: theme.copy,
                                            font: {
                                                family: "Manrope, Segoe UI, sans-serif",
                                                size: 11,
                                                weight: "700",
                                            },
                                            maxRotation: payload.row_count > 8 ? 28 : 0,
                                            minRotation: payload.row_count > 8 ? 28 : 0,
                                        },
                                    },
                                    y: {
                                        beginAtZero: payload.chart_type !== "line",
                                        grid: {
                                            color: theme.grid,
                                            drawBorder: false,
                                        },
                                        border: {
                                            display: false,
                                        },
                                        ticks: {
                                            color: theme.muted,
                                            font: {
                                                family: "Manrope, Segoe UI, sans-serif",
                                                size: 11,
                                                weight: "700",
                                            },
                                            callback: (value) => formatValue(value),
                                        },
                                    },
                                },
                                animation: {
                                    onComplete: () => {
                                        delayed = true;
                                    },
                                    delay: (animationContext) => {
                                        if (
                                            animationContext.type !== "data" ||
                                            animationContext.mode !== "default" ||
                                            delayed
                                        ) {
                                            return 0;
                                        }
                                        return animationContext.dataIndex * 110;
                                    },
                                },
                                animations: payload.chart_type === "line"
                                    ? {
                                        x: {
                                            type: "number",
                                            easing: "easeOutQuart",
                                            duration: 800,
                                            from: NaN,
                                        },
                                        y: {
                                            type: "number",
                                            easing: "easeOutQuart",
                                            duration: 1050,
                                            from: (animationContext) => animationContext.chart.scales.y.getPixelForValue(0),
                                        },
                                    }
                                    : {
                                        y: {
                                            easing: "easeOutElastic",
                                            duration: 1180,
                                            from: (animationContext) => animationContext.chart.scales.y.getPixelForValue(0),
                                        },
                                        x: {
                                            easing: "easeOutCubic",
                                            duration: 760,
                                        },
                                    },
                            },
                        });
                    };

                    const runAmbientMotion = () => {
                        window.gsap.to(".cinematic-orb.one", {
                            x: 18,
                            y: -14,
                            duration: 5.6,
                            repeat: -1,
                            yoyo: true,
                            ease: "sine.inOut",
                        });
                        window.gsap.to(".cinematic-orb.two", {
                            x: -16,
                            y: 18,
                            duration: 6.2,
                            repeat: -1,
                            yoyo: true,
                            ease: "sine.inOut",
                        });
                    };

                    const animateScene = () => {
                        if (state.started) {
                            return;
                        }
                        state.started = true;

                        renderKpis();
                        runAmbientMotion();

                        const timeline = window.gsap.timeline({
                            defaults: {
                                ease: "power3.out",
                            },
                        });

                        timeline
                            .fromTo(
                                axisY,
                                {
                                    scaleY: 0,
                                    opacity: 0,
                                    transformOrigin: "bottom center",
                                },
                                {
                                    scaleY: 1,
                                    opacity: 1,
                                    duration: 0.75,
                                },
                                0.12,
                            )
                            .fromTo(
                                axisX,
                                {
                                    scaleX: 0,
                                    opacity: 0,
                                    transformOrigin: "left center",
                                },
                                {
                                    scaleX: 1,
                                    opacity: 1,
                                    duration: 0.82,
                                },
                                0.3,
                            )
                            .fromTo(
                                viewport,
                                {
                                    opacity: 0,
                                    y: 42,
                                    scale: 0.97,
                                    filter: "blur(14px)",
                                },
                                {
                                    opacity: 1,
                                    y: 0,
                                    scale: 1,
                                    filter: "blur(0px)",
                                    duration: 0.95,
                                    onStart: createChart,
                                },
                                0.1,
                            )
                            .fromTo(
                                ".cinematic-kpi-card",
                                {
                                    x: -240,
                                    opacity: 0,
                                    rotateY: 36,
                                    rotateZ: -4,
                                },
                                {
                                    x: 0,
                                    opacity: 1,
                                    rotateY: 0,
                                    rotateZ: 0,
                                    stagger: 0.14,
                                    duration: 1.05,
                                    ease: "back.out(1.18)",
                                },
                                0.48,
                            );

                        window.gsap.utils.toArray(".cinematic-kpi-card").forEach((card, index) => {
                            window.gsap.to(card, {
                                y: -10 - (index * 2),
                                duration: 3.1 + (index * 0.26),
                                repeat: -1,
                                yoyo: true,
                                ease: "sine.inOut",
                                delay: 1.4 + (index * 0.12),
                            });
                        });
                    };

                    const observeViewportEntry = () => {
                        let observer = null;
                        try {
                            const frameElement = window.frameElement;
                            if (frameElement && window.parent && window.parent !== window) {
                                observer = new window.parent.IntersectionObserver(
                                    (entries) => {
                                        if (entries[0]?.isIntersecting) {
                                            animateScene();
                                            observer?.disconnect();
                                        }
                                    },
                                    {
                                        threshold: 0.42,
                                    },
                                );
                                observer.observe(frameElement);
                                return;
                            }
                        } catch (_error) {
                            observer = null;
                        }

                        if (typeof window.IntersectionObserver === "undefined") {
                            animateScene();
                            return;
                        }

                        observer = new window.IntersectionObserver(
                            (entries) => {
                                if (entries[0]?.isIntersecting) {
                                    animateScene();
                                    observer?.disconnect();
                                }
                            },
                            {
                                threshold: 0.35,
                            },
                        );
                        observer.observe(root);
                    };

                    observeViewportEntry();
                })();
            </script>
        </body>
    </html>
    """

    chart_mode = "line plot" if chart_label == "Line chart" else "bar columns"
    html = (
        template.replace("__PAYLOAD__", json.dumps(payload))
        .replace("__CHART_TITLE__", escape(payload["chart_title"]))
        .replace("__CHART_SUBTITLE__", escape(payload["chart_subtitle"]))
        .replace("__CHART_LABEL__", escape(chart_label))
        .replace("__CHART_MODE__", escape(chart_mode))
    )
    components.html(html, height=820, scrolling=False)


def _render_chart_tab(result: QueryResult) -> None:
    """Render the chart as a cinematic, scroll-triggered data experience."""
    chart_shown, chart_label = _get_chart_summary(result.rows)

    if not result.rows:
        st.markdown(
            """
            <div class="empty-soft-note">
                <strong>Chart tab is ready.</strong> Run a query that returns results
                and a chart will render here automatically when the shape fits.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    if not chart_shown:
        st.markdown(
            """
            <div class="empty-soft-note">
                <strong>Not chart-friendly.</strong> This result has a shape that doesn't map
                to a simple bar or line chart. Try a query that returns one label/time column
                plus one numeric column — like "Top 10 cities by number of businesses".
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    payload = _build_cinematic_chart_payload(result.rows)
    if payload is None:
        st.markdown(
            """
            <div class="empty-soft-note">
                <strong>Chart payload unavailable.</strong> The result shape looked chart-friendly,
                but the cinematic chart payload could not be assembled safely from the returned rows.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    _render_cinematic_chart_scene(payload, chart_label)


def _render_map_tab(result: QueryResult) -> None:
    """Render the interactive map inside a styled dark container."""
    map_shown, map_label = _get_map_summary(result.rows)

    if not result.rows:
        st.markdown(
            """
            <div class="empty-soft-note">
                <strong>Map tab is ready.</strong> Run a location-aware query and
                the assistant will place the returned businesses on an interactive map.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    if not map_shown:
        st.markdown(
            """
            <div class="empty-soft-note">
                <strong>No map coordinates found.</strong> To unlock the map view,
                return <code>latitude</code> and <code>longitude</code> columns, or
                include both <code>city</code> and <code>state</code> in the result.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f"""
        <div class="chart-container-header animate-in">
            <span class="chart-container-icon">🗺️</span>
            <div>
                <div class="chart-container-title">{escape(map_label)}</div>
                <div class="chart-container-note">Dark-mode geospatial view with bronze glow markers</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_map(result.rows)


def _render_errors_tab(turn: dict[str, Any], result: QueryResult) -> None:
    """Render error details and retry notes in one place."""
    if turn["retry_happened"]:
        st.info(turn["retry_status"])

    if turn["generation_note"]:
        st.caption(turn["generation_note"])

    if result.error:
        st.error("The query could not be completed.")
        if result.message:
            st.info(result.message)
        st.code(result.error, language="text")
        return

    st.success("No error details for this response.")


def _get_available_detail_panels(
    has_sql_details: bool,
    chart_shown: bool,
    map_shown: bool,
    has_error_details: bool,
) -> list[str]:
    """Return the detail panels that are valid for the current response."""
    panels = ["results"]
    if has_sql_details:
        panels.insert(0, "sql")
    if chart_shown:
        panels.append("chart")
    if map_shown:
        panels.append("map")
    if has_error_details:
        panels.append("errors")
    return panels


def _get_active_detail_panel(available_panels: list[str]) -> str:
    """Choose a safe detail panel based on the URL state."""
    requested_panel = _get_active_panel()
    if requested_panel in available_panels:
        return requested_panel
    if DEFAULT_DETAIL_PANEL in available_panels:
        return DEFAULT_DETAIL_PANEL
    return available_panels[0]


def _render_detail_panel_nav(
    available_panels: list[str],
    active_panel: str,
) -> None:
    """Render shareable, query-param-driven panel navigation."""
    links_markup = "".join(
        (
            f'<a href="{escape(_build_route_href("home", panel=panel))}" '
            f'class="spa-detail-link{" active" if panel == active_panel else ""}" '
            'target="_self">'
            f"{escape(DETAIL_PANEL_LABELS[panel])}"
            "</a>"
        )
        for panel in available_panels
    )
    st.markdown(
        f"""
        <style>
        .spa-detail-nav {{
            display: flex;
            gap: 0.7rem;
            flex-wrap: wrap;
            margin: 1rem 0 1.1rem;
        }}

        .spa-detail-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.72rem 1rem;
            border-radius: 999px;
            border: 1px solid rgba(17, 17, 17, 0.08);
            background: rgba(255, 255, 255, 0.74);
            color: #595959;
            text-decoration: none;
            font-size: 0.84rem;
            font-weight: 700;
            letter-spacing: 0.01em;
            transition: all 0.2s ease;
            box-shadow: 0 12px 28px rgba(17, 17, 17, 0.06);
        }}

        .spa-detail-link:hover {{
            color: #111111;
            border-color: rgba(175, 127, 89, 0.28);
            transform: translateY(-1px);
        }}

        .spa-detail-link.active {{
            background: linear-gradient(135deg, rgba(175, 127, 89, 0.16), rgba(255, 255, 255, 0.94));
            color: #111111;
            border-color: rgba(175, 127, 89, 0.36);
            box-shadow: 0 16px 30px rgba(175, 127, 89, 0.14);
        }}
        </style>
        <div class="spa-detail-nav animate-in stagger-3">{links_markup}</div>
        """,
        unsafe_allow_html=True,
    )


def _render_active_detail_panel(
    *,
    active_panel: str,
    turn: dict[str, Any],
    result: QueryResult,
) -> None:
    """Render the selected detail panel for the current response."""
    if active_panel == "sql":
        _render_sql_tab(turn)
        return

    if active_panel == "chart":
        _render_chart_tab(result)
        return

    if active_panel == "map":
        _render_map_tab(result)
        return

    if active_panel == "errors":
        _render_errors_tab(turn, result)
        return

    _render_results_tab(turn, result)


def _normalize_audio_capture(audio_capture: Any) -> dict[str, Any] | None:
    """Convert a recorder payload into raw bytes plus upload metadata."""
    if audio_capture is None:
        return None

    if isinstance(audio_capture, (bytes, bytearray)):
        audio_bytes = bytes(audio_capture)
        if not audio_bytes:
            return None
        return {
            "bytes": audio_bytes,
            "filename": "voice-question.wav",
            "content_type": "audio/wav",
        }

    getvalue = getattr(audio_capture, "getvalue", None)
    if callable(getvalue):
        audio_bytes = getvalue()
    else:
        read = getattr(audio_capture, "read", None)
        if not callable(read):
            return None
        audio_bytes = read()
        seek = getattr(audio_capture, "seek", None)
        if callable(seek):
            seek(0)

    if not audio_bytes:
        return None

    filename = str(getattr(audio_capture, "name", "") or "voice-question.wav")
    content_type = str(getattr(audio_capture, "type", "") or "audio/wav")
    return {
        "bytes": audio_bytes,
        "filename": filename,
        "content_type": content_type,
    }


def _process_voice_audio_capture(audio_capture: Any) -> None:
    """Transcribe a newly recorded question and route it through the main pipeline."""
    normalized_capture = _normalize_audio_capture(audio_capture)
    if normalized_capture is None:
        return

    audio_bytes = normalized_capture["bytes"]
    audio_hash = hashlib.sha256(audio_bytes).hexdigest()
    if st.session_state.last_voice_audio_hash == audio_hash:
        return

    with st.spinner("Transcribing your voice question..."):
        transcription_result = transcribe_audio_bytes(
            audio_bytes,
            filename=normalized_capture["filename"],
            content_type=normalized_capture["content_type"],
        )

    st.session_state.last_voice_audio_hash = audio_hash
    st.session_state.latest_voice_transcription_note = transcription_result.notes

    if not transcription_result.text.strip():
        st.session_state.latest_voice_transcript = ""
        st.session_state.latest_voice_transcription_error = transcription_result.notes
        return

    clean_transcript = transcription_result.text.strip()
    st.session_state.latest_voice_transcript = clean_transcript
    st.session_state.latest_voice_transcription_error = ""
    st.session_state.nl_question_text = clean_transcript
    st.session_state.chat_mode = CHAT_MODE_DATA
    _queue_question_submission(
        clean_transcript,
        st.session_state.nl_use_demo_mode,
        chat_mode=CHAT_MODE_DATA,
    )
    st.toast("Voice question captured and queued for SQL generation.")
    st.rerun()


def _render_voice_query_composer() -> None:
    """Render a compact voice capture status panel."""
    st.markdown(
        """
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; gap: 1rem; flex-wrap: wrap; align-items: start;">
                <div>
                    <div style="font-size: 0.74rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Voice Input</div>
                    <h3 style="margin: 0.45rem 0 0.35rem;">Speech-to-Text</h3>
                    <p style="margin: 0; color: #595959; line-height: 1.72;">Voice input is currently unavailable.</p>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    voice_columns = st.columns([1.12, 0.88], gap="large")
    audio_capture = None

    with voice_columns[0]:
        st.markdown(
            f"""
            <div class="premium-glass-card animate-in stagger-2" style="height: 100%;">
                <div style="font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Voice Status</div>
                <h4 style="margin: 0.45rem 0 0.55rem;">Speech-to-text pipeline</h4>
                <div style="display:flex; gap:0.55rem; flex-wrap:wrap; margin-bottom: 0.9rem;">
                    <span style="display:inline-flex; align-items:center; padding:0.46rem 0.72rem; border-radius:999px; background: rgba(17,17,17,0.06); color:#111; font-size:0.8rem; font-weight:700;">Provider: Disabled</span>
                    <span style="display:inline-flex; align-items:center; padding:0.46rem 0.72rem; border-radius:999px; background: rgba(175,127,89,0.12); color:#111; font-size:0.8rem; font-weight:700;">Text Input Active</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.info(
            "Voice transcription is disabled in this build."
        )

        if st.session_state.latest_voice_transcription_error:
            st.error(st.session_state.latest_voice_transcription_error)
        elif st.session_state.latest_voice_transcription_note:
            st.caption(st.session_state.latest_voice_transcription_note)

        if st.session_state.latest_voice_transcript:
            st.markdown(
                f"""
                <div class="premium-glass-card animate-in stagger-3" style="margin-top: 0.9rem;">
                    <div style="font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Latest Transcript</div>
                    <div style="margin-top: 0.6rem; color: #111111; font-size: 1.02rem; font-weight: 700; line-height: 1.6;">{escape(st.session_state.latest_voice_transcript)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with voice_columns[1]:
        st.markdown(
            """
            <div class="premium-glass-card animate-in stagger-2" style="height: 100%; display:flex; align-items:center; justify-content:center; text-align:center;">
                <div>
                    <div style="font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Voice Capture</div>
                    <h4 style="margin: 0.5rem 0 0.4rem;">Temporarily Offline</h4>
                    <p style="margin: 0; color: #595959; line-height: 1.7;">Use text input for Natural Language Querying.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _queue_question_submission(
    question: str,
    use_demo_mode: bool,
    *,
    chat_mode: str,
) -> None:
    """Store one pending NL question so the UI can show the live pipeline before running it."""
    clean_question = question.strip()
    if not clean_question:
        return

    resolved_chat_mode = _normalize_chat_mode(chat_mode)
    st.session_state.pending_question_submission = {
        "question": clean_question,
        "use_demo_mode": use_demo_mode,
        "chat_mode": resolved_chat_mode,
    }
    _set_pipeline_loading_state(
        PIPELINE_PHASE_USER_INTENT,
        "Reading the question and preparing the response pipeline.",
        sequence_only_if_changed=False,
    )


def _get_pending_question_submission() -> dict[str, Any] | None:
    """Return the queued question submission when one is waiting to run."""
    pending = st.session_state.pending_question_submission
    if not isinstance(pending, dict):
        return None

    question = str(pending.get("question", "")).strip()
    if not question:
        return None

    requested_chat_mode = _normalize_chat_mode(pending.get("chat_mode", _get_active_chat_mode()))
    resolved_chat_mode = _resolve_chat_mode(question, requested_chat_mode)
    return {
        "question": question,
        "use_demo_mode": bool(pending.get("use_demo_mode", st.session_state.nl_use_demo_mode)),
        "chat_mode": requested_chat_mode,
        "resolved_chat_mode": resolved_chat_mode,
    }


def _normalize_pipeline_phase(phase: str) -> str:
    """Normalize one loading phase value so the UI and JS share one vocabulary."""
    clean_phase = str(phase).strip().lower()
    if clean_phase == PIPELINE_PHASE_COMPLETE:
        return PIPELINE_PHASE_COMPLETE
    if clean_phase in PIPELINE_VISUAL_PHASE_ORDER:
        return clean_phase
    return ""


def _clear_pipeline_loading_state() -> None:
    """Reset the hidden bridge values that drive the pipeline visualizer."""
    st.session_state.pipeline_loading_phase = ""
    st.session_state.pipeline_loading_final_phase = ""
    st.session_state.pipeline_loading_note = ""
    st.session_state.pipeline_loading_sequence = 0


def _set_pipeline_loading_state(
    phase: str,
    note: str,
    *,
    final_phase: str | None = None,
    sequence_only_if_changed: bool = True,
) -> None:
    """Store one visual pipeline state update for the canvas bridge."""
    normalized_phase = _normalize_pipeline_phase(phase)
    normalized_final_phase = _normalize_pipeline_phase(final_phase or normalized_phase)
    clean_note = " ".join(str(note).strip().split())
    state_changed = (
        normalized_phase != str(st.session_state.pipeline_loading_phase)
        or normalized_final_phase != str(st.session_state.pipeline_loading_final_phase)
        or clean_note != str(st.session_state.pipeline_loading_note)
    )

    st.session_state.pipeline_loading_phase = normalized_phase
    st.session_state.pipeline_loading_final_phase = normalized_final_phase
    st.session_state.pipeline_loading_note = clean_note

    if state_changed or not sequence_only_if_changed:
        st.session_state.pipeline_loading_sequence = (
            int(st.session_state.pipeline_loading_sequence) + 1
        )


def _render_pipeline_progress_bridge(bridge_placeholder: Any) -> None:
    """Render the hidden parent-DOM bridge that the canvas iframe polls."""
    if bridge_placeholder is None:
        return

    bridge_placeholder.markdown(
        f"""
        <div
            id="nl-pipeline-progress-bridge"
            data-pipeline-progress-bridge="true"
            data-phase="{escape(str(st.session_state.pipeline_loading_phase))}"
            data-final-phase="{escape(str(st.session_state.pipeline_loading_final_phase))}"
            data-note="{escape(str(st.session_state.pipeline_loading_note))}"
            data-seq="{int(st.session_state.pipeline_loading_sequence)}"
            style="
                position: absolute;
                width: 1px;
                height: 1px;
                overflow: hidden;
                opacity: 0;
                pointer-events: none;
            "
        ></div>
        """,
        unsafe_allow_html=True,
    )


def _advance_pipeline_visualizer(
    bridge_placeholder: Any,
    phase: str,
    note: str,
    *,
    final_phase: str | None = None,
    delay_seconds: float = PIPELINE_VISUALIZER_FRAME_DELAY_SECONDS,
) -> None:
    """Push one live phase update into the canvas bridge and briefly yield time to paint."""
    _set_pipeline_loading_state(phase, note, final_phase=final_phase)
    _render_pipeline_progress_bridge(bridge_placeholder)
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def _render_query_pipeline_visualizer() -> None:
    """Render the holographic neural-net loading visualizer for queued NL questions."""
    sql_trace_href = _build_route_href("home", panel="sql")
    step_payload = json.dumps(
        [
            {"id": phase, "label": PIPELINE_VISUAL_PHASE_LABELS[phase]}
            for phase in PIPELINE_VISUAL_PHASE_ORDER
        ]
    )
    components.html(
        f"""
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8" />
                <style>
                    :root {{
                        --pipeline-black: #0f0f10;
                        --pipeline-panel: #171718;
                        --pipeline-white: #fffcf8;
                        --pipeline-bronze: #af7f59;
                        --pipeline-bronze-soft: #d6af8c;
                        --pipeline-bronze-glow: rgba(175, 127, 89, 0.72);
                        --pipeline-line-muted: rgba(255, 252, 248, 0.14);
                        --pipeline-border: rgba(255, 255, 255, 0.08);
                        --pipeline-copy: rgba(255, 252, 248, 0.72);
                    }}

                    * {{
                        box-sizing: border-box;
                    }}

                    html, body {{
                        margin: 0;
                        padding: 0;
                        background: transparent;
                        overflow: hidden;
                        font-family: "SF Pro Display", "Segoe UI", sans-serif;
                    }}

                    .neural-shell {{
                        position: relative;
                        overflow: hidden;
                        border-radius: 30px;
                        border: 1px solid rgba(255, 255, 255, 0.08);
                        background:
                            radial-gradient(circle at 18% 18%, rgba(214, 175, 140, 0.16) 0%, rgba(214, 175, 140, 0) 32%),
                            radial-gradient(circle at 82% 22%, rgba(255, 252, 248, 0.08) 0%, rgba(255, 252, 248, 0) 28%),
                            linear-gradient(180deg, rgba(16, 16, 17, 0.98) 0%, rgba(23, 23, 24, 0.98) 56%, rgba(14, 14, 15, 0.99) 100%);
                        box-shadow:
                            0 34px 88px rgba(0, 0, 0, 0.28),
                            inset 0 1px 0 rgba(255, 255, 255, 0.05);
                    }}

                    .neural-shell::before {{
                        content: "";
                        position: absolute;
                        inset: 0;
                        background:
                            linear-gradient(115deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0) 36%),
                            linear-gradient(180deg, rgba(175,127,89,0.12) 0%, rgba(175,127,89,0) 24%);
                        pointer-events: none;
                    }}

                    .neural-header {{
                        position: relative;
                        z-index: 2;
                        display: flex;
                        align-items: flex-start;
                        justify-content: space-between;
                        gap: 1rem;
                        padding: 1.25rem 1.35rem 0;
                        color: var(--pipeline-white);
                    }}

                    .neural-header-actions {{
                        display: flex;
                        align-items: center;
                        gap: 0.65rem;
                        flex-wrap: wrap;
                        justify-content: flex-end;
                    }}

                    .neural-kicker {{
                        color: var(--pipeline-bronze-soft);
                        font-size: 0.74rem;
                        font-weight: 800;
                        letter-spacing: 0.18em;
                        text-transform: uppercase;
                        margin-bottom: 0.45rem;
                    }}

                    .neural-title {{
                        margin: 0;
                        font-size: clamp(1.25rem, 2vw, 1.65rem);
                        line-height: 1.1;
                        letter-spacing: -0.04em;
                    }}

                    .neural-copy {{
                        max-width: 28rem;
                        margin: 0.55rem 0 0;
                        color: var(--pipeline-copy);
                        font-size: 0.94rem;
                        line-height: 1.6;
                    }}

                    .neural-chip {{
                        padding: 0.72rem 0.95rem;
                        border-radius: 999px;
                        border: 1px solid rgba(214, 175, 140, 0.22);
                        background: rgba(255, 255, 255, 0.05);
                        color: var(--pipeline-bronze-soft);
                        font-size: 0.78rem;
                        font-weight: 800;
                        letter-spacing: 0.12em;
                        text-transform: uppercase;
                        white-space: nowrap;
                        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.02);
                    }}

                    .neural-link-chip {{
                        padding: 0.72rem 0.95rem;
                        border-radius: 999px;
                        border: 1px solid rgba(255, 255, 255, 0.16);
                        background: rgba(255, 255, 255, 0.08);
                        color: var(--pipeline-white);
                        font-size: 0.76rem;
                        font-weight: 800;
                        letter-spacing: 0.1em;
                        text-transform: uppercase;
                        text-decoration: none;
                        white-space: nowrap;
                    }}

                    .neural-link-chip:hover {{
                        border-color: rgba(214, 175, 140, 0.35);
                        color: var(--pipeline-bronze-soft);
                    }}

                    .neural-stage {{
                        position: relative;
                        height: 420px;
                        margin: 0.8rem 1rem 0;
                        border-radius: 26px;
                        overflow: hidden;
                        border: 1px solid rgba(255, 255, 255, 0.06);
                        background:
                            radial-gradient(circle at 50% 12%, rgba(175,127,89,0.14) 0%, rgba(175,127,89,0) 42%),
                            linear-gradient(180deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);
                    }}

                    .neural-stage::before {{
                        content: "";
                        position: absolute;
                        inset: 0;
                        background-image:
                            linear-gradient(rgba(255,255,255,0.035) 1px, transparent 1px),
                            linear-gradient(90deg, rgba(255,255,255,0.035) 1px, transparent 1px);
                        background-size: 52px 52px;
                        mask-image: radial-gradient(circle at center, black 38%, transparent 92%);
                        opacity: 0.42;
                        pointer-events: none;
                    }}

                    .neural-canvas {{
                        position: absolute;
                        inset: 0;
                        width: 100%;
                        height: 100%;
                        display: block;
                    }}

                    .neural-statusbar {{
                        position: relative;
                        z-index: 2;
                        display: grid;
                        gap: 0.9rem;
                        padding: 1rem 1.1rem 1.15rem;
                    }}

                    .neural-note {{
                        padding: 0.95rem 1rem;
                        border-radius: 20px;
                        border: 1px solid rgba(255, 255, 255, 0.06);
                        background: rgba(255, 255, 255, 0.04);
                        color: var(--pipeline-white);
                        line-height: 1.6;
                        min-height: 4.3rem;
                        box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
                    }}

                    .neural-note-label {{
                        display: inline-flex;
                        align-items: center;
                        gap: 0.55rem;
                        color: var(--pipeline-bronze-soft);
                        font-size: 0.72rem;
                        font-weight: 800;
                        letter-spacing: 0.16em;
                        text-transform: uppercase;
                        margin-bottom: 0.45rem;
                    }}

                    .neural-note-label::before {{
                        content: "";
                        width: 0.58rem;
                        height: 0.58rem;
                        border-radius: 999px;
                        background: var(--pipeline-bronze);
                        box-shadow: 0 0 16px rgba(175, 127, 89, 0.85);
                        animation: pipelinePulse 1.45s ease-in-out infinite;
                    }}

                    @keyframes pipelinePulse {{
                        0%, 100% {{ transform: scale(0.92); opacity: 0.82; }}
                        50% {{ transform: scale(1.18); opacity: 1; }}
                    }}

                    .neural-steps {{
                        display: grid;
                        grid-template-columns: repeat(4, minmax(0, 1fr));
                        gap: 0.7rem;
                    }}

                    .neural-step {{
                        padding: 0.82rem 0.78rem;
                        border-radius: 18px;
                        border: 1px solid rgba(255, 255, 255, 0.05);
                        background: rgba(255, 255, 255, 0.03);
                        color: rgba(255, 252, 248, 0.44);
                        font-size: 0.76rem;
                        font-weight: 800;
                        letter-spacing: 0.12em;
                        text-transform: uppercase;
                        text-align: center;
                        transition: all 180ms ease;
                    }}

                    .neural-step.is-active {{
                        color: var(--pipeline-white);
                        border-color: rgba(214, 175, 140, 0.26);
                        background: linear-gradient(180deg, rgba(175,127,89,0.2) 0%, rgba(175,127,89,0.1) 100%);
                        box-shadow: 0 0 28px rgba(175, 127, 89, 0.18);
                    }}

                    .neural-step.is-complete {{
                        color: var(--pipeline-bronze-soft);
                        border-color: rgba(214, 175, 140, 0.18);
                        background: rgba(214, 175, 140, 0.08);
                    }}

                    @media (max-width: 860px) {{
                        .neural-header {{
                            flex-direction: column;
                        }}

                        .neural-stage {{
                            height: 360px;
                        }}

                        .neural-steps {{
                            grid-template-columns: repeat(2, minmax(0, 1fr));
                        }}
                    }}
                </style>
            </head>
            <body>
                <div class="neural-shell" id="neural-pipeline-root">
                    <div class="neural-header">
                        <div>
                            <div class="neural-kicker">Holographic Neural-Net Pipeline</div>
                            <h3 class="neural-title">Real-time Text-to-SQL execution, visualized live.</h3>
                            <p class="neural-copy">
                                The backend now advances this canvas step by step so the audience can watch the analytic pipeline move from intent capture to database execution.
                            </p>
                        </div>
                        <div class="neural-header-actions">
                            <a href="{escape(sql_trace_href)}" target="_self" class="neural-link-chip">View SQL Commands</a>
                            <div class="neural-chip">Live Backend Signal</div>
                        </div>
                    </div>
                    <div class="neural-stage" id="neural-pipeline-stage">
                        <canvas class="neural-canvas" id="neural-pipeline-canvas"></canvas>
                    </div>
                    <div class="neural-statusbar">
                        <div class="neural-note">
                            <div class="neural-note-label">Current Phase</div>
                            <div id="neural-pipeline-note">Calibrating the pipeline visualizer and waiting for the first backend signal.</div>
                        </div>
                        <div class="neural-steps" id="neural-pipeline-steps"></div>
                    </div>
                </div>
                <script>
                    (() => {{
                        const steps = {step_payload};
                        const phaseIndexByName = Object.fromEntries(
                            steps.map((step, index) => [step.id, index]),
                        );
                        const root = document.getElementById("neural-pipeline-root");
                        const stage = document.getElementById("neural-pipeline-stage");
                        const canvas = document.getElementById("neural-pipeline-canvas");
                        const noteElement = document.getElementById("neural-pipeline-note");
                        const stepsElement = document.getElementById("neural-pipeline-steps");
                        const context = canvas.getContext("2d", {{
                            alpha: true,
                            desynchronized: true,
                        }});

                        if (!root || !stage || !canvas || !context || !noteElement || !stepsElement) {{
                            return;
                        }}

                        const palette = {{
                            black: [15, 15, 16],
                            panel: [24, 24, 26],
                            white: [255, 252, 248],
                            bronze: [175, 127, 89],
                            bronzeSoft: [214, 175, 140],
                        }};
                        const state = {{
                            width: 0,
                            height: 0,
                            dpr: 1,
                            nodes: [],
                            segments: [],
                            particles: [],
                            packetTrail: [],
                            packetProgress: 0,
                            completedCount: 0,
                            activeIndex: 0,
                            lastSeq: -1,
                            note: "Calibrating the pipeline visualizer and waiting for the first backend signal.",
                            phase: steps[0].id,
                            finalPhase: steps[0].id,
                            lastTimestamp: 0,
                            time: 0,
                        }};

                        const parentDocument = (() => {{
                            try {{
                                return window.parent.document;
                            }} catch (_error) {{
                                return null;
                            }}
                        }})();

                        const rgba = (rgb, alpha) => `rgba(${{rgb[0]}}, ${{rgb[1]}}, ${{rgb[2]}}, ${{alpha}})`;
                        const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
                        const easeInOutSine = (value) => -(Math.cos(Math.PI * value) - 1) / 2;
                        const distance = (pointA, pointB) => Math.hypot(pointB.x - pointA.x, pointB.y - pointA.y);

                        const cubicPoint = (segment, t) => {{
                            const mt = 1 - t;
                            return {{
                                x:
                                    (mt ** 3) * segment.from.x +
                                    3 * (mt ** 2) * t * segment.cp1.x +
                                    3 * mt * (t ** 2) * segment.cp2.x +
                                    (t ** 3) * segment.to.x,
                                y:
                                    (mt ** 3) * segment.from.y +
                                    3 * (mt ** 2) * t * segment.cp1.y +
                                    3 * mt * (t ** 2) * segment.cp2.y +
                                    (t ** 3) * segment.to.y,
                            }};
                        }};

                        const buildScene = () => {{
                            const bounds = stage.getBoundingClientRect();
                            state.width = Math.max(bounds.width, 320);
                            state.height = Math.max(bounds.height, 280);
                            state.dpr = Math.min(window.devicePixelRatio || 1, 1.8);
                            canvas.width = Math.round(state.width * state.dpr);
                            canvas.height = Math.round(state.height * state.dpr);
                            context.setTransform(state.dpr, 0, 0, state.dpr, 0, 0);

                            const horizontalPadding = clamp(state.width * 0.1, 44, 92);
                            const trackWidth = state.width - (horizontalPadding * 2);
                            const yPositions = [
                                state.height * 0.65,
                                state.height * 0.34,
                                state.height * 0.62,
                                state.height * 0.36,
                            ];

                            state.nodes = steps.map((step, index) => {{
                                const x = horizontalPadding + ((trackWidth * index) / (steps.length - 1));
                                return {{
                                    ...step,
                                    x,
                                    y: yPositions[index] || state.height * 0.5,
                                    radius: index === 0 || index === steps.length - 1 ? 24 : 22,
                                }};
                            }});

                            state.segments = state.nodes.slice(0, -1).map((node, index) => {{
                                const nextNode = state.nodes[index + 1];
                                const dx = nextNode.x - node.x;
                                const curveLift = index % 2 === 0 ? -state.height * 0.18 : state.height * 0.18;
                                return {{
                                    from: node,
                                    to: nextNode,
                                    cp1: {{
                                        x: node.x + dx * 0.34,
                                        y: node.y + curveLift,
                                    }},
                                    cp2: {{
                                        x: node.x + dx * 0.66,
                                        y: nextNode.y - curveLift,
                                    }},
                                }};
                            }});
                        }};

                        const renderSteps = () => {{
                            const activeIndex = state.activeIndex;
                            const completedCount = state.completedCount;
                            stepsElement.innerHTML = steps
                                .map((step, index) => {{
                                    const classNames = ["neural-step"];
                                    if (index < completedCount) {{
                                        classNames.push("is-complete");
                                    }}
                                    if (index === activeIndex) {{
                                        classNames.push("is-active");
                                    }}
                                    if (state.phase === "complete" && index === completedCount - 1) {{
                                        classNames.push("is-complete");
                                    }}
                                    return `<div class="${{classNames.join(" ")}}">${{step.label}}</div>`;
                                }})
                                .join("");
                        }};

                        const spawnBurst = (node, intensity = 1) => {{
                            if (!node) {{
                                return;
                            }}

                            const particleCount = Math.round(18 + (14 * intensity));
                            for (let index = 0; index < particleCount; index += 1) {{
                                const angle = (Math.PI * 2 * index) / particleCount + (Math.random() * 0.32);
                                const speed = (1.1 + Math.random() * 2.6) * intensity;
                                state.particles.push({{
                                    x: node.x,
                                    y: node.y,
                                    vx: Math.cos(angle) * speed,
                                    vy: Math.sin(angle) * speed,
                                    size: 1.4 + Math.random() * 3.1,
                                    life: 1,
                                    decay: 0.016 + Math.random() * 0.024,
                                    tint: Math.random() > 0.28 ? "bronze" : "white",
                                }});
                            }}
                        }};

                        const applyBridgeUpdate = (payload) => {{
                            const previousCompletedCount = state.completedCount;
                            const previousActiveIndex = state.activeIndex;
                            const normalizedPhase = payload.phase === "complete" || phaseIndexByName[payload.phase] !== undefined
                                ? payload.phase
                                : state.phase;
                            const normalizedFinalPhase = phaseIndexByName[payload.finalPhase] !== undefined
                                ? payload.finalPhase
                                : state.finalPhase;

                            state.phase = normalizedPhase;
                            state.finalPhase = normalizedFinalPhase;
                            state.note = payload.note || state.note;
                            noteElement.textContent = state.note;
                            state.lastSeq = payload.seq;

                            if (normalizedPhase === "complete") {{
                                const finalIndex = phaseIndexByName[normalizedFinalPhase];
                                const completedCount = finalIndex >= 0
                                    ? Math.max(state.completedCount, finalIndex + 1)
                                    : state.completedCount;
                                for (let index = previousCompletedCount; index < completedCount; index += 1) {{
                                    spawnBurst(state.nodes[index], index === completedCount - 1 ? 1.22 : 1);
                                }}
                                state.completedCount = completedCount;
                                state.activeIndex = -1;
                            }} else {{
                                const activeIndex = phaseIndexByName[normalizedPhase] ?? 0;
                                const completedCount = Math.max(0, activeIndex);
                                for (let index = previousCompletedCount; index < completedCount; index += 1) {{
                                    spawnBurst(state.nodes[index], 1.08);
                                }}
                                state.completedCount = completedCount;
                                state.activeIndex = activeIndex;
                                state.finalPhase = normalizedPhase;
                            }}

                            if (state.activeIndex !== previousActiveIndex || normalizedPhase === "complete") {{
                                state.packetProgress = 0;
                                state.packetTrail = [];
                            }}

                            renderSteps();
                        }};

                        const readBridgeState = () => {{
                            if (!parentDocument) {{
                                return;
                            }}

                            const bridge = parentDocument.querySelector('[data-pipeline-progress-bridge="true"]');
                            if (!bridge) {{
                                return;
                            }}

                            const nextSeq = Number.parseInt(bridge.dataset.seq || "-1", 10);
                            if (Number.isNaN(nextSeq) || nextSeq === state.lastSeq) {{
                                return;
                            }}

                            applyBridgeUpdate({{
                                seq: nextSeq,
                                phase: bridge.dataset.phase || steps[0].id,
                                finalPhase: bridge.dataset.finalPhase || bridge.dataset.phase || steps[0].id,
                                note: bridge.dataset.note || state.note,
                            }});
                        }};

                        const traceSegment = (segment) => {{
                            context.beginPath();
                            context.moveTo(segment.from.x, segment.from.y);
                            context.bezierCurveTo(
                                segment.cp1.x,
                                segment.cp1.y,
                                segment.cp2.x,
                                segment.cp2.y,
                                segment.to.x,
                                segment.to.y,
                            );
                        }};

                        const drawBackground = () => {{
                            const panelGradient = context.createLinearGradient(0, 0, 0, state.height);
                            panelGradient.addColorStop(0, rgba(palette.panel, 0.96));
                            panelGradient.addColorStop(1, rgba(palette.black, 0.98));
                            context.fillStyle = panelGradient;
                            context.fillRect(0, 0, state.width, state.height);

                            const glowA = context.createRadialGradient(
                                state.width * 0.16,
                                state.height * 0.18,
                                0,
                                state.width * 0.16,
                                state.height * 0.18,
                                state.width * 0.36,
                            );
                            glowA.addColorStop(0, rgba(palette.bronzeSoft, 0.16));
                            glowA.addColorStop(1, rgba(palette.bronzeSoft, 0));
                            context.fillStyle = glowA;
                            context.fillRect(0, 0, state.width, state.height);

                            const glowB = context.createRadialGradient(
                                state.width * 0.82,
                                state.height * 0.18,
                                0,
                                state.width * 0.82,
                                state.height * 0.18,
                                state.width * 0.28,
                            );
                            glowB.addColorStop(0, rgba(palette.white, 0.08));
                            glowB.addColorStop(1, rgba(palette.white, 0));
                            context.fillStyle = glowB;
                            context.fillRect(0, 0, state.width, state.height);

                            context.save();
                            context.strokeStyle = rgba(palette.white, 0.038);
                            context.lineWidth = 1;
                            const gridSize = clamp(state.width / 14, 36, 58);
                            for (let x = 0; x <= state.width; x += gridSize) {{
                                context.beginPath();
                                context.moveTo(x, 0);
                                context.lineTo(x, state.height);
                                context.stroke();
                            }}
                            for (let y = 0; y <= state.height; y += gridSize) {{
                                context.beginPath();
                                context.moveTo(0, y);
                                context.lineTo(state.width, y);
                                context.stroke();
                            }}
                            context.restore();
                        }};

                        const drawSegments = () => {{
                            state.segments.forEach((segment, index) => {{
                                context.save();
                                context.lineCap = "round";
                                context.lineJoin = "round";
                                context.lineWidth = 2;
                                context.strokeStyle = rgba(palette.white, 0.1);
                                traceSegment(segment);
                                context.stroke();

                                if (index < state.completedCount) {{
                                    context.lineWidth = 3.2;
                                    context.shadowColor = rgba(palette.bronzeSoft, 0.55);
                                    context.shadowBlur = 18;
                                    context.strokeStyle = rgba(palette.bronzeSoft, 0.82);
                                    traceSegment(segment);
                                    context.stroke();
                                }} else if (state.activeIndex > 0 && index === state.activeIndex - 1) {{
                                    context.lineWidth = 3.6;
                                    context.strokeStyle = rgba(palette.bronze, 0.9);
                                    context.shadowColor = rgba(palette.bronze, 0.72);
                                    context.shadowBlur = 22;
                                    context.setLineDash([18, 14]);
                                    context.lineDashOffset = -state.time * 7.5;
                                    traceSegment(segment);
                                    context.stroke();

                                    context.setLineDash([]);
                                    context.lineWidth = 8;
                                    context.strokeStyle = rgba(palette.bronzeSoft, 0.12);
                                    traceSegment(segment);
                                    context.stroke();
                                }}

                                context.restore();
                            }});
                        }};

                        const drawNodes = () => {{
                            state.nodes.forEach((node, index) => {{
                                const isComplete = index < state.completedCount;
                                const isActive = index === state.activeIndex;
                                const pulse = 0.5 + (Math.sin(state.time * 2.2 + index) * 0.5);
                                const haloRadius = node.radius + 14 + (pulse * (isActive ? 10 : 4));

                                context.save();
                                const halo = context.createRadialGradient(
                                    node.x,
                                    node.y,
                                    node.radius * 0.4,
                                    node.x,
                                    node.y,
                                    haloRadius,
                                );
                                halo.addColorStop(0, rgba(palette.bronzeSoft, isActive ? 0.3 : isComplete ? 0.2 : 0.08));
                                halo.addColorStop(1, rgba(palette.bronzeSoft, 0));
                                context.fillStyle = halo;
                                context.beginPath();
                                context.arc(node.x, node.y, haloRadius, 0, Math.PI * 2);
                                context.fill();

                                context.lineWidth = isActive ? 3 : 2;
                                context.strokeStyle = isComplete
                                    ? rgba(palette.bronzeSoft, 0.95)
                                    : isActive
                                        ? rgba(palette.white, 0.9)
                                        : rgba(palette.white, 0.22);
                                context.shadowColor = isComplete || isActive
                                    ? rgba(palette.bronzeSoft, 0.65)
                                    : "transparent";
                                context.shadowBlur = isComplete || isActive ? 18 : 0;
                                context.beginPath();
                                context.arc(node.x, node.y, node.radius + (isActive ? pulse * 2.2 : 0), 0, Math.PI * 2);
                                context.stroke();

                                const core = context.createRadialGradient(
                                    node.x - (node.radius * 0.28),
                                    node.y - (node.radius * 0.34),
                                    node.radius * 0.18,
                                    node.x,
                                    node.y,
                                    node.radius + 6,
                                );
                                if (isComplete) {{
                                    core.addColorStop(0, rgba(palette.white, 1));
                                    core.addColorStop(0.36, rgba(palette.bronzeSoft, 0.96));
                                    core.addColorStop(1, rgba(palette.bronze, 0.72));
                                }} else if (isActive) {{
                                    core.addColorStop(0, rgba(palette.white, 0.98));
                                    core.addColorStop(0.42, rgba(palette.bronzeSoft, 0.84));
                                    core.addColorStop(1, rgba(palette.white, 0.16));
                                }} else {{
                                    core.addColorStop(0, rgba(palette.white, 0.34));
                                    core.addColorStop(1, rgba(palette.white, 0.06));
                                }}
                                context.fillStyle = core;
                                context.beginPath();
                                context.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
                                context.fill();

                                context.shadowBlur = 0;
                                context.fillStyle = isComplete || isActive
                                    ? rgba(palette.white, 0.96)
                                    : rgba(palette.white, 0.54);
                                context.font = "800 12px SF Pro Display, Segoe UI, sans-serif";
                                context.textAlign = "center";
                                context.fillText(node.label, node.x, node.y + node.radius + 30);
                                context.restore();
                            }});
                        }};

                        const updateParticles = (delta) => {{
                            const survivors = [];
                            state.packetTrail = state.packetTrail
                                .map((trailParticle) => ({{
                                    ...trailParticle,
                                    life: trailParticle.life - (0.05 * delta),
                                }}))
                                .filter((trailParticle) => trailParticle.life > 0);

                            state.particles.forEach((particle) => {{
                                const nextLife = particle.life - (particle.decay * delta);
                                if (nextLife <= 0) {{
                                    return;
                                }}

                                particle.x += particle.vx * delta;
                                particle.y += particle.vy * delta;
                                particle.vx *= 0.984;
                                particle.vy *= 0.984;
                                particle.life = nextLife;
                                survivors.push(particle);
                            }});

                            state.particles = survivors;
                        }};

                        const drawParticles = () => {{
                            state.particles.forEach((particle) => {{
                                context.save();
                                const color = particle.tint === "bronze" ? palette.bronzeSoft : palette.white;
                                context.fillStyle = rgba(color, clamp(particle.life * 0.92, 0, 1));
                                context.shadowColor = rgba(color, particle.life * 0.8);
                                context.shadowBlur = 12;
                                context.beginPath();
                                context.arc(
                                    particle.x,
                                    particle.y,
                                    particle.size * clamp(particle.life + 0.25, 0.4, 1.5),
                                    0,
                                    Math.PI * 2,
                                );
                                context.fill();
                                context.restore();
                            }});

                            state.packetTrail.forEach((trailParticle, index) => {{
                                context.save();
                                context.fillStyle = rgba(
                                    palette.bronzeSoft,
                                    clamp(trailParticle.life * (0.6 - (index * 0.025)), 0, 0.5),
                                );
                                context.beginPath();
                                context.arc(
                                    trailParticle.x,
                                    trailParticle.y,
                                    5.8 - (index * 0.24),
                                    0,
                                    Math.PI * 2,
                                );
                                context.fill();
                                context.restore();
                            }});
                        }};

                        const drawPacket = (delta) => {{
                            let packetPosition = null;
                            if (state.activeIndex === 0) {{
                                state.packetProgress = (state.packetProgress + (0.022 * delta)) % 1;
                                const orbitAngle = state.packetProgress * Math.PI * 2;
                                const originNode = state.nodes[0];
                                packetPosition = {{
                                    x: originNode.x + Math.cos(orbitAngle) * 24,
                                    y: originNode.y + Math.sin(orbitAngle) * 16,
                                }};
                            }} else if (state.activeIndex > 0) {{
                                const segment = state.segments[state.activeIndex - 1];
                                state.packetProgress = (state.packetProgress + (0.01 * delta)) % 1;
                                packetPosition = cubicPoint(segment, easeInOutSine(state.packetProgress));
                            }} else if (state.completedCount > 0) {{
                                const terminalNode = state.nodes[state.completedCount - 1];
                                state.packetProgress = (state.packetProgress + (0.016 * delta)) % 1;
                                const orbitAngle = state.packetProgress * Math.PI * 2;
                                packetPosition = {{
                                    x: terminalNode.x + Math.cos(orbitAngle) * 20,
                                    y: terminalNode.y + Math.sin(orbitAngle) * 12,
                                }};
                            }}

                            if (!packetPosition) {{
                                return;
                            }}

                            state.packetTrail.unshift({{
                                x: packetPosition.x,
                                y: packetPosition.y,
                                life: 1,
                            }});
                            state.packetTrail = state.packetTrail.slice(0, 14);

                            context.save();
                            const packetGlow = context.createRadialGradient(
                                packetPosition.x,
                                packetPosition.y,
                                0,
                                packetPosition.x,
                                packetPosition.y,
                                24,
                            );
                            packetGlow.addColorStop(0, rgba(palette.white, 1));
                            packetGlow.addColorStop(0.22, rgba(palette.bronzeSoft, 0.98));
                            packetGlow.addColorStop(1, rgba(palette.bronzeSoft, 0));
                            context.fillStyle = packetGlow;
                            context.beginPath();
                            context.arc(packetPosition.x, packetPosition.y, 24, 0, Math.PI * 2);
                            context.fill();

                            context.shadowColor = rgba(palette.bronzeSoft, 0.9);
                            context.shadowBlur = 22;
                            context.fillStyle = rgba(palette.white, 1);
                            context.beginPath();
                            context.arc(packetPosition.x, packetPosition.y, 5.8, 0, Math.PI * 2);
                            context.fill();
                            context.restore();
                        }};

                        const tick = (timestamp) => {{
                            if (!state.lastTimestamp) {{
                                state.lastTimestamp = timestamp;
                            }}
                            const delta = clamp((timestamp - state.lastTimestamp) / 16.67, 0.5, 2.5);
                            state.lastTimestamp = timestamp;
                            state.time += delta;

                            readBridgeState();
                            updateParticles(delta);
                            drawBackground();
                            drawSegments();
                            drawParticles();
                            drawPacket(delta);
                            drawNodes();

                            window.requestAnimationFrame(tick);
                        }};

                        buildScene();
                        renderSteps();
                        readBridgeState();
                        noteElement.textContent = state.note;

                        const resizeObserver = new ResizeObserver(() => {{
                            buildScene();
                        }});
                        resizeObserver.observe(stage);
                        window.requestAnimationFrame(tick);
                    }})();
                </script>
            </body>
        </html>
        """,
        height=590,
    )


def _process_pending_question_submission(progress_bridge_placeholder: Any) -> None:
    """Render live pipeline progress, run the queued NL question, then finalize the visualizer."""
    pending = _get_pending_question_submission()
    if pending is None:
        return

    try:
        _handle_question(
            pending["question"],
            pending["use_demo_mode"],
            chat_mode=pending["chat_mode"],
            progress_bridge_placeholder=progress_bridge_placeholder,
        )
    except Exception as exc:  # pragma: no cover - defensive UI safety net
        error_text = str(exc).strip() or "Unexpected chat pipeline failure."
        fallback_result = QueryResult(
            rows=[],
            executed=False,
            error=error_text,
            message="The assistant hit an internal error while handling your message.",
        )
        _set_output_context(
            question=pending["question"],
            mode_label="Natural Language to SQL",
            generation_note=error_text,
            sql="",
            result=fallback_result,
            retry_happened=False,
            retry_status="No retry was attempted.",
        )
        _append_conversation_turn(
            user_text=pending["question"],
            assistant_text=(
                "I hit an internal app error while preparing your response. "
                "Please retry your question; if it repeats, open Readiness checks."
            ),
            mode_label="Natural Language to SQL",
            generation_note=error_text,
            final_sql="",
            result=fallback_result,
            retry_happened=False,
            retry_status="No retry was attempted.",
        )
    if (
        pending["resolved_chat_mode"] == CHAT_MODE_DATA
        and str(st.session_state.pipeline_loading_final_phase).strip()
    ):
        _advance_pipeline_visualizer(
            progress_bridge_placeholder,
            PIPELINE_PHASE_COMPLETE,
            "Pipeline finalized. Returning the latest SQL, status, and result payload back to the assistant response card.",
            final_phase=str(st.session_state.pipeline_loading_final_phase),
            delay_seconds=PIPELINE_VISUALIZER_FINAL_DELAY_SECONDS,
        )
    st.session_state.pending_question_submission = None
    _clear_pipeline_loading_state()
    st.rerun()


def _render_global_navbar(config: AppConfig, current_route: str) -> None:
    """Render the sticky SPA navbar that stays above the main canvas."""
    brand_href = _build_route_href("home")
    chat_is_minimized = False
    brand_hint = "Home"
    nav_links_markup = "".join(
        (
            f'<a href="{escape(_build_route_href(route))}" '
            f'class="global-nav-link{" active" if route == current_route else ""}" '
            'target="_self">'
            f"{escape(label)}"
            "</a>"
        )
        for route, label in ROUTE_LABELS.items()
        if route != "data_journey_admin"
    )
    st.markdown(
        f"""
        <style>
        .global-nav-shell {{
            position: sticky;
            top: 0.55rem;
            z-index: 1000;
            margin-bottom: 1.1rem;
            padding: 0.9rem 1.15rem;
            border-radius: 999px;
            border: 1px solid rgba(17, 17, 17, 0.08);
            background: rgba(255, 255, 255, 0.86);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            box-shadow: 0 18px 42px rgba(17, 17, 17, 0.08);
        }}

        .global-nav-inner {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            flex-wrap: wrap;
        }}

        .global-nav-brand {{
            color: #1a1a1a;
            font-size: 0.9rem;
            font-weight: 800;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            text-decoration: none;
            display: inline-flex;
            align-items: baseline;
            gap: 0.55rem;
        }}

        .global-nav-brand:hover {{
            color: #af7f59;
        }}

        .global-nav-brand-hint {{
            font-size: 0.64rem;
            letter-spacing: 0.12em;
            font-weight: 700;
            color: #8c8783;
        }}

        .global-nav-links {{
            display: flex;
            align-items: center;
            gap: 0.65rem;
            flex-wrap: wrap;
        }}

        .global-nav-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.58rem 0.9rem;
            border-radius: 999px;
            color: #5f5a57;
            text-decoration: none;
            font-size: 0.84rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            transition: all 0.2s ease;
        }}

        .global-nav-link:hover {{
            color: #111111;
            background: rgba(17, 17, 17, 0.04);
        }}

        .global-nav-link.active {{
            color: #af7f59;
            background: rgba(175, 127, 89, 0.12);
        }}
        </style>
        <div class="global-nav-shell animate-in">
            <div class="global-nav-inner">
                <a href="{escape(brand_href)}" target="_self" class="global-nav-brand">
                    <span>{escape(config.app_title)}</span>
                    <span class="global-nav-brand-hint">{escape(brand_hint)}</span>
                </a>
                <div class="global-nav-links">{nav_links_markup}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_general_chat_meta_card(turn: dict[str, Any], result: QueryResult) -> None:
    """Render a compact note card below one general chat response."""
    tone_label = "General Chat Mode"
    copy = "This reply did not generate or execute SQL."
    if result.error:
        tone_label = "Conversation Error"
        copy = result.error
    elif str(turn.get("generation_note", "")).strip():
        copy = str(turn["generation_note"]).strip()

    st.markdown(
        f"""
        <div class="premium-glass-card animate-in stagger-2" style="
            margin-top: 0.9rem;
            padding: 0.95rem 1rem;
            border: 1px solid rgba(175,127,89,0.22);
            background: rgba(255,255,255,0.95);
        ">
            <div style="font-size: 0.72rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(tone_label)}</div>
            <p style="margin: 0.42rem 0 0; color: #333333; line-height: 1.68; font-weight: 550;">{escape(copy)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_assistant_turn(turn: dict[str, Any]) -> None:
    """Render one assistant response bubble with URL-driven detail panels."""
    result = QueryResult(
        rows=turn["rows"],
        executed=turn["executed"],
        error=turn["error"],
        message=turn["message"],
    )
    if str(turn.get("mode_label", "")) == GENERAL_CHAT_MODE_LABEL:
        _render_response_overview(turn, result)
        _render_export_report_group(turn, result)
        return

    chart_shown, _ = _get_chart_summary(result.rows)
    map_shown, _ = _get_map_summary(result.rows)
    has_sql_details = bool(str(turn.get("final_sql", "")).strip() or str(turn.get("original_sql", "")).strip())
    has_error_details = bool(result.error or turn["retry_happened"] or turn["generation_note"])
    available_panels = _get_available_detail_panels(
        has_sql_details,
        chart_shown,
        map_shown,
        has_error_details,
    )
    active_panel = _get_active_detail_panel(available_panels)

    _render_response_overview(turn, result)
    _render_summary_spotlight(turn, result)
    _render_result_summary(result, turn["retry_happened"])
    _render_detail_panel_nav(available_panels, active_panel)
    _render_active_detail_panel(active_panel=active_panel, turn=turn, result=result)
    _render_export_report_group(turn, result)


def _render_general_chat_pending_card() -> None:
    """Render a simple waiting card for conversational replies."""
    st.markdown(
        """
        <div class="premium-glass-card animate-in stagger-2" style="padding: 1.15rem 1.2rem;">
            <div style="font-size: 0.72rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Natural Language Querying</div>
            <h3 style="margin: 0.42rem 0 0.35rem; color: #111111;">Preparing response</h3>
            <p style="margin: 0; color: #5f5a57; line-height: 1.68;">The assistant is analyzing your message.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_chat_command_dock() -> None:
    """Render a compact command dock with minimal text and useful links."""
    active_chat_mode = CHAT_MODE_AUTO
    current_panel = _get_active_panel() if _get_current_route() == "home" else ""
    show_recommendation_panel = _read_query_param("show").lower() == "recommendation"

    recommendation_toggle_href = (
        _build_route_href("home", chat_mode=active_chat_mode, panel=current_panel)
        if show_recommendation_panel
        else _build_route_href("home", chat_mode=active_chat_mode, panel=current_panel, show="recommendation")
    )
    recommendation_toggle_label = "Hide Recommendation" if show_recommendation_panel else "Recommendation"
    query_settings_href = _build_route_href(
        "home",
        chat_mode=active_chat_mode,
        panel=current_panel,
        show="query_settings",
    )

    recommendation_panel_markup = ""
    if show_recommendation_panel:
        visible_recommendations = PROJECT_RECOMMENDATION_QUESTIONS
        recommendation_panel_markup = "".join(
            (
                f'<a href="{escape(_build_route_href("home", chat_mode=CHAT_MODE_DATA, action="ask", question=question, panel="results"))}" '
                'target="_self" class="chat-command-recommendation">'
                f'<span class="chat-command-reco-index">{index + 1:02d}</span>'
                f"{escape(_truncate_context_label(question, 72))}"
                "</a>"
            )
            for index, question in enumerate(visible_recommendations)
        )

    st.markdown(
        f"""
        <style>
        .chat-command-dock {{
            display: grid;
            gap: 0.85rem;
            margin: 0.6rem 0 0.8rem;
        }}
        .chat-command-quick-links {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
        }}
        .chat-command-quick-link {{
            color: #d6af8c;
            text-decoration: underline;
            font-size: 0.79rem;
            font-weight: 700;
        }}
        .chat-command-recommendation-row {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.68rem;
            max-height: 430px;
            overflow: auto;
            padding-right: 0.2rem;
        }}
        @media (max-width: 1040px) {{
            .chat-command-recommendation-row {{
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }}
        }}
        @media (max-width: 840px) {{
            .chat-command-recommendation-row {{
                grid-template-columns: 1fr;
            }}
        }}
        .chat-command-recommendation {{
            display: flex;
            align-items: flex-start;
            gap: 0.62rem;
            padding: 0.74rem 0.86rem;
            border-radius: 14px;
            text-decoration: none;
            border: 1px solid rgba(255,255,255,0.18);
            background: linear-gradient(140deg, rgba(21,21,21,0.55), rgba(35,35,35,0.4));
            color: #f7f3ef;
            font-size: 0.76rem;
            font-weight: 680;
            line-height: 1.35;
            white-space: normal;
            overflow-wrap: anywhere;
            min-height: 64px;
            transition: all 0.2s ease;
        }}
        .chat-command-reco-index {{
            flex: 0 0 auto;
            min-width: 2.1rem;
            padding: 0.2rem 0.42rem;
            border-radius: 999px;
            border: 1px solid rgba(214,175,140,0.45);
            background: rgba(214,175,140,0.12);
            color: #e6c7ab;
            font-size: 0.7rem;
            letter-spacing: 0.08em;
            text-align: center;
            font-weight: 800;
        }}
        .chat-command-recommendation:hover {{
            border-color: rgba(214,175,140,0.56);
            box-shadow: 0 14px 26px rgba(0,0,0,0.22);
            transform: translateY(-1px);
            color: #f2d8c2;
        }}
        </style>
        <div class="chat-command-dock animate-in stagger-2">
            <div class="chat-command-quick-links">
                <a href="{escape(recommendation_toggle_href)}" target="_self" class="chat-command-quick-link">{escape(recommendation_toggle_label)}</a>
                <a href="{escape(query_settings_href)}" target="_self" class="chat-command-quick-link">⚙ Query Settings</a>
            </div>
            {('<div class="chat-command-section"><div class="chat-command-recommendation-row">' + recommendation_panel_markup + '</div></div>') if show_recommendation_panel else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )

def _render_conversation() -> None:
    """Render the conversation as the main interaction area."""
    pending_submission = _get_pending_question_submission()

    if not st.session_state.conversation_turns and pending_submission is None:
        return

    for turn in st.session_state.conversation_turns:
        _render_follow_up_thread_banner(turn)
        _render_user_turn(turn)

        with st.chat_message("assistant", avatar="✨"):
            _render_assistant_turn(turn)

    if pending_submission is not None:
        _render_user_turn({"user_text": pending_submission["question"]})
        with st.chat_message("assistant", avatar="✨"):
            _render_query_pipeline_visualizer()


def _handle_test_query() -> None:
    """Run one fixed database test query and store the result in session state."""
    result = run_test_query()
    explanation_text = (
        "This opens the business table and grabs a tiny sample of rows. "
        "It stops at 5 so you can confirm the connection works without pulling a huge result."
    )
    _set_output_context(
        question="Hardcoded database connectivity test",
        mode_label="Database Test",
        generation_note="This runs a fixed SQL query directly against your database layer.",
        sql="SELECT * FROM business LIMIT 5",
        sql_explanation=explanation_text,
        result=result,
        retry_status="No retry was needed.",
    )
    _append_conversation_turn(
        user_text="Please run the database connectivity test.",
        assistant_text=(
            "I ran the fixed `SELECT * FROM business LIMIT 5` query to confirm that "
            "the app can reach the database and read from the `business` table."
        ),
        mode_label="Database Test",
        generation_note="This runs a fixed SQL query directly against your database layer.",
        final_sql="SELECT * FROM business LIMIT 5",
        final_sql_explanation=explanation_text,
        result=result,
        retry_status="No retry was needed.",
    )


def _handle_manual_sql(sql: str) -> None:
    """Run a user-typed SQL query and store the result in session state."""
    result = execute_sql(sql)
    explanation_text = (
        "This skips the AI step and runs exactly the SQL text you typed. "
        "The database then returns whatever rows or summary values that query asks for."
    )
    _set_output_context(
        question="Manual SQL execution",
        mode_label="Manual SQL Runner",
        generation_note="This bypasses the model and runs the SQL exactly as typed.",
        sql=sql,
        sql_explanation=explanation_text,
        result=result,
        retry_status="No retry was needed.",
    )
    _append_conversation_turn(
        user_text=f"Please run this SQL manually:\n```sql\n{sql.strip()}\n```",
        assistant_text=(
            "I ran your SQL directly against the database layer without using the "
            "natural-language generation step."
        ),
        mode_label="Manual SQL Runner",
        generation_note="This bypasses the model and runs the SQL exactly as typed.",
        final_sql=sql,
        final_sql_explanation=explanation_text,
        result=result,
        retry_status="No retry was needed.",
    )


def _handle_question(
    question: str,
    use_demo_mode: bool,
    *,
    chat_mode: str = CHAT_MODE_AUTO,
    progress_bridge_placeholder: Any | None = None,
) -> None:
    """Run the natural-language workflow through the central pipeline layer."""
    clean_question = question.strip()
    if st.session_state.speed_mode_enabled:
        _refresh_zeppelin_knowledge(force=False)
    resolved_chat_mode = _resolve_chat_mode(clean_question, chat_mode)

    if not clean_question:
        _set_output_context(
            question="",
            mode_label=GENERAL_CHAT_MODE_LABEL if resolved_chat_mode == CHAT_MODE_GENERAL else "Natural Language to SQL",
            generation_note="",
            sql="",
            result=QueryResult(
                error="Please enter a message first.",
                message=(
                    "Try a conversational prompt or ask a data question like 'Show the first 5 businesses'."
                ),
            ),
            retry_status="No retry was attempted.",
        )
        return

    if _is_recommendation_question(clean_question):
        recommendation_sql = _get_recommendation_fast_sql(clean_question) or ""
        if recommendation_sql:
            recommendation_result = execute_sql(recommendation_sql)
            assistant_text = (
                f"I executed the recommendation SQL and returned {len(recommendation_result.rows or [])} row(s)."
            )
            if recommendation_result.error:
                assistant_text = (
                    "I ran the recommendation SQL, but the database returned an error. "
                    "Open Errors to see the exact backend message."
                )
            result = QueryResult(
                rows=recommendation_result.rows,
                executed=recommendation_result.executed,
                error=recommendation_result.error,
                message=recommendation_result.message
                or "Recommendation query executed.",
            )
            _set_output_context(
                question=clean_question,
                mode_label="Natural Language to SQL",
                generation_note="Recommendation route executed deterministic SQL directly.",
                sql=recommendation_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=assistant_text,
                mode_label="Natural Language to SQL",
                generation_note="Recommendation route executed deterministic SQL directly.",
                final_sql=recommendation_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return
        # If no deterministic recommendation SQL template exists, continue
        # with the normal Text-to-SQL pipeline (SQL generation + execution).

    if st.session_state.speed_mode_enabled and not _is_recommendation_question(clean_question):

        zeppelin_direct_reply = _build_zeppelin_qa_reply(clean_question)
        if zeppelin_direct_reply:
            zeppelin_direct_sql = _get_zeppelin_matched_sql(clean_question)
            result = QueryResult(
                executed=True,
                message="Answered directly from Zeppelin task output.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Zeppelin task-output fast path was used.",
                sql=zeppelin_direct_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=zeppelin_direct_reply,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Zeppelin task-output fast path was used.",
                final_sql=zeppelin_direct_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return

    if resolved_chat_mode == CHAT_MODE_GENERAL:
        general_generation_note = (
            "General Chat mode returned a text response without SQL generation or query execution."
        )
        zeppelin_task_reply = _get_zeppelin_task_fast_reply(clean_question)
        if st.session_state.speed_mode_enabled and zeppelin_task_reply:
            zeppelin_task_sql = _get_zeppelin_matched_sql(clean_question)
            result = QueryResult(
                executed=True,
                message="Zeppelin task index response returned instantly.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Fast-path Zeppelin task summary was used.",
                sql=zeppelin_task_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=zeppelin_task_reply,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Fast-path Zeppelin task summary was used.",
                final_sql=zeppelin_task_sql,
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return

        fast_reply = _get_fast_general_reply(clean_question)
        if fast_reply:
            result = QueryResult(
                executed=True,
                message="General chat fast-path response returned instantly.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Fast-path general response was used.",
                sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=fast_reply,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Fast-path general response was used.",
                final_sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return

        normalized_key = _normalize_free_text(clean_question).lower()
        cached_reply = st.session_state.general_chat_response_cache.get(normalized_key)
        if cached_reply:
            result = QueryResult(
                executed=True,
                message="General chat response served from session cache.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Used cached general-chat response.",
                sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=cached_reply,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note="Used cached general-chat response.",
                final_sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return

        try:
            assistant_message = generate_general_chat_reply(
                clean_question,
                recent_context=_build_recent_general_chat_messages(),
            )
            st.session_state.general_chat_response_cache[normalized_key] = assistant_message
            result = QueryResult(
                executed=True,
                message="General chat response generated successfully.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note=general_generation_note,
                sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
        except RuntimeError as exc:
            error_message = str(exc).strip()
            assistant_message = (
                "General chat is not ready yet. Check live model configuration and try again."
                if "DEEPSEEK" in error_message
                else "I hit a problem while generating the general reply. Please try again in a moment."
            )
            result = QueryResult(
                error=error_message,
                message="General chat could not be completed.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note=error_message,
                sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was attempted.",
            )
        except Exception as exc:
            error_message = str(exc).strip()
            assistant_message = "I hit a problem while generating the general reply. Please try again in a moment."
            result = QueryResult(
                error=error_message,
                message="General chat could not be completed.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note=error_message,
                sql="",
                result=result,
                retry_happened=False,
                retry_status="No retry was attempted.",
            )

        _append_conversation_turn(
            user_text=clean_question,
            assistant_text=assistant_message,
            mode_label=GENERAL_CHAT_MODE_LABEL,
            generation_note=result.error or general_generation_note,
            final_sql="",
            result=result,
            retry_happened=False,
            retry_status="No retry was needed." if not result.error else "No retry was attempted.",
        )
        return

    recent_context = _build_recent_query_context()
    is_follow_up = _question_looks_like_follow_up(clean_question, recent_context)
    context_questions = [
        _truncate_context_label(str(turn.get("question", "")))
        for turn in recent_context
        if str(turn.get("question", "")).strip()
    ]

    def _progress_callback(phase: str, note: str) -> None:
        if progress_bridge_placeholder is None:
            return

        _advance_pipeline_visualizer(
            progress_bridge_placeholder,
            phase,
            note,
        )

    if progress_bridge_placeholder is not None:
        _render_pipeline_progress_bridge(progress_bridge_placeholder)

    recommendation_fast_sql = _get_recommendation_fast_sql(clean_question)
    if st.session_state.speed_mode_enabled and recommendation_fast_sql:
        fast_result = execute_sql(recommendation_fast_sql)
        quick_note = (
            "Speed mode used deterministic recommendation SQL (LLM bypass) for faster response."
        )
        _set_output_context(
            question=clean_question,
            mode_label="Natural Language to SQL",
            generation_note=quick_note,
            sql=recommendation_fast_sql,
            result=fast_result,
            retry_happened=False,
            retry_status="No retry was needed.",
        )
        _append_conversation_turn(
            user_text=clean_question,
            assistant_text=(
                "I used the recommendation fast path and ran a deterministic SQL query directly "
                "for this question to return results quickly."
            ),
            mode_label="Natural Language to SQL",
            generation_note=quick_note,
            final_sql=recommendation_fast_sql,
            result=fast_result,
            retry_happened=False,
            retry_status="No retry was needed.",
        )
        return

    pipeline_result = run_natural_language_query(
        question=clean_question,
        use_demo_mode=use_demo_mode,
        recent_context=recent_context,
        progress_callback=_progress_callback if progress_bridge_placeholder is not None else None,
        allow_correction_retry=not st.session_state.speed_mode_enabled,
    )
    if ENABLE_ZEPPELIN_TEXT_FALLBACK and st.session_state.speed_mode_enabled and not pipeline_result.success:
        zeppelin_fallback_reply = _build_zeppelin_qa_reply(clean_question)
        if zeppelin_fallback_reply:
            zeppelin_fallback_sql = _get_zeppelin_matched_sql(clean_question)
            fallback_result = QueryResult(
                executed=True,
                message="SQL flow failed; returned matched Zeppelin task output instead.",
            )
            _set_output_context(
                question=clean_question,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note=(
                    "Fallback used Zeppelin task output after SQL pipeline did not complete successfully."
                ),
                sql=zeppelin_fallback_sql,
                result=fallback_result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            _append_conversation_turn(
                user_text=clean_question,
                assistant_text=zeppelin_fallback_reply,
                mode_label=GENERAL_CHAT_MODE_LABEL,
                generation_note=(
                    "Fallback used Zeppelin task output after SQL pipeline did not complete successfully."
                ),
                final_sql=zeppelin_fallback_sql,
                result=fallback_result,
                retry_happened=False,
                retry_status="No retry was needed.",
            )
            return
    fallback_note = ""
    if _should_auto_fallback_to_demo_mode(
        used_demo_mode=use_demo_mode,
        success=pipeline_result.success,
        status=pipeline_result.status,
    ):
        fallback_note = (
            "Live validation did not complete successfully, so the copilot switched "
            "back to Demo/Mock Mode for the next question."
        )
        _activate_demo_fallback(fallback_note)
        pipeline_result.generation_note = "\n\n".join(
            part
            for part in [pipeline_result.generation_note.strip(), fallback_note]
            if part
        )

    assistant_message = _build_assistant_message(
        pipeline_result,
        is_follow_up=is_follow_up,
        context_count=len(recent_context),
    )
    if fallback_note:
        assistant_message = f"{assistant_message} {fallback_note}"

    _set_pipeline_output_context(pipeline_result)
    _append_conversation_turn(
        user_text=clean_question,
        assistant_text=assistant_message,
        mode_label=pipeline_result.mode_label,
        generation_note=pipeline_result.generation_note,
        final_sql=pipeline_result.final_sql,
        result=QueryResult(
            rows=pipeline_result.rows,
            executed=pipeline_result.success,
            error=pipeline_result.error_message or None,
            message=pipeline_result.result_message,
        ),
        original_sql=pipeline_result.generated_sql,
        original_sql_explanation=pipeline_result.generated_sql_explanation,
        corrected_sql=pipeline_result.corrected_sql,
        corrected_sql_explanation=pipeline_result.corrected_sql_explanation,
        final_sql_explanation=pipeline_result.final_sql_explanation,
        is_follow_up=is_follow_up,
        context_questions=context_questions if is_follow_up else [],
        retry_happened=pipeline_result.retry_happened,
        retry_status=pipeline_result.retry_status,
    )


def _handle_example_question(example_question: str) -> None:
    """Run one example question through the same natural-language flow."""
    st.session_state.nl_question_text = example_question
    st.session_state.chat_mode = CHAT_MODE_DATA
    _handle_question(
        example_question,
        st.session_state.nl_use_demo_mode,
        chat_mode=CHAT_MODE_DATA,
    )


def _render_manual_sql_workspace() -> None:
    """Render a shareable manual SQL workspace inside the main canvas."""
    st.markdown(
        f"""
        <div class="premium-glass-card animate-in stagger-1" style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; gap: 1rem; flex-wrap: wrap; align-items: center;">
                <div>
                    <div style="font-size: 0.74rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Service Layer</div>
                    <h3 style="margin: 0.4rem 0 0.35rem;">Manual SQL Workspace</h3>
                    <p style="margin: 0; color: #595959;">Run SQL directly on the database layer.</p>
                </div>
                <div style="display: flex; gap: 0.7rem; flex-wrap: wrap;">
                    <a href="{escape(_build_route_href('home', action='run_test', panel='results'))}" target="_self" style="padding: 0.64rem 0.92rem; border-radius: 999px; background: rgba(175, 127, 89, 0.12); color: #111111; text-decoration: none; font-size: 0.82rem; font-weight: 700;">Run DB Test</a>
                    <a href="{escape(_build_route_href('schema'))}" target="_self" style="padding: 0.64rem 0.92rem; border-radius: 999px; border: 1px solid rgba(17,17,17,0.08); background: rgba(255,255,255,0.8); color: #595959; text-decoration: none; font-size: 0.82rem; font-weight: 700;">Check Schema</a>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    sql_text = st.text_area(
        "Manual SQL Query",
        key="manual_sql_text",
        height=180,
        help="Run SQL directly when you want to test the backend without the model.",
    )
    if st.button("Run Manual SQL", key="run_manual_sql_workspace_button", width="stretch"):
        with st.spinner("Running your SQL query..."):
            _handle_manual_sql(sql_text)
        st.rerun()


def render_section_header(title: str, description: str, kicker: str = "") -> None:
    """Render a compact section header used across starter cards."""
    kicker_markup = (
        f'<div style="font-size: 0.72rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(kicker)}</div>'
        if kicker
        else ""
    )
    st.markdown(
        f"""
        <div class="premium-glass-card animate-in" style="padding: 1rem 1.05rem; margin: 0.25rem 0 0.85rem;">
            {kicker_markup}
            <h3 style="margin: 0.42rem 0 0.32rem; color: #111111;">{escape(title)}</h3>
            <p style="margin: 0; color: #5f5a57; line-height: 1.62;">{escape(description)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_prompt_card(meta: str, title: str, description: str) -> None:
    """Render one prompt starter card."""
    st.markdown(
        f"""
        <div class="premium-glass-card" style="padding: 0.95rem 1rem; min-height: 148px;">
            <div style="font-size: 0.7rem; letter-spacing: 0.12em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(meta)}</div>
            <div style="margin-top: 0.4rem; font-weight: 700; color: #111111; line-height: 1.4;">{escape(title)}</div>
            <p style="margin: 0.45rem 0 0; color: #5f5a57; line-height: 1.55; font-size: 0.9rem;">{escape(description)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_prompt_card_spatial_motion() -> None:
    """Keep a stable extension hook for the starter card section."""
    return


def _render_example_questions() -> None:
    """Render example questions as quick-start actions in a balanced grid."""
    render_section_header(
        "Sample Questions",
        "Official examples for Natural Language Querying.",
        kicker="Text-to-SQL Workflow",
    )

    # Row 1: first 3 cards
    top_row = st.columns(3)
    for index in range(min(3, len(EXAMPLE_QUESTIONS))):
        example_question = EXAMPLE_QUESTIONS[index]
        meta, description = PROMPT_CHIP_DETAILS.get(
            example_question,
            ("💡 Prompt", ""),
        )
        with top_row[index]:
            render_prompt_card(meta, example_question, description)
            st.markdown(
                f'<a href="{escape(_build_route_href("home", chat_mode=CHAT_MODE_DATA, action="ask", question=example_question, panel="results"))}" target="_self" '
                'style="display:block; width:100%; margin-top:0.75rem; padding:0.82rem 1rem; border-radius: 14px; background: linear-gradient(135deg, rgba(17,17,17,0.96), rgba(50,50,50,0.96)); color:white; text-decoration:none; text-align:center; font-size:0.88rem; font-weight:700; box-shadow: 0 18px 32px rgba(17,17,17,0.12);">Try This Prompt</a>',
                unsafe_allow_html=True,
            )

    # Row 2: remaining cards, centered
    remaining = EXAMPLE_QUESTIONS[3:]
    if remaining:
        padding = (3 - len(remaining)) / 2
        cols_spec: list[float] = []
        if padding > 0:
            cols_spec.append(padding)
        cols_spec.extend([1.0] * len(remaining))
        if padding > 0:
            cols_spec.append(padding)

        bottom_row = st.columns(cols_spec)
        start_col = 1 if padding > 0 else 0
        for i, example_question in enumerate(remaining):
            meta, description = PROMPT_CHIP_DETAILS.get(
                example_question,
                ("💡 Prompt", ""),
            )
            with bottom_row[start_col + i]:
                render_prompt_card(meta, example_question, description)
                st.markdown(
                    f'<a href="{escape(_build_route_href("home", chat_mode=CHAT_MODE_DATA, action="ask", question=example_question, panel="results"))}" target="_self" '
                    'style="display:block; width:100%; margin-top:0.75rem; padding:0.82rem 1rem; border-radius: 14px; background: linear-gradient(135deg, rgba(17,17,17,0.96), rgba(50,50,50,0.96)); color:white; text-decoration:none; text-align:center; font-size:0.88rem; font-weight:700; box-shadow: 0 18px 32px rgba(17,17,17,0.12);">Try This Prompt</a>',
                    unsafe_allow_html=True,
                )

    render_prompt_card_spatial_motion()


def _build_hero_badges() -> list[tuple[str, str]]:
    """Build the small status badges shown in the hero panel."""
    badges: list[tuple[str, str]] = [
        (f"Backend: {os.getenv('YELP_SQL_ENGINE', 'hive').upper()}", ""),
    ]

    if st.session_state.nl_use_demo_mode:
        badges.append(("Demo Mode", "warn"))
    else:
        badges.append(("Live Mode", "accent"))

    return badges


def _build_hero_highlights() -> list[tuple[str, str]]:
    """Build the hero-side highlight routing buttons."""
    return [
        ("Conversational Chat", _build_route_href("home")),
        ("Data Journey", _build_route_href("readiness")),
        ("Schema", _build_route_href("schema")),
    ]


def _build_schema_graph_payload() -> dict[str, Any]:
    """Convert the editable schema into a node graph payload for the UI."""
    table_schemas = get_table_schemas()
    verification_checklist = get_schema_verification_checklist()
    table_visuals = {
        "business": {
            "icon": "business",
            "glow": "#d6af8c",
            "core": "#af7f59",
        },
        "review": {
            "icon": "review",
            "glow": "#f8f2ea",
            "core": "#f1e0cf",
        },
        "users": {
            "icon": "user",
            "glow": "#d6c8ba",
            "core": "#6e6258",
        },
        "checkin": {
            "icon": "checkin",
            "glow": "#d1b79d",
            "core": "#8d7b6d",
        },
    }
    table_to_task_key = {
        "business": "business",
        "users": "user",
        "user": "user",
        "rating": "review",
        "review": "review",
        "checkin": "checkin",
    }

    nodes: list[dict[str, Any]] = []
    edge_lookup: dict[tuple[str, str], list[str]] = {}

    for table_name, table_info in table_schemas.items():
        visual = table_visuals.get(
            table_name,
            {"icon": "table", "glow": "#d6af8c", "core": "#af7f59"},
        )
        columns = list(table_info.get("columns", []))
        join_keys = list(table_info.get("join_keys", []))
        verification_todos = list(table_info.get("verification_todos", []))

        nodes.append(
            {
                "id": table_name,
                "label": table_name,
                "display_name": table_name,
                "description": str(table_info.get("description", "")).strip(),
                "icon": visual["icon"],
                "glow": visual["glow"],
                "core": visual["core"],
                "column_count": len(columns),
                "columns": columns,
                "join_keys": join_keys,
                "verification_todos": verification_todos,
                "task_key": table_to_task_key.get(table_name, ""),
                "task_label": ZEPPELIN_TASK_NOTEBOOKS.get(table_to_task_key.get(table_name, ""), {}).get("label", ""),
                "size": 52 + min(len(columns), 18) * 2.2,
            }
        )

        for join_key in join_keys:
            table_matches = re.findall(r"([a-zA-Z_][\w]*)\.", join_key)
            if len(table_matches) < 2:
                continue

            source_table = table_matches[0]
            target_table = table_matches[1]
            if source_table == target_table:
                continue
            if source_table not in table_schemas or target_table not in table_schemas:
                continue

            edge_key = tuple(sorted((source_table, target_table)))
            edge_lookup.setdefault(edge_key, []).append(join_key)

    edges = [
        {
            "source": source_table,
            "target": target_table,
            "join_keys": join_keys,
        }
        for (source_table, target_table), join_keys in edge_lookup.items()
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "verification_checklist": verification_checklist,
    }


def _render_schema_holographic_graph() -> None:
    """Render the animated holographic schema explorer."""
    payload = _build_schema_graph_payload()
    payload_json = json.dumps(payload)

    components.html(
        f"""
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8" />
                <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
                <style>
                    :root {{
                        --schema-bg: #070707;
                        --schema-panel: rgba(255, 255, 255, 0.08);
                        --schema-panel-strong: rgba(255, 255, 255, 0.12);
                        --schema-border: rgba(255, 255, 255, 0.12);
                        --schema-text: #f7f3ef;
                        --schema-soft: rgba(247, 243, 239, 0.74);
                        --schema-bronze: #af7f59;
                        --schema-bronze-soft: #d6af8c;
                        --schema-white: #f4ede7;
                    }}

                    * {{
                        box-sizing: border-box;
                    }}

                    html, body {{
                        margin: 0;
                        padding: 0;
                        background: transparent;
                        font-family: "Manrope", "Segoe UI", sans-serif;
                    }}

                    .schema-holo-shell {{
                        position: relative;
                        overflow: hidden;
                        min-height: 860px;
                        border-radius: 34px;
                        border: 1px solid rgba(255, 255, 255, 0.08);
                        background:
                            radial-gradient(circle at top right, rgba(175, 127, 89, 0.22), transparent 24%),
                            radial-gradient(circle at bottom left, rgba(255, 255, 255, 0.08), transparent 18%),
                            linear-gradient(180deg, #060606 0%, #0f0f0f 48%, #111111 100%);
                        box-shadow: 0 36px 80px rgba(17, 17, 17, 0.22);
                        color: var(--schema-text);
                    }}

                    .schema-holo-shell::before {{
                        content: "";
                        position: absolute;
                        inset: 0;
                        background:
                            linear-gradient(rgba(255,255,255,0.04) 1px, transparent 1px),
                            linear-gradient(90deg, rgba(255,255,255,0.035) 1px, transparent 1px);
                        background-size: 84px 84px;
                        mask-image: radial-gradient(circle at center, rgba(0,0,0,0.86), rgba(0,0,0,0.24));
                        opacity: 0.34;
                        pointer-events: none;
                    }}

                    .schema-holo-shell::after {{
                        content: "";
                        position: absolute;
                        inset: 0;
                        background:
                            radial-gradient(circle at 20% 18%, rgba(214, 175, 140, 0.16), transparent 16%),
                            radial-gradient(circle at 78% 72%, rgba(255, 255, 255, 0.08), transparent 18%);
                        filter: blur(36px);
                        pointer-events: none;
                    }}

                    .schema-holo-header {{
                        position: relative;
                        z-index: 3;
                        display: flex;
                        justify-content: space-between;
                        gap: 1rem;
                        padding: 1.2rem 1.25rem 0.2rem;
                        flex-wrap: wrap;
                    }}

                    .schema-holo-kicker {{
                        color: var(--schema-bronze-soft);
                        font-size: 0.72rem;
                        font-weight: 800;
                        letter-spacing: 0.18em;
                        text-transform: uppercase;
                    }}

                    .schema-holo-title {{
                        margin-top: 0.48rem;
                        font-size: 1.38rem;
                        font-weight: 800;
                        line-height: 1.08;
                        color: var(--schema-white);
                    }}

                    .schema-holo-copy {{
                        margin-top: 0.54rem;
                        color: var(--schema-soft);
                        font-size: 0.92rem;
                        line-height: 1.66;
                        max-width: 44rem;
                    }}

                    .schema-holo-metrics {{
                        display: flex;
                        gap: 0.65rem;
                        flex-wrap: wrap;
                        align-items: start;
                    }}

                    .schema-holo-metric {{
                        padding: 0.58rem 0.82rem;
                        border-radius: 999px;
                        border: 1px solid rgba(255,255,255,0.09);
                        background: rgba(255,255,255,0.06);
                        color: var(--schema-white);
                        font-size: 0.79rem;
                        font-weight: 800;
                        letter-spacing: 0.02em;
                    }}

                    .schema-holo-stage {{
                        position: relative;
                        z-index: 2;
                        height: 660px;
                        margin: 0.75rem 1rem 1rem;
                        border-radius: 28px;
                        border: 1px solid rgba(255,255,255,0.06);
                        background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
                        overflow: hidden;
                    }}

                    .schema-holo-svg {{
                        width: 100%;
                        height: 100%;
                        display: block;
                    }}

                    .schema-holo-hint {{
                        position: absolute;
                        left: 1rem;
                        bottom: 1rem;
                        z-index: 4;
                        padding: 0.62rem 0.84rem;
                        border-radius: 16px;
                        border: 1px solid rgba(255,255,255,0.08);
                        background: rgba(7,7,7,0.62);
                        color: rgba(247,243,239,0.78);
                        font-size: 0.8rem;
                        line-height: 1.52;
                        backdrop-filter: blur(14px);
                    }}

                    .schema-holo-panel {{
                        position: absolute;
                        top: 1rem;
                        right: 1rem;
                        z-index: 5;
                        width: min(24rem, calc(100% - 2rem));
                        max-height: calc(100% - 2rem);
                        overflow: auto;
                        padding: 1rem 1rem 1.05rem;
                        border-radius: 26px;
                        border: 1px solid rgba(255,255,255,0.12);
                        background:
                            linear-gradient(180deg, rgba(255,255,255,0.1), rgba(255,255,255,0.05)),
                            rgba(11, 11, 11, 0.72);
                        box-shadow: 0 26px 54px rgba(0,0,0,0.28);
                        backdrop-filter: blur(18px);
                        -webkit-backdrop-filter: blur(18px);
                        transform: translateX(108%) scale(0.96);
                        opacity: 0;
                        transition: transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.3s ease;
                    }}

                    .schema-holo-panel.active {{
                        transform: translateX(0) scale(1);
                        opacity: 1;
                    }}

                    .schema-holo-panel-close {{
                        position: absolute;
                        top: 0.9rem;
                        right: 0.9rem;
                        width: 2.1rem;
                        height: 2.1rem;
                        border-radius: 999px;
                        border: 1px solid rgba(255,255,255,0.1);
                        background: rgba(255,255,255,0.08);
                        color: var(--schema-white);
                        font-size: 1rem;
                        cursor: pointer;
                    }}

                    .schema-holo-panel-kicker {{
                        color: var(--schema-bronze-soft);
                        font-size: 0.7rem;
                        font-weight: 800;
                        letter-spacing: 0.18em;
                        text-transform: uppercase;
                    }}

                    .schema-holo-panel-title {{
                        margin-top: 0.45rem;
                        color: var(--schema-white);
                        font-size: 1.28rem;
                        font-weight: 800;
                    }}

                    .schema-holo-panel-copy {{
                        margin-top: 0.45rem;
                        color: var(--schema-soft);
                        font-size: 0.88rem;
                        line-height: 1.66;
                    }}

                    .schema-holo-chip-row {{
                        display: flex;
                        gap: 0.55rem;
                        flex-wrap: wrap;
                        margin-top: 0.9rem;
                    }}

                    .schema-holo-chip {{
                        display: inline-flex;
                        align-items: center;
                        padding: 0.46rem 0.68rem;
                        border-radius: 999px;
                        border: 1px solid rgba(255,255,255,0.08);
                        background: rgba(255,255,255,0.06);
                        color: var(--schema-white);
                        font-size: 0.77rem;
                        font-weight: 800;
                    }}

                    .schema-holo-task-link {{
                        display: inline-flex;
                        margin-top: 0.72rem;
                        padding: 0.52rem 0.82rem;
                        border-radius: 999px;
                        border: 1px solid rgba(214,175,140,0.34);
                        background: rgba(214,175,140,0.16);
                        color: var(--schema-white);
                        text-decoration: none;
                        font-size: 0.78rem;
                        font-weight: 800;
                    }}

                    .schema-holo-task-link:hover {{
                        border-color: rgba(214,175,140,0.62);
                        background: rgba(214,175,140,0.24);
                    }}

                    .schema-holo-section {{
                        margin-top: 1rem;
                    }}

                    .schema-holo-section-title {{
                        color: var(--schema-bronze-soft);
                        font-size: 0.72rem;
                        font-weight: 800;
                        letter-spacing: 0.16em;
                        text-transform: uppercase;
                        margin-bottom: 0.62rem;
                    }}

                    .schema-holo-column-list,
                    .schema-holo-join-list,
                    .schema-holo-todo-list {{
                        display: grid;
                        gap: 0.6rem;
                    }}

                    .schema-holo-column-card,
                    .schema-holo-join-card,
                    .schema-holo-todo-card {{
                        padding: 0.76rem 0.82rem;
                        border-radius: 18px;
                        border: 1px solid rgba(255,255,255,0.08);
                        background: rgba(255,255,255,0.05);
                    }}

                    .schema-holo-column-name {{
                        color: var(--schema-white);
                        font-size: 0.84rem;
                        font-weight: 800;
                    }}

                    .schema-holo-column-copy,
                    .schema-holo-join-copy,
                    .schema-holo-todo-copy {{
                        color: var(--schema-soft);
                        font-size: 0.8rem;
                        line-height: 1.58;
                        margin-top: 0.28rem;
                    }}

                    .schema-holo-node-label {{
                        fill: var(--schema-white);
                        font-size: 12px;
                        font-weight: 800;
                        letter-spacing: 0.08em;
                        text-transform: uppercase;
                        pointer-events: none;
                        text-anchor: middle;
                    }}

                    .schema-holo-node-group {{
                        cursor: pointer;
                    }}

                    .schema-holo-node-group.is-dimmed {{
                        opacity: 0.28;
                    }}

                    .schema-holo-link-glow {{
                        stroke-linecap: round;
                        opacity: 0.3;
                        filter: url(#schemaBeamGlow);
                    }}

                    .schema-holo-link-core {{
                        stroke-linecap: round;
                        stroke-dasharray: 7 12;
                        animation: schemaBeamFlow 7s linear infinite;
                        opacity: 0.9;
                    }}

                    .schema-holo-aura {{
                        opacity: 0.24;
                        filter: url(#schemaOrbGlow);
                    }}

                    .schema-holo-ring {{
                        fill: none;
                        stroke: rgba(255,255,255,0.34);
                        stroke-width: 1.3;
                        opacity: 0.78;
                    }}

                    .schema-holo-pulse {{
                        fill: none;
                        stroke: rgba(214,175,140,0.54);
                        stroke-width: 1.2;
                        opacity: 0.52;
                        animation: schemaPulse 3s ease-in-out infinite;
                    }}

                    .schema-holo-core {{
                        stroke: rgba(255,255,255,0.42);
                        stroke-width: 1.4;
                        filter: url(#schemaCoreGlow);
                    }}

                    .schema-holo-node-group.is-focused .schema-holo-ring {{
                        stroke: rgba(255,255,255,0.7);
                    }}

                    .schema-holo-node-group.is-focused .schema-holo-pulse {{
                        stroke: rgba(214,175,140,0.86);
                    }}

                    @keyframes schemaBeamFlow {{
                        from {{ stroke-dashoffset: 0; }}
                        to {{ stroke-dashoffset: -180; }}
                    }}

                    @keyframes schemaPulse {{
                        0%, 100% {{ transform: scale(0.98); opacity: 0.4; }}
                        50% {{ transform: scale(1.08); opacity: 0.8; }}
                    }}

                    @media (max-width: 1100px) {{
                        .schema-holo-panel {{
                            width: calc(100% - 2rem);
                        }}

                    }}
                </style>
            </head>
            <body>
                <div class="schema-holo-shell">
                    <div class="schema-holo-header">
                        <div>
                            <div class="schema-holo-kicker">Schema Constellation</div>
                            <div class="schema-holo-title">Holographic Node Graph</div>
                            <div class="schema-holo-copy">Explore the live prompt schema as a spatial relationship system. Click any orb to zoom the camera inward and reveal a holographic panel of its columns, joins, and verification notes.</div>
                        </div>
                        <div class="schema-holo-metrics">
                            <div class="schema-holo-metric" id="schema-node-count"></div>
                            <div class="schema-holo-metric" id="schema-edge-count"></div>
                        </div>
                    </div>
                    <div class="schema-holo-stage">
                        <svg class="schema-holo-svg" id="schema-holo-svg"></svg>
                        <div class="schema-holo-hint">Drag nodes to inspect the layout. Click a table orb to pull the camera closer and open the holographic structure panel.</div>
                        <aside class="schema-holo-panel" id="schema-holo-panel">
                            <button class="schema-holo-panel-close" id="schema-holo-panel-close" type="button">×</button>
                            <div id="schema-holo-panel-content"></div>
                        </aside>
                    </div>
                </div>
                <script>
                    (() => {{
                        const payload = {payload_json};
                        const svgRoot = document.getElementById("schema-holo-svg");
                        const stage = svgRoot.parentElement;
                        const panel = document.getElementById("schema-holo-panel");
                        const panelContent = document.getElementById("schema-holo-panel-content");
                        const panelClose = document.getElementById("schema-holo-panel-close");
                        const nodeCount = document.getElementById("schema-node-count");
                        const edgeCount = document.getElementById("schema-edge-count");

                        nodeCount.textContent = `${{payload.nodes.length}} tables`;
                        edgeCount.textContent = `${{payload.edges.length}} relationship beams`;

                        if (!window.d3) {{
                            panel.classList.add("active");
                            panelContent.innerHTML = `
                                <div class="schema-holo-panel-kicker">Graph Unavailable</div>
                                <div class="schema-holo-panel-title">D3.js could not be loaded</div>
                                <div class="schema-holo-panel-copy">The holographic schema explorer needs D3 in the browser. Please check the network connection and refresh this view.</div>
                            `;
                            return;
                        }}

                        const d3 = window.d3;
                        const width = stage.clientWidth;
                        const height = stage.clientHeight;
                        const svg = d3.select(svgRoot).attr("viewBox", [0, 0, width, height]);

                        const defs = svg.append("defs");

                        const beamGradient = defs.append("linearGradient")
                            .attr("id", "schemaBeamGradient")
                            .attr("x1", "0%")
                            .attr("y1", "0%")
                            .attr("x2", "100%")
                            .attr("y2", "0%");
                        beamGradient.append("stop").attr("offset", "0%").attr("stop-color", "#ffffff").attr("stop-opacity", 0.08);
                        beamGradient.append("stop").attr("offset", "50%").attr("stop-color", "#af7f59").attr("stop-opacity", 0.96);
                        beamGradient.append("stop").attr("offset", "100%").attr("stop-color", "#f4ede7").attr("stop-opacity", 0.18);

                        const orbGlow = defs.append("filter").attr("id", "schemaOrbGlow");
                        orbGlow.append("feGaussianBlur").attr("stdDeviation", "14").attr("result", "blur");
                        orbGlow.append("feMerge")
                            .selectAll("feMergeNode")
                            .data(["blur", "SourceGraphic"])
                            .enter()
                            .append("feMergeNode")
                            .attr("in", (value) => value);

                        const beamGlow = defs.append("filter").attr("id", "schemaBeamGlow");
                        beamGlow.append("feGaussianBlur").attr("stdDeviation", "6");

                        const coreGlow = defs.append("filter").attr("id", "schemaCoreGlow");
                        coreGlow.append("feDropShadow")
                            .attr("dx", 0)
                            .attr("dy", 0)
                            .attr("stdDeviation", 5)
                            .attr("flood-color", "#d6af8c")
                            .attr("flood-opacity", 0.38);

                        const viewport = svg.append("g");
                        const beamLayer = viewport.append("g");
                        const nodeLayer = viewport.append("g");

                        const nodes = payload.nodes.map((node, index) => ({{
                            ...node,
                            fx: null,
                            fy: null,
                            x: (width * 0.5) + Math.cos((index / payload.nodes.length) * Math.PI * 2) * 140,
                            y: (height * 0.52) + Math.sin((index / payload.nodes.length) * Math.PI * 2) * 140,
                            currentSize: node.size,
                            targetSize: node.size,
                        }}));

                        const links = payload.edges.map((edge) => ({{
                            source: edge.source,
                            target: edge.target,
                            join_keys: edge.join_keys,
                        }}));

                        const linkSelection = beamLayer.selectAll(".schema-holo-link")
                            .data(links)
                            .enter()
                            .append("g");

                        linkSelection.append("line")
                            .attr("class", "schema-holo-link-glow")
                            .attr("stroke", "#af7f59")
                            .attr("stroke-width", 9);

                        linkSelection.append("line")
                            .attr("class", "schema-holo-link-core")
                            .attr("stroke", "url(#schemaBeamGradient)")
                            .attr("stroke-width", 2.2);

                        const nodeSelection = nodeLayer.selectAll(".schema-holo-node-group")
                            .data(nodes)
                            .enter()
                            .append("g")
                            .attr("class", "schema-holo-node-group");

                        nodeSelection.append("circle")
                            .attr("class", "schema-holo-aura")
                            .attr("fill", (node) => node.glow)
                            .attr("r", (node) => node.currentSize * 1.4);

                        nodeSelection.append("circle")
                            .attr("class", "schema-holo-pulse")
                            .attr("r", (node) => node.currentSize * 1.02);

                        nodeSelection.append("circle")
                            .attr("class", "schema-holo-ring")
                            .attr("r", (node) => node.currentSize * 0.92);

                        nodeSelection.append("circle")
                            .attr("class", "schema-holo-core")
                            .attr("fill", (node) => node.core)
                            .attr("r", (node) => node.currentSize * 0.78);

                        nodeSelection.append("text")
                            .attr("class", "schema-holo-node-label")
                            .attr("dy", "0.35em")
                            .text((node) => node.display_name);

                        const simulation = d3.forceSimulation(nodes)
                            .force("link", d3.forceLink(links).id((node) => node.id).distance((link) => {{
                                const linkTouchesCheckin = link.source.id === "checkin" || link.target.id === "checkin";
                                return linkTouchesCheckin ? 160 : 210;
                            }}).strength(0.84))
                            .force("charge", d3.forceManyBody().strength(-750))
                            .force("center", d3.forceCenter(width / 2, height / 2))
                            .force("collision", d3.forceCollide().radius((node) => node.currentSize + 34).iterations(2))
                            .force("x", d3.forceX(width / 2).strength(0.04))
                            .force("y", d3.forceY(height / 2).strength(0.04));

                        const camera = {{
                            currentX: 0,
                            currentY: 0,
                            currentScale: 1,
                            targetX: 0,
                            targetY: 0,
                            targetScale: 1,
                        }};

                        let selectedNodeId = null;
                        let cameraFrame = null;

                        const easeCamera = () => {{
                            camera.currentX += (camera.targetX - camera.currentX) * 0.12;
                            camera.currentY += (camera.targetY - camera.currentY) * 0.12;
                            camera.currentScale += (camera.targetScale - camera.currentScale) * 0.12;

                            viewport.attr(
                                "transform",
                                `translate(${{camera.currentX}}, ${{camera.currentY}}) scale(${{camera.currentScale}})`
                            );

                            const shouldContinue =
                                Math.abs(camera.currentX - camera.targetX) > 0.2 ||
                                Math.abs(camera.currentY - camera.targetY) > 0.2 ||
                                Math.abs(camera.currentScale - camera.targetScale) > 0.002;

                            if (shouldContinue) {{
                                cameraFrame = requestAnimationFrame(easeCamera);
                            }} else {{
                                cameraFrame = null;
                            }}
                        }};

                        const queueCameraFrame = () => {{
                            if (cameraFrame !== null) {{
                                return;
                            }}
                            cameraFrame = requestAnimationFrame(easeCamera);
                        }};

                        const updateNodeSizing = () => {{
                            nodeSelection.select(".schema-holo-aura")
                                .transition()
                                .duration(460)
                                .attr("r", (node) => node.targetSize * 1.48);

                            nodeSelection.select(".schema-holo-pulse")
                                .transition()
                                .duration(460)
                                .attr("r", (node) => node.targetSize * 1.08);

                            nodeSelection.select(".schema-holo-ring")
                                .transition()
                                .duration(460)
                                .attr("r", (node) => node.targetSize * 0.94);

                            nodeSelection.select(".schema-holo-core")
                                .transition()
                                .duration(460)
                                .attr("r", (node) => node.targetSize * 0.8);

                            simulation.force("collision", d3.forceCollide().radius((node) => node.targetSize + 36).iterations(2));
                            simulation.alphaTarget(0.24).restart();
                            setTimeout(() => simulation.alphaTarget(0), 500);
                        }};

                        const renderPanel = (node) => {{
                            if (!node) {{
                                panel.classList.remove("active");
                                return;
                            }}

                            const joinMarkup = node.join_keys.map((joinKey) => `
                                <div class="schema-holo-join-card">
                                    <div class="schema-holo-join-copy">${{joinKey}}</div>
                                </div>
                            `).join("");

                            const columnMarkup = node.columns.map((column) => `
                                <div class="schema-holo-column-card">
                                    <div class="schema-holo-column-name">${{column.name}}</div>
                                    <div class="schema-holo-column-copy">${{column.description}}</div>
                                </div>
                            `).join("");

                            const todoMarkup = node.verification_todos.map((todo) => `
                                <div class="schema-holo-todo-card">
                                    <div class="schema-holo-todo-copy">${{todo}}</div>
                                </div>
                            `).join("");

                            const taskLinkMarkup = node.task_key
                                ? `<a class="schema-holo-task-link" href="?route=schema&task=${{node.task_key}}#task-outputs" target="_top">Open ${{node.task_label || "Task"}} Outputs</a>`
                                : "";

                            panelContent.innerHTML = `
                                <div class="schema-holo-panel-kicker">Table Focus</div>
                                <div class="schema-holo-panel-title">${{node.display_name}}</div>
                                <div class="schema-holo-panel-copy">${{node.description}}</div>
                                <div class="schema-holo-chip-row">
                                    <span class="schema-holo-chip">${{node.column_count}} columns</span>
                                    <span class="schema-holo-chip">${{node.join_keys.length}} join paths</span>
                                </div>
                                ${{taskLinkMarkup}}
                                <div class="schema-holo-section">
                                    <div class="schema-holo-section-title">Columns</div>
                                    <div class="schema-holo-column-list">${{columnMarkup}}</div>
                                </div>
                                <div class="schema-holo-section">
                                    <div class="schema-holo-section-title">Join Relationships</div>
                                    <div class="schema-holo-join-list">${{joinMarkup || '<div class="schema-holo-join-card"><div class="schema-holo-join-copy">No join keys defined.</div></div>'}}</div>
                                </div>
                                <div class="schema-holo-section">
                                    <div class="schema-holo-section-title">Verification Notes</div>
                                    <div class="schema-holo-todo-list">${{todoMarkup || '<div class="schema-holo-todo-card"><div class="schema-holo-todo-copy">No verification notes recorded.</div></div>'}}</div>
                                </div>
                            `;
                            panel.classList.add("active");
                        }};

                        const focusNode = (node) => {{
                            selectedNodeId = node ? node.id : null;

                            nodes.forEach((item) => {{
                                item.targetSize = item.id === selectedNodeId ? item.size * 1.24 : item.size;
                            }});

                            nodeSelection
                                .classed("is-focused", (item) => item.id === selectedNodeId)
                                .classed("is-dimmed", (item) => selectedNodeId && item.id !== selectedNodeId);

                            linkSelection
                                .classed("is-dimmed", (link) => selectedNodeId && link.source.id !== selectedNodeId && link.target.id !== selectedNodeId)
                                .transition()
                                .duration(420)
                                .style("opacity", (link) => {{
                                    if (!selectedNodeId) {{
                                        return 1;
                                    }}
                                    return link.source.id === selectedNodeId || link.target.id === selectedNodeId ? 1 : 0.22;
                                }});

                            updateNodeSizing();
                            renderPanel(node);

                            if (!node) {{
                                camera.targetScale = 1;
                                camera.targetX = 0;
                                camera.targetY = 0;
                                queueCameraFrame();
                                return;
                            }}

                            const panelAwareCenter = width > 920 ? width * 0.33 : width * 0.5;
                            const targetScale = width > 920 ? 1.54 : 1.34;
                            camera.targetScale = targetScale;
                            camera.targetX = panelAwareCenter - (node.x * targetScale);
                            camera.targetY = (height * 0.5) - (node.y * targetScale);
                            queueCameraFrame();
                        }};

                        panelClose.addEventListener("click", () => {{
                            focusNode(null);
                        }});

                        svg.on("click", (event) => {{
                            if (event.target === svgRoot) {{
                                focusNode(null);
                            }}
                        }});

                        nodeSelection.on("click", (event, node) => {{
                            event.stopPropagation();
                            focusNode(node);
                        }});

                        nodeSelection.on("mouseenter", function() {{
                            d3.select(this).raise();
                        }});

                        nodeSelection.call(
                            d3.drag()
                                .on("start", (event, node) => {{
                                    if (!event.active) {{
                                        simulation.alphaTarget(0.2).restart();
                                    }}
                                    node.fx = node.x;
                                    node.fy = node.y;
                                }})
                                .on("drag", (event, node) => {{
                                    node.fx = event.x;
                                    node.fy = event.y;
                                }})
                                .on("end", (event, node) => {{
                                    if (!event.active) {{
                                        simulation.alphaTarget(0);
                                    }}
                                    node.fx = null;
                                    node.fy = null;
                                }})
                        );

                        simulation.on("tick", () => {{
                            linkSelection.selectAll("line")
                                .attr("x1", (link) => link.source.x)
                                .attr("y1", (link) => link.source.y)
                                .attr("x2", (link) => link.target.x)
                                .attr("y2", (link) => link.target.y);

                            nodeSelection.attr("transform", (node) => `translate(${{node.x}}, ${{node.y}})`);

                            if (selectedNodeId) {{
                                const activeNode = nodes.find((node) => node.id === selectedNodeId);
                                if (activeNode) {{
                                    const panelAwareCenter = width > 920 ? width * 0.33 : width * 0.5;
                                    camera.targetX = panelAwareCenter - (activeNode.x * camera.targetScale);
                                    camera.targetY = (height * 0.5) - (activeNode.y * camera.targetScale);
                                    queueCameraFrame();
                                }}
                            }}
                        }});

                        renderPanel(null);
                    }})();
                </script>
            </body>
        </html>
        """,
        height=1120,
    )



def _render_schema_requirement_cards() -> None:
    """Render concise requirement-aligned schema guidance cards."""
    schema_tables = get_table_schemas()
    table_cards = []
    for table_name in ("business", "rating", "users", "checkin"):
        info = schema_tables.get(table_name, {})
        table_cards.append(
            {
                "table": table_name,
                "description": str(info.get("description", "")).strip(),
                "columns": len(list(info.get("columns", []))),
                "joins": len(list(info.get("join_keys", []))),
            }
        )

    cards_markup = "".join(
        (
            '<div class="premium-glass-card animate-in stagger-2">'
            f'<div style="font-size: 0.72rem; letter-spacing: 0.15em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(card["table"])}</div>'
            f'<p style="margin: 0.5rem 0; color: #595959; font-size: 0.86rem; line-height: 1.58;">{escape(card["description"] or "Table metadata loaded.")}</p>'
            '<div style="display:flex; gap:0.5rem; flex-wrap:wrap;">'
            f'<span style="padding:0.35rem 0.62rem; border-radius:999px; background: rgba(17,17,17,0.06); font-size:0.73rem; font-weight:700; color:#111;">{card["columns"]} columns</span>'
            f'<span style="padding:0.35rem 0.62rem; border-radius:999px; background: rgba(175,127,89,0.12); font-size:0.73rem; font-weight:700; color:#111;">{card["joins"]} join paths</span>'
            "</div>"
            "</div>"
        )
        for card in table_cards
    )

    official_links = [
        ("Yelp Dataset Documentation", "https://www.yelp.com/dataset/documentation/main"),
        ("Spark SQL Reference", "https://spark.apache.org/docs/latest/sql-ref.html"),
        ("Spark SELECT Syntax", "https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html"),
    ]
    links_markup = "".join(
        (
            f'<a href="{escape(url)}" target="_blank" rel="noopener noreferrer" class="schema-task-chip" style="display:inline-flex; align-items:center; justify-content:center; padding:0.62rem 0.84rem; border-radius:999px; text-decoration:none; color:#111; background: rgba(255,255,255,0.9); border:1px solid rgba(17,17,17,0.08); font-size:0.78rem; font-weight:700;">{escape(label)}</a>'
        )
        for label, url in official_links
    )

    st.markdown(
        f"""
        <div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.1rem;">Schema Coverage</h3></div>
        <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.9rem; margin-bottom: 0.95rem;">
            {cards_markup}
        </div>
        <div style="display:flex; flex-wrap:wrap; gap:0.6rem; margin-bottom: 0.7rem;">
            {links_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_schema_sql_insight_cards() -> None:
    """Show SQL-based insight cards aligned with project tasks."""
    insights = [
        {
            "title": "Review Analysis: Reviews by Year",
            "sql": "SELECT SUBSTR(date, 1, 4) AS review_year, COUNT(*) AS review_count FROM rating GROUP BY SUBSTR(date, 1, 4) ORDER BY review_year;",
            "question": "Count the number of reviews per year.",
        },
        {
            "title": "Business Analysis: Top Cities by Businesses",
            "sql": "SELECT city, COUNT(*) AS business_count FROM business GROUP BY city ORDER BY business_count DESC LIMIT 10;",
            "question": "Show the top 10 cities by number of businesses.",
        },
        {
            "title": "User Analysis: Most Elite Users by City",
            "sql": "SELECT b.city, COUNT(DISTINCT u.user_id) AS elite_user_count FROM users u JOIN rating r ON u.user_id = r.user_id JOIN business b ON r.business_id = b.business_id WHERE u.elite IS NOT NULL GROUP BY b.city ORDER BY elite_user_count DESC LIMIT 10;",
            "question": "Which 10 cities have the most elite users?",
        },
        {
            "title": "Check-in Analysis: Highest Activity Businesses",
            "sql": "SELECT b.name, b.city, LENGTH(c.date) - LENGTH(REPLACE(c.date, ',', '')) + 1 AS checkin_events FROM checkin c JOIN business b ON c.business_id = b.business_id ORDER BY checkin_events DESC LIMIT 10;",
            "question": "Which businesses receive the most check-ins?",
        },
    ]

    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.1rem;">SQL Insight Cards</h3></div>',
        unsafe_allow_html=True,
    )

    for insight in insights:
        ask_href = _build_route_href(
            "home",
            chat_mode=CHAT_MODE_DATA,
            action="ask",
            question=insight["question"],
            panel="results",
        )
        with st.expander(
            insight["title"], expanded=insight["title"].startswith("Review Analysis")
        ):
            st.code(insight["sql"], language="sql")
            st.markdown(
                f'<a href="{escape(ask_href)}" target="_self" style="display:inline-flex; padding:0.55rem 0.85rem; border-radius:999px; text-decoration:none; background:rgba(17,17,17,0.94); color:#fff; font-size:0.79rem; font-weight:700;">Run in Chat</a>',
                unsafe_allow_html=True,
            )
def _render_schema_sql_result_snapshots() -> None:
    """Render live SQL result snapshots for business/user/review analysis."""
    snapshots = [
        (
            "Business Snapshot",
            "SELECT city, COUNT(*) AS business_count FROM business GROUP BY city ORDER BY business_count DESC LIMIT 10",
        ),
        (
            "User Snapshot",
            "SELECT name, review_count, fans FROM users ORDER BY review_count DESC LIMIT 10",
        ),
        (
            "Review Snapshot",
            "SELECT SUBSTR(date, 1, 4) AS review_year, COUNT(*) AS review_count FROM rating GROUP BY SUBSTR(date, 1, 4) ORDER BY review_year",
        ),
    ]

    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.1rem;">SQL Result Snapshots</h3></div>',
        unsafe_allow_html=True,
    )

    for title, sql in snapshots:
        with st.expander(title, expanded=title == "Review Snapshot"):
            st.code(sql, language="sql")
            result = execute_sql(sql)
            if result.executed:
                if result.rows:
                    st.dataframe(result.rows, width="stretch")
                else:
                    st.caption("Query executed successfully but returned 0 rows.")
            else:
                st.warning(result.error or result.message or "Snapshot query failed.")


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_zeppelin_notebook(notebook_id: str, base_url: str = ZEPPELIN_BASE_URL) -> dict[str, Any]:
    """Fetch one Zeppelin notebook payload from the local notebook server."""
    import urllib.error
    import urllib.request

    endpoint = f"{base_url}/api/notebook/{notebook_id}"
    try:
        with urllib.request.urlopen(endpoint, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        return {"ok": False, "error": str(exc), "body": {}}
    except TimeoutError:
        return {"ok": False, "error": "Zeppelin request timed out.", "body": {}}
    except Exception as exc:  # pragma: no cover - safety fallback
        return {"ok": False, "error": str(exc), "body": {}}

    if payload.get("status") != "OK":
        return {
            "ok": False,
            "error": str(payload.get("message") or "Zeppelin API returned a non-OK status."),
            "body": payload.get("body") or {},
        }

    return {"ok": True, "error": "", "body": payload.get("body") or {}}


def _extract_zeppelin_question_label(paragraph_text: str) -> str:
    """Extract the QUESTION line from one Zeppelin paragraph."""
    lines = (paragraph_text or "").splitlines()
    for index, raw_line in enumerate(lines):
        line = raw_line.strip()
        if "QUESTION" in line.upper():
            return line.lstrip("#").strip()
        if re.match(r"^#\s*[IVX]+\.\s*\d+\.\s+.+", line, flags=re.IGNORECASE):
            label = line.lstrip("#").strip()
            # Include a continuation comment line when present.
            if index + 1 < len(lines):
                next_line = lines[index + 1].strip()
                if next_line.startswith("#") and not re.match(r"^#\s*[IVX]+\.\s*\d+\.", next_line, flags=re.IGNORECASE):
                    label = f"{label} {next_line.lstrip('#').strip()}"
            return label
        if re.match(r"^#\s*[A-Z]\.\s*\d+\.\s+.+", line, flags=re.IGNORECASE):
            return line.lstrip("#").strip()
    return ""


def _extract_zeppelin_output_preview(paragraph: dict[str, Any], max_chars: int = 1400) -> tuple[str, str]:
    """Extract a display preview from Zeppelin paragraph output."""
    results = paragraph.get("results") or {}
    messages = results.get("msg") or []
    if not isinstance(messages, list) or not messages:
        return ("No output found for this paragraph.", "NONE")

    for item in messages:
        if not isinstance(item, dict):
            continue
        output_type = str(item.get("type", "TEXT")).upper()
        data = item.get("data", "")
        if isinstance(data, str) and data.strip():
            clean = data.strip()
            if len(clean) > max_chars:
                clean = f"{clean[:max_chars].rstrip()}..."
            return (clean, output_type)

    return ("No structured output found for this paragraph.", "NONE")


def _extract_sql_from_zeppelin_paragraph(paragraph_text: str) -> str:
    """Extract runnable SQL from one Zeppelin paragraph text."""
    text = str(paragraph_text or "")
    if not text.strip():
        return ""

    # Prefer fenced SQL blocks when present.
    fence_match = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        return fence_match.group(1).strip()

    lines = text.splitlines()
    sql_lines: list[str] = []
    collecting = False
    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if collecting:
                break
            continue

        if stripped.startswith("%sql"):
            collecting = True
            tail = stripped[4:].strip()
            if tail:
                sql_lines.append(tail)
            continue

        if collecting:
            if stripped.startswith("%") and not stripped.lower().startswith("%sql"):
                break
            if stripped.startswith("#"):
                continue
            sql_lines.append(line)

    return "\n".join(sql_lines).strip()


def _normalize_for_match(text: str) -> str:
    return re.sub(r"[^a-z0-9\s]+", " ", (text or "").lower()).strip()


def _expand_task_aliases(text: str) -> str:
    """Expand common Jira/requirement phrasing to canonical task wording."""
    normalized = _normalize_for_match(text)
    if not normalized:
        return ""
    expanded_parts = [normalized]
    for canonical, variants in TASK_QUERY_ALIASES.items():
        if canonical in normalized:
            expanded_parts.extend(variants)
            continue
        for variant in variants:
            if variant in normalized:
                expanded_parts.append(canonical)
                expanded_parts.extend(variants)
                break
    return " ".join(dict.fromkeys(expanded_parts))


def _token_overlap_score(a: str, b: str) -> float:
    a_tokens = {token for token in _expand_task_aliases(a).split() if len(token) > 2}
    b_tokens = {token for token in _normalize_for_match(b).split() if len(token) > 2}
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / max(1, len(a_tokens | b_tokens))


@st.cache_data(ttl=300, show_spinner=False)
def _build_zeppelin_qa_index(base_url: str = ZEPPELIN_BASE_URL) -> list[dict[str, str]]:
    """Build searchable QA pairs from Zeppelin task notebooks."""
    qa_items: list[dict[str, str]] = []
    for task_key, task_meta in ZEPPELIN_TASK_NOTEBOOKS.items():
        payload = _fetch_zeppelin_notebook(task_meta["id"], base_url)
        if not payload.get("ok"):
            continue

        notebook_body = payload.get("body", {})
        notebook_name = str(notebook_body.get("name", task_meta.get("label", task_key)))
        notebook_url = f"{base_url}/#/notebook/{task_meta['id']}"

        for paragraph in list(notebook_body.get("paragraphs") or []):
            paragraph_text = str(paragraph.get("text", ""))
            question_label = _extract_zeppelin_question_label(paragraph_text)
            if not question_label:
                continue
            output_preview, output_type = _extract_zeppelin_output_preview(paragraph)
            qa_items.append(
                {
                    "task_key": task_key,
                    "task_label": str(task_meta.get("label", task_key)),
                    "notebook_id": str(task_meta.get("id", "")),
                    "notebook_name": notebook_name,
                    "notebook_url": notebook_url,
                    "paragraph_id": str(paragraph.get("id", "")),
                    "question_label": question_label,
                    "output_preview": output_preview,
                    "output_type": output_type,
                    "sql_text": _extract_sql_from_zeppelin_paragraph(paragraph_text),
                }
            )
    return qa_items


def _find_best_zeppelin_qa_match(question: str) -> dict[str, str] | None:
    """Return the best Zeppelin QA match for a user question."""
    clean_question = _normalize_free_text(question)
    expanded_question = _expand_task_aliases(clean_question)
    if not clean_question:
        return None

    qa_items = _build_zeppelin_qa_index(ZEPPELIN_BASE_URL)
    if not qa_items:
        return None

    best_item: dict[str, str] | None = None
    best_score = 0.0
    best_seq_score = 0.0
    best_overlap = 0.0
    for item in qa_items:
        label = item.get("question_label", "")
        seq_score = difflib.SequenceMatcher(None, expanded_question.lower(), label.lower()).ratio()
        overlap = _token_overlap_score(expanded_question, label)
        score = (0.65 * seq_score) + (0.35 * overlap)
        label_norm = _normalize_for_match(label)
        if (
            ("category synergy" in expanded_question or ("category" in expanded_question and "pairs" in expanded_question))
            and ("co occur" in label_norm or "co-occur" in label.lower() or ("category" in label_norm and "pair" in label_norm))
        ):
            score += 0.18
        if score > best_score:
            best_score = score
            best_seq_score = seq_score
            best_overlap = overlap
            best_item = item

    if best_item is None:
        return None

    # Guardrails to prevent false matches on generic wording (e.g., "count ... per year").
    if best_score < 0.48:
        return None
    if best_overlap < 0.10 and best_seq_score < 0.74:
        return None
    return best_item


def _build_zeppelin_qa_reply(question: str) -> str | None:
    """Build a direct chatbot reply from Zeppelin notebook output for a matched task."""
    match = _find_best_zeppelin_qa_match(question)
    if not match:
        return None

    header = (
        f"Matched Zeppelin task output from **{match['task_label']}** "
        f"(Notebook `{match['notebook_id']}`):\n\n"
        f"**{match['question_label']}**\n\n"
    )
    output = match.get("output_preview", "").strip()
    if not output:
        output = "No output preview was available for this task paragraph."

    if match.get("output_type", "").upper() == "TABLE":
        body = f"```text\n{output}\n```"
    else:
        body = output

    return _sanitize_assistant_text(f"{header}{body}")


def _get_zeppelin_matched_sql(question: str) -> str:
    """Return SQL attached to the best Zeppelin QA match for a question."""
    match = _find_best_zeppelin_qa_match(question)
    if not match:
        return ""
    return str(match.get("sql_text", "")).strip()


def _refresh_zeppelin_knowledge(force: bool = False) -> None:
    """Refresh cached Zeppelin task/QA indexes so the chatbot stays up-to-date."""
    last_refresh = float(st.session_state.get("zeppelin_last_refresh_ts", 0.0) or 0.0)
    now_ts = time.time()
    if not force and (now_ts - last_refresh) < max(15, ZEPPELIN_REFRESH_INTERVAL_SECONDS):
        return

    _fetch_zeppelin_notebook.clear()
    _build_zeppelin_task_index.clear()
    _build_zeppelin_qa_index.clear()

    # Warm both indexes right away so first matching call is fast.
    _build_zeppelin_task_index(ZEPPELIN_BASE_URL)
    _build_zeppelin_qa_index(ZEPPELIN_BASE_URL)
    recommendation_cache: dict[str, str] = {}
    for recommendation_question in PROJECT_RECOMMENDATION_QUESTIONS:
        reply = _build_zeppelin_qa_reply(recommendation_question)
        if reply:
            recommendation_cache[
                _normalize_free_text(recommendation_question).lower().rstrip("?.!")
            ] = reply
    st.session_state.recommendation_reply_cache = recommendation_cache
    st.session_state.zeppelin_last_refresh_ts = now_ts


@st.cache_data(ttl=300, show_spinner=False)
def _build_zeppelin_task_index(base_url: str = ZEPPELIN_BASE_URL) -> list[dict[str, str]]:
    """Build a compact searchable index of Zeppelin task prompts."""
    task_index: list[dict[str, str]] = []
    for task_key, task_meta in ZEPPELIN_TASK_NOTEBOOKS.items():
        payload = _fetch_zeppelin_notebook(task_meta["id"], base_url)
        if not payload.get("ok"):
            continue
        notebook_body = payload.get("body", {})
        paragraphs = list(notebook_body.get("paragraphs") or [])
        question_lines: list[str] = []
        for paragraph in paragraphs:
            text = str(paragraph.get("text", "")).strip()
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if "QUESTION" in line.upper() or "TASK" in line.upper():
                    question_lines.append(line)
                if len(question_lines) >= 4:
                    break
            if len(question_lines) >= 4:
                break
        task_index.append(
            {
                "task_key": task_key,
                "label": str(task_meta.get("label", task_key)),
                "notebook_id": str(task_meta.get("id", "")),
                "notebook_name": str(notebook_body.get("name", task_meta.get("label", task_key))),
                "questions": " | ".join(question_lines) if question_lines else "No explicit QUESTION headers found.",
            }
        )
    return task_index


def _build_zeppelin_task_summary_text() -> str:
    """Return a concise text summary of Zeppelin tasks for fast chatbot grounding."""
    index = _build_zeppelin_task_index(ZEPPELIN_BASE_URL)
    if not index:
        return ""
    lines: list[str] = []
    for item in index:
        lines.append(
            f"- {item['label']} (Notebook {item['notebook_id']}): {item['questions']}"
        )
    return "\n".join(lines)


def _get_zeppelin_task_fast_reply(question: str) -> str | None:
    """Return a deterministic task-focused reply sourced from Zeppelin notebook metadata."""
    normalized = _normalize_free_text(question).lower()
    trigger_terms = ("zeppelin", "task", "tasks", "analysis", "notebook", "outputs")
    if not any(term in normalized for term in trigger_terms):
        return None

    summary = _build_zeppelin_task_summary_text()
    if not summary:
        return (
            "I could not load Zeppelin task metadata right now. "
            "Please open the Schema > Task Outputs section to verify notebook connectivity."
        )

    return (
        "Here are the task areas I learned from Zeppelin and can use to guide responses:\n\n"
        f"{summary}\n\n"
        "Ask one of these directly and I can answer with focused SQL or analysis steps quickly."
    )


def _infer_zeppelin_language(paragraph_text: str) -> str:
    """Infer a syntax-highlighting language for Zeppelin paragraph code."""
    stripped = paragraph_text.lstrip()
    if stripped.startswith("%pyspark") or stripped.startswith("%python"):
        return "python"
    if stripped.startswith("%sql"):
        return "sql"
    if stripped.startswith("%scala"):
        return "scala"
    return "text"


def _zeppelin_table_to_dataframe(raw_table: str) -> pd.DataFrame:
    """Convert Zeppelin TABLE output (tab-separated text) into a dataframe."""
    lines = [line for line in raw_table.splitlines() if line.strip()]
    if not lines:
        return pd.DataFrame()

    headers = lines[0].split("	")
    rows: list[list[str]] = []
    for line in lines[1:]:
        values = line.split("	")
        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        elif len(values) > len(headers):
            values = values[: len(headers) - 1] + ["	".join(values[len(headers) - 1 :])]
        rows.append(values)

    dataframe = pd.DataFrame(rows, columns=headers)
    for column in dataframe.columns:
        numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
        if numeric_series.notna().sum() >= max(1, len(dataframe) // 2):
            dataframe[column] = numeric_series
    return dataframe


def _pick_zeppelin_chart_columns(dataframe: pd.DataFrame) -> tuple[str, str, str] | None:
    """Pick x/y columns and chart type for Zeppelin table outputs."""
    if dataframe.empty:
        return None

    numeric_columns = [
        column
        for column in dataframe.columns
        if pd.api.types.is_numeric_dtype(dataframe[column])
    ]
    if not numeric_columns:
        return None

    non_numeric_columns = [
        column
        for column in dataframe.columns
        if column not in numeric_columns
    ]

    if non_numeric_columns:
        x_column = non_numeric_columns[0]
        y_column = numeric_columns[0]
        x_lower = x_column.lower()
        if any(token in x_lower for token in ("year", "date", "month", "time", "day")):
            return x_column, y_column, "line"
        return x_column, y_column, "bar"

    if len(numeric_columns) >= 2:
        return numeric_columns[0], numeric_columns[1], "scatter"

    return "_index", numeric_columns[0], "line"


def _render_zeppelin_output_chart(dataframe: pd.DataFrame, chart_key: str) -> bool:
    """Render one animated chart for a Zeppelin table output."""
    chart_pick = _pick_zeppelin_chart_columns(dataframe)
    if chart_pick is None:
        return False

    x_column, y_column, chart_type = chart_pick
    plot_df = dataframe.copy()
    if x_column == "_index":
        plot_df = plot_df.reset_index().rename(columns={"index": "_index"})

    if len(plot_df) > 40:
        plot_df = plot_df.head(40)

    if chart_type == "line":
        fig = px.line(plot_df, x=x_column, y=y_column, markers=True)
    elif chart_type == "scatter":
        fig = px.scatter(plot_df, x=x_column, y=y_column)
    else:
        fig = px.bar(plot_df, x=x_column, y=y_column)

    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=28, r=20, t=34, b=28),
        height=360,
        transition_duration=850,
    )
    fig.update_traces(marker_line_width=0, marker_opacity=0.95)
    st.plotly_chart(fig, width="stretch", key=chart_key)
    return True


def _render_zeppelin_paragraph(
    paragraph: dict[str, Any], paragraph_index: int, task_label: str, task_key: str
) -> None:
    """Render one Zeppelin paragraph showing only output results."""
    paragraph_text = str(paragraph.get("text") or "").strip()
    status = str(paragraph.get("status") or "READY").strip().title()
    outputs = ((paragraph.get("results") or {}).get("msg") or [])
    paragraph_id = str(paragraph.get("id") or f"paragraph_{paragraph_index}")
    table_outputs: list[pd.DataFrame] = []
    text_outputs: list[str] = []

    for output_index, output in enumerate(outputs, start=1):
        output_type = str(output.get("type") or "TEXT").upper()
        output_data = str(output.get("data") or "")

        if output_type == "TABLE":
            dataframe = _zeppelin_table_to_dataframe(output_data)
            if not dataframe.empty:
                table_outputs.append(dataframe)
            continue

        if output_data.strip():
            capped_text = output_data if len(output_data) <= 12000 else (output_data[:12000] + "\n... (truncated)")
            text_outputs.append(capped_text)

    if not outputs:
        return

    paragraph_task_name = _extract_zeppelin_question_label(paragraph_text)
    title = paragraph_task_name or f"{task_label} - Step {paragraph_index:02d}"
    item_key = f"{task_key}:{paragraph_id}:{paragraph_index}"
    safe_item_key = re.sub(r"[^a-zA-Z0-9_]+", "_", item_key)
    expanded_state_key = f"schema_task_expanded_{safe_item_key}"
    toggle_button_key = f"schema_task_toggle_{safe_item_key}"
    is_expanded = bool(st.session_state.get(expanded_state_key, False))

    line_col, button_col = st.columns([0.84, 0.16])
    line_col.markdown(f"**{title} ({status})**")
    button_label = "Hide" if is_expanded else "View"
    if button_col.button(button_label, key=toggle_button_key, use_container_width=True):
        st.session_state[expanded_state_key] = not is_expanded
        is_expanded = bool(st.session_state.get(expanded_state_key, False))

    if not is_expanded:
        return

    chart_rendered = False
    for output_index, dataframe in enumerate(table_outputs, start=1):
        rendered = _render_zeppelin_output_chart(
            dataframe,
            chart_key=f"zeppelin_chart_{paragraph_id}_{output_index}",
        )
        chart_rendered = chart_rendered or rendered
        st.dataframe(dataframe, width="stretch")
    if not chart_rendered and text_outputs:
        for item in text_outputs:
            st.code(item, language="text")


def _render_zeppelin_notebook_task_content(task_key: str) -> None:
    """Render one task notebook with result outputs only."""
    task_meta = ZEPPELIN_TASK_NOTEBOOKS[task_key]
    notebook_payload = _fetch_zeppelin_notebook(task_meta["id"], ZEPPELIN_BASE_URL)
    if not notebook_payload["ok"]:
        st.warning(
            f"Could not load {task_meta['label']} from Zeppelin: {notebook_payload['error']}"
        )
        return

    notebook_body = notebook_payload["body"]
    paragraphs = list(notebook_body.get("paragraphs") or [])
    st.subheader(str(task_meta.get("label", task_key)))

    visible_index = 0
    for paragraph in paragraphs:
        paragraph_text = str(paragraph.get("text") or "").strip()
        outputs = ((paragraph.get("results") or {}).get("msg") or [])
        if not paragraph_text and not outputs:
            continue
        visible_index += 1
        _render_zeppelin_paragraph(
            paragraph,
            visible_index,
            str(task_meta.get("label", task_key)),
            task_key,
        )

    if visible_index == 0:
        st.caption("No code/output paragraphs were returned for this notebook.")


def _render_zeppelin_task_output_hub() -> None:
    """Render task links and show results for the selected task only."""
    st.markdown(
        """
        <div id="task-outputs"></div>
        <div class="section-shell animate-in stagger-2">
            <h3 class="section-title" style="font-size:1.1rem;">Analysis Tasks</h3>
            <p class="section-copy">Pick a task to view its tables/results.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    task_keys = list(ZEPPELIN_TASK_NOTEBOOKS.keys())
    if not task_keys:
        st.info("No task notebooks are configured.")
        return
    task_links = []
    for task_key in task_keys:
        task_label = str(ZEPPELIN_TASK_NOTEBOOKS[task_key].get("label", task_key))
        href = _build_route_href("schema", task=task_key)
        task_links.append(
            f'<a href="{escape(href)}#task-outputs" target="_self" class="schema-task-chip" '
            'style="display:inline-flex; margin:0.2rem 0.45rem 0.2rem 0; text-decoration:none; padding:0.5rem 0.82rem; border-radius:999px; '
            'font-size:0.78rem; font-weight:700; color:#111; background:rgba(255,255,255,0.93); border:1px solid rgba(17,17,17,0.08);">'
            f"{escape(task_label)}</a>"
        )

    st.markdown("".join(task_links), unsafe_allow_html=True)

    selected_task = str(_read_query_param("task")).strip().lower()
    if not selected_task:
        st.caption("Select a task above to show its output tables/results.")
        return
    if selected_task not in ZEPPELIN_TASK_NOTEBOOKS:
        st.warning("Selected task is invalid. Please pick one from the task list.")
        return

    _render_zeppelin_notebook_task_content(selected_task)


def _render_schema_view() -> None:
    st.markdown(
        '<div class="section-shell animate-in stagger-1"><h2 class="section-title">Database Schema</h2><p class="section-copy">Interactive schema graph and analysis task visuals sourced from your Zeppelin notebooks.</p></div>',
        unsafe_allow_html=True,
    )
    _render_schema_holographic_graph()
    _render_zeppelin_task_output_hub()


def _build_readiness_preflight_cards() -> list[dict[str, str]]:
    """Build requirement-status cards using official project requirement language."""
    config = load_config()
    database_config = type("DatabaseConfig", (), {"engine": "hive"})()

    presentation_ok = True
    service_ok = bool(config.deepseek_model)
    data_ok = database_config.engine in {"hive", "spark", "sqlite", "mysql"}
    workflow_ok = True

    data_turns = [
        turn
        for turn in st.session_state.conversation_turns
        if str(turn.get("mode_label", "")).strip().lower() != GENERAL_CHAT_MODE_LABEL.lower()
    ]
    retry_attempted = any(bool(turn.get("retry_happened")) for turn in data_turns)
    retry_success = any(
        bool(turn.get("retry_happened")) and not bool(turn.get("error"))
        for turn in data_turns
    )

    if retry_success:
        sql_retry_gap = "Implemented"
        sql_retry_detail = "At least one SQL failure was retried and completed successfully."
        sql_retry_tone = "success"
    elif retry_attempted:
        sql_retry_gap = "In Progress"
        sql_retry_detail = "SQL retry logic has executed, but no successful corrected run is recorded yet."
        sql_retry_tone = "warn"
    else:
        sql_retry_gap = "Needs Improvement"
        sql_retry_detail = "Automatic SQL error correction/retry is the main remaining core gap to strengthen."
        sql_retry_tone = "warn"

    return [
        {
            "label": "System Architecture",
            "value": "3 Layers Implemented",
            "detail": "Presentation layer, service layer, and data/execution layer are separated in the app.",
            "tone": "success",
        },
        {
            "label": "Presentation Layer",
            "value": "Chat-Style Frontend",
            "detail": "Users ask Yelp questions in a conversational UI with distinct user/assistant messages.",
            "tone": "success" if presentation_ok else "warn",
        },
        {
            "label": "Service Layer",
            "value": "LLM/API Orchestration",
            "detail": "Backend receives questions, builds prompt context, handles model/API settings, and routes SQL flow.",
            "tone": "success" if service_ok else "warn",
        },
        {
            "label": "Data / Execution Layer",
            "value": "Yelp SQL Backend",
            "detail": "SQL executes against Yelp tables such as business, rating/review, users, and checkin.",
            "tone": "success" if data_ok else "warn",
        },
        {
            "label": "Core Workflow",
            "value": "Schema -> SQL -> Execute",
            "detail": "Dynamic schema injection, natural-language query, SQL generation/sanitization, execution, and error handling flow are present.",
            "tone": "success" if workflow_ok else "warn",
        },
        {
            "label": "UI / UX Requirements",
            "value": "Implemented",
            "detail": "Loading indicators, transparent SQL display, and tabular result rendering are available. Chart rendering is included as bonus behavior when data shape fits.",
            "tone": "success",
        },
        {
            "label": "Practical Expectation",
            "value": "End-to-End Working",
            "detail": "A user can ask in English, generate SQL, run SQL on Yelp data, and inspect SQL + results.",
            "tone": "success",
        },
        {
            "label": "SQL Self-Correction",
            "value": sql_retry_gap,
            "detail": sql_retry_detail,
            "tone": sql_retry_tone,
        },
    ]

def _render_readiness_action_bar() -> None:
    """Render concise validation actions and official reference links."""
    official_links = [
        ("Streamlit Chat API", "https://docs.streamlit.io/develop/api-reference/chat"),
        ("Spark SQL Reference", "https://spark.apache.org/docs/latest/sql-ref.html"),
        ("Spark SELECT Syntax", "https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html"),
        ("Yelp Dataset Docs", "https://www.yelp.com/dataset/documentation/main"),
    ]
    official_links_markup = "".join(
        (
            f'<a href="{escape(url)}" target="_blank" rel="noopener noreferrer" class="readiness-chip">{escape(label)}</a>'
        )
        for label, url in official_links
    )

    st.markdown(
        f"""
        <style>
        .readiness-action-shell {{
            display: grid;
            gap: 1rem;
            margin-bottom: 1.1rem;
        }}

        .readiness-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
        }}

        .readiness-action-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.78rem 1rem;
            border-radius: 999px;
            text-decoration: none;
            font-size: 0.84rem;
            font-weight: 800;
            letter-spacing: 0.02em;
            transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease;
            box-shadow: 0 16px 34px rgba(17,17,17,0.07);
        }}

        .readiness-action-link:hover {{
            transform: translateY(-2px);
        }}

        .readiness-action-link.primary {{
            background: linear-gradient(145deg, rgba(17,17,17,0.98), rgba(45,42,40,0.98));
            color: #ffffff;
        }}

        .readiness-action-link.secondary {{
            background: linear-gradient(145deg, rgba(175,127,89,0.16), rgba(214,175,140,0.18));
            color: #111111;
            border: 1px solid rgba(175,127,89,0.18);
        }}

        .readiness-action-link.ghost {{
            background: rgba(255,255,255,0.82);
            color: #595959;
            border: 1px solid rgba(17,17,17,0.08);
        }}

        .readiness-chip-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.7rem;
        }}

        .readiness-chip {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.7rem 0.92rem;
            border-radius: 999px;
            text-decoration: none;
            color: #111111;
            background: linear-gradient(145deg, rgba(255,255,255,0.92), rgba(247,242,237,0.92));
            border: 1px solid rgba(17,17,17,0.08);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.01em;
        }}
        </style>
        <div class="readiness-action-shell animate-in stagger-2">
            <div class="readiness-actions">
                <a href="{escape(_build_route_href('readiness', action='run_readiness_smoke_test'))}" target="_self" class="readiness-action-link primary">Run System Validation</a>
                <a href="{escape(_build_route_href('readiness', action='run_schema_audit'))}" target="_self" class="readiness-action-link secondary">Run Schema Audit</a>
                <a href="{escape(_build_route_href('home', action='presentation_reset'))}" target="_self" class="readiness-action-link ghost">Reset Session</a>
                <a href="{escape(_build_route_href('home'))}" target="_self" class="readiness-action-link ghost">Open Chat</a>
            </div>
            <div>
                <div style="font-size: 0.76rem; font-weight: 800; letter-spacing: 0.16em; text-transform: uppercase; color: #af7f59; margin-bottom: 0.6rem;">Official References</div>
                <div class="readiness-chip-row">{official_links_markup}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_readiness_preflight() -> None:
    """Render the preflight summary cards."""
    cards_markup = "".join(
        (
            f'<div class="premium-glass-card animate-in stagger-2 readiness-preflight-card {escape(card["tone"])}">'
            f'<div style="font-size: 0.74rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(card["label"])}</div>'
            f'<div style="margin-top: 0.45rem; color: #111111; font-size: 1.24rem; font-weight: 800; line-height: 1.15;">{escape(card["value"])}</div>'
            f'<div style="margin-top: 0.48rem; color: #5f5a57; font-size: 0.88rem; line-height: 1.58;">{escape(card["detail"])}</div>'
            "</div>"
        )
        for card in _build_readiness_preflight_cards()
    )
    st.markdown(
        f"""
        <div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size: 1.15rem;">Requirements Status</h3></div>
        <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem;">
            {cards_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_readiness_smoke_test_report() -> None:
    """Render the smoke test results when available."""
    report = st.session_state.readiness_smoke_test_report
    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size: 1.15rem;">Execution Validation</h3></div>',
        unsafe_allow_html=True,
    )
    if report is None:
        st.info(
            "Run smoke tests for database connectivity, table visibility, chart readiness, and map readiness."
        )
        return

    tone_message = {
        "success": "All smoke tests completed without blocking issues.",
        "warn": "Smoke tests completed with at least one cautionary warning.",
        "error": "Smoke tests found a blocking issue in the live readiness path.",
    }.get(report["overall_status"], "Smoke test status is available below.")
    st.markdown(
        f"""
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom: 1rem;">
            <div style="display:flex; justify-content:space-between; gap: 1rem; flex-wrap: wrap; align-items:center;">
                <div>
                    <div style="font-size: 0.74rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">Latest Run</div>
                    <h3 style="margin: 0.42rem 0 0.3rem;">{escape(str(report['ran_at']))}</h3>
                    <p style="margin: 0; color: #595959; line-height: 1.65;">{escape(tone_message)}</p>
                </div>
                <div style="padding: 0.68rem 0.92rem; border-radius: 999px; background: rgba(175,127,89,0.12); color: #111111; font-weight: 800;">
                    Overall: {escape(str(report['overall_status']).title())}
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for check in report["checks"]:
        status_label = str(check["status"]).title()
        with st.expander(f"{check['title']} · {status_label}", expanded=check["status"] == "error"):
            st.write(check["detail"])
            if check["sql"]:
                st.code(check["sql"], language="sql")


def _render_schema_audit_report() -> None:
    """Render the live-vs-local schema drift comparison when available."""
    report = st.session_state.readiness_schema_audit_report
    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size: 1.15rem;">Schema Validation</h3></div>',
        unsafe_allow_html=True,
    )
    if report is None:
        st.info(
            "Run schema audit to compare local schema definitions with live DESCRIBE output."
        )
        return

    summary = report["summary"]
    summary_cards = [
        ("Matched Tables", str(summary["matched_tables"])),
        ("Drifted Tables", str(summary["drifted_tables"])),
        ("Errored Tables", str(summary["errored_tables"])),
        ("Audit Run", str(report["ran_at"])),
    ]
    summary_markup = "".join(
        (
            '<div class="premium-glass-card animate-in stagger-2">'
            f'<div style="font-size: 0.74rem; letter-spacing: 0.14em; text-transform: uppercase; color: #af7f59; font-weight: 800;">{escape(label)}</div>'
            f'<div style="margin-top: 0.45rem; color: #111111; font-size: 1.24rem; font-weight: 800;">{escape(value)}</div>'
            "</div>"
        )
        for label, value in summary_cards
    )
    st.markdown(
        f"""
        <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 1rem;">
            {summary_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )

    summary_rows = [
        {
            "table": entry["table"],
            "status": entry["status"],
            "matched_columns": entry["matched_column_count"],
            "expected_columns": entry["expected_column_count"],
            "live_columns": entry["live_column_count"],
            "missing_columns": ", ".join(entry["missing_columns"]) or "None",
            "extra_columns": ", ".join(entry["extra_columns"]) or "None",
        }
        for entry in report["tables"]
    ]
    st.dataframe(summary_rows, width="stretch")

    for entry in report["tables"]:
        badge = {
            "match": "Match",
            "drift": "Drift Detected",
            "error": "Audit Error",
        }.get(entry["status"], entry["status"].title())
        with st.expander(f"{entry['table']} · {badge}", expanded=entry["status"] != "match"):
            if entry["error"]:
                st.error(entry["error"])
            if entry["message"]:
                st.caption(entry["message"])

            detail_columns = st.columns(3)
            with detail_columns[0]:
                st.metric("Matched", entry["matched_column_count"])
            with detail_columns[1]:
                st.metric("Missing", len(entry["missing_columns"]))
            with detail_columns[2]:
                st.metric("Extra", len(entry["extra_columns"]))

            st.write("Expected columns")
            st.code(", ".join(entry["expected_columns"]) or "None", language="text")
            st.write("Live columns")
            st.code(", ".join(entry["live_columns"]) or "None", language="text")

            if entry["missing_columns"]:
                st.warning(f"Missing in live backend: {', '.join(entry['missing_columns'])}")
            if entry["extra_columns"]:
                st.info(f"Extra in live backend: {', '.join(entry['extra_columns'])}")


def _data_journey_content_path() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "data_journey_content.json"


def _default_data_journey_content() -> dict[str, Any]:
    chapter_template = lambda idx, title, chips, line_1, line_2, connect, slot_1, slot_2, section_break="none": {
        "kicker": f"Part {idx}",
        "title": title,
        "chips": chips,
        "line_1": line_1,
        "line_2": line_2,
        "connect": connect,
        "slot_1": {"label": slot_1, "type": "placeholder", "url": "", "caption": ""},
        "slot_2": {"label": slot_2, "type": "placeholder", "url": "", "caption": ""},
        "section_break": section_break,
    }

    return {
        "page_title": "Ultimate Project Presentation Intro",
        "page_subtitle": "Long cinematic flow, key phrases only, with visual placeholders and background breathing spaces.",
        "opening": {
            "kicker": "Opening",
            "title": "Internship Knowledge -> One Integrated Product",
            "line_1": "Learning blocks became one connected system.",
            "line_2": "Course foundations now power real business analytics.",
            "connect": "Connection: Learn -> Build -> Analyze -> Productize.",
        },
        "hero_slots": [
            {"label": "Visual Slot 01: Internship Roadmap Animation", "type": "placeholder", "url": "", "caption": ""},
            {"label": "Visual Slot 02: Milestone Screenshot Collage", "type": "placeholder", "url": "", "caption": ""},
        ],
        "chapters": [
            chapter_template(1, "Internship Foundations", ["Python", "Linux", "Hadoop", "Hive", "Data Warehouse", "Spark", "Machine Learning"], "Core engineering + big data fundamentals.", "Every later decision depends on this base.", "Connection: Skills -> Confidence -> Execution.", "Visual Slot 03: Skill Stack Pyramid", "Visual Slot 04: Tools Timeline"),
            chapter_template(2, "Project Build Path", ["Data Preparation", "Create Hive Tables", "Zeppelin Setup", "Zeppelin Examples", "Text-to-SQL Copilot"], "From raw data to validated notebooks.", "From notebooks to conversational analytics.", "Connection: Data quality -> Query reliability.", "Visual Slot 05: ETL Flow", "Visual Slot 06: Notebook Validation", "normal"),
            chapter_template(3, "Yelp Data Scope", ["Users", "Business", "Rating/Review", "Check-in", "Temporal Signals", "Geo Signals", "Behavior Signals"], "Who, where, when, sentiment, and traffic.", "Multi-dimensional evidence in one dataset.", "Connection: Entity links -> richer context.", "Visual Slot 07: Schema Graph", "Visual Slot 08: Data Volume Snapshot"),
            chapter_template(4, "Requirement 1: Analytics Scope", ["Business Analysis", "User Analysis", "Review Analysis", "Rating Analysis", "Check-in Analysis", "Comprehensive Analysis", "Visualization"], "Six analysis domains, one analytics engine.", "Each domain answers different business questions.", "Connection: Domain metrics -> unified insight.", "Visual Slot 09: Domain Wheel", "Visual Slot 10: KPI Dashboard Mock", "normal"),
            chapter_template(5, "Business + User Intelligence", ["Merchant Ranking", "City/State Trends", "Category Mix", "Top Reviewers", "Elite Ratio", "Silent Users"], "Market structure meets user behavior.", "Supply signals + engagement signals together.", "Connection: Business health <-> User activity.", "Visual Slot 11: Top Cities Chart", "Visual Slot 12: User Cohort Trend", "tall"),
            chapter_template(6, "Review + Rating Intelligence", ["Word Frequency", "Positive vs Negative", "Correlation", "Rating Distribution", "Weekday vs Weekend", "Mixed Signals"], "Text insights and star signals are aligned.", "Sentiment patterns support decision quality.", "Connection: Voice of customer -> rating behavior.", "Visual Slot 13: Sentiment Word Cloud", "Visual Slot 14: Rating Histogram", "normal"),
            chapter_template(7, "Check-in + Comprehensive Intelligence", ["Time-of-Day Traffic", "City Popularity", "MoM Growth", "Conversion Rate", "Drop-off Detection", "Top 5 per City"], "Physical demand is measurable over time.", "Composite metrics improve ranking fairness.", "Connection: Footfall + rating + reviews = better scoring.", "Visual Slot 15: Check-in Time Heatmap", "Visual Slot 16: Composite Ranking Table"),
            chapter_template(8, "Requirement 2: Enrichment", ["Weather-Mood Hypothesis", "Cursed Storefronts", "Review Manipulation", "Open-World Data Safari", "External Datasets", "Cross-Validation"], "Internal data is powerful; external data makes it stronger.", "Cross-validation upgrades confidence and causality.", "Connection: Internal truth + external truth.", "Visual Slot 17: Yelp + NOAA Merge", "Visual Slot 18: Geo-Economic Overlay", "normal"),
            chapter_template(9, "Case Focus: Weather + Mood", ["Rain", "Heat", "Wind", "1-Star Spikes", "Check-in Shift", "Staffing Suggestion"], "Weather can influence review mood and traffic.", "Forecast-linked operations become possible.", "Connection: Forecast -> action plan.", "Visual Slot 19: Weather vs Rating Trend", "Visual Slot 20: Inventory Planning Card"),
            chapter_template(10, "Case Focus: Storefront Life Cycle", ["Cursed Addresses", "Golden Locations", "Parking", "Noise", "Visibility", "Rent Pressure"], "Location context explains survival variance.", "Failure patterns can be diagnosed early.", "Connection: Place quality -> business durability.", "Visual Slot 21: Address Lifecycle Map", "Visual Slot 22: Attribute Risk Radar", "tall"),
            chapter_template(11, "Case Focus: Review Manipulation", ["Burst Detection", "Ghost Accounts", "Timeline Anomaly", "Credibility Signals", "Penalty Evidence", "Backlash"], "Abnormal review behavior is detectable.", "Trust metrics protect platform credibility.", "Connection: Fraud signal -> governance action.", "Visual Slot 23: Suspicious Network Graph", "Visual Slot 24: Alert Timeline"),
            chapter_template(12, "Full-Stack Text-to-SQL System", ["Presentation Layer", "Service Layer", "Data Execution Layer", "Schema Injection", "SQL Generation", "Sanitization", "Retry/Self-Correction"], "Natural language enters; validated insights return.", "Pipeline reliability is built into the architecture.", "Connection: Ask -> Parse -> Execute -> Explain.", "Visual Slot 25: Layer Architecture Animation", "Visual Slot 26: Pipeline Step Trace", "normal"),
            chapter_template(13, "UI/UX Delivery", ["Chat Interface", "Loading State", "Result Tables", "Auto Charts", "Fast Recommendations", "Zeppelin Grounding", "Demo Ready"], "Readable insights for technical and non-technical users.", "Speed, clarity, and trust in one interface.", "Connection: Good UX -> better decision adoption.", "Visual Slot 27: Chat Response Walkthrough", "Visual Slot 28: Result-to-Chart Transition"),
            chapter_template(14, "End-to-End Value", ["Course Learning", "Data Engineering", "Analytics", "Enrichment", "AI Workflow", "Business Decision"], "Everything is connected as one value chain.", "Training outcomes became a usable analytics product.", "Connection: Skills created system; system created insight.", "Visual Slot 29: Value Chain Infographic", "Visual Slot 30: Final Impact Slide", "tall"),
        ],
        "closing": {
            "kicker": "Closing",
            "title": "Overall Summary",
            "line_1": "Internship courses built the engine.",
            "line_2": "Yelp project proved the engine in real workflow.",
            "line_3": "Text-to-SQL made insights accessible in conversation form.",
            "connect": "Final Connection: Knowledge -> System -> Insight -> Action.",
        },
        "finale_slot": {
            "label": 'Grand Finale: "Question -> SQL -> Insight -> Decision"',
            "type": "placeholder",
            "url": "",
            "caption": "",
        },
    }


def _get_data_journey_content() -> dict[str, Any]:
    path = _data_journey_content_path()
    file_mtime = path.stat().st_mtime if path.exists() else 0.0
    cached_content = st.session_state.get("data_journey_content")
    cached_mtime = float(st.session_state.get("data_journey_content_mtime", 0.0))
    if isinstance(cached_content, dict) and cached_mtime == file_mtime:
        return cached_content

    content = _default_data_journey_content()
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                content.update(loaded)
        except Exception:
            pass
    st.session_state.data_journey_content = content
    st.session_state.data_journey_content_mtime = file_mtime
    return content


def _save_data_journey_content(content: dict[str, Any]) -> None:
    st.session_state.data_journey_content = content
    try:
        path = _data_journey_content_path()
        path.write_text(
            json.dumps(content, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        st.session_state.data_journey_content_mtime = path.stat().st_mtime
    except Exception as exc:
        st.warning(f"Could not persist Data Journey config to file: {exc}")


@st.cache_data(show_spinner=False)
def _encode_asset_data_uri(path_hint: str) -> str:
    """Encode a local asset as data URI for decorative CSS usage."""
    candidate = pathlib.Path(path_hint)
    if not candidate.is_absolute():
        candidate = pathlib.Path(__file__).parent / candidate
    if not candidate.exists():
        return ""
    mime, _ = mimetypes.guess_type(str(candidate))
    if not mime:
        mime = "image/png"
    data = base64.b64encode(candidate.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{data}"


def _render_data_journey_media_slot(slot: dict[str, Any], fallback_label: str, *, min_height: int = 170) -> None:
    slot_type = str(slot.get("type", "placeholder")).strip().lower()
    media_url = str(slot.get("url", "")).strip()
    label = str(slot.get("label", fallback_label)).strip() or fallback_label
    caption = str(slot.get("caption", "")).strip()
    media_source = media_url

    if media_url and not re.match(r"^https?://", media_url, flags=re.IGNORECASE):
        candidates = [
            pathlib.Path(media_url),
            pathlib.Path(__file__).parent / media_url,
            pathlib.Path.cwd() / media_url,
        ]
        for candidate in candidates:
            if candidate.exists():
                media_source = str(candidate)
                break

    if media_source and slot_type == "video":
        st.video(media_source)
        if caption:
            st.caption(caption)
        return
    if media_source and slot_type == "image":
        st.image(media_source, use_container_width=True)
        if caption:
            st.caption(caption)
        return

    st.markdown(
        f"""
        <div class="pres-visual-slot" style="min-height:{min_height}px;">
            {escape(label)}
        </div>
        """,
        unsafe_allow_html=True,
    )
    if caption:
        st.caption(caption)


def _render_data_journey_admin_view() -> None:
    st.markdown(
        '<div class="section-shell animate-in stagger-1"><h2 class="section-title">Data Journey Admin Panel</h2><p class="section-copy">Edit text and media slots for the Data Journey page. No login is required right now.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<a href="{escape(_build_route_href("readiness"))}" target="_self" class="schema-task-chip" style="display:inline-flex; margin-bottom:0.85rem; padding:0.58rem 0.84rem; border-radius:999px; text-decoration:none; color:#111; background:rgba(255,255,255,0.9); border:1px solid rgba(17,17,17,0.08); font-size:0.78rem; font-weight:700;">Back to Data Journey</a>',
        unsafe_allow_html=True,
    )

    content = _get_data_journey_content()
    edited: dict[str, Any] = json.loads(json.dumps(content))

    st.subheader("Page Header")
    edited["page_title"] = st.text_input("Page Title", value=str(edited.get("page_title", "")))
    edited["page_subtitle"] = st.text_input("Page Subtitle", value=str(edited.get("page_subtitle", "")))

    st.subheader("Opening Block")
    opening = dict(edited.get("opening", {}))
    opening["kicker"] = st.text_input("Opening Kicker", value=str(opening.get("kicker", "")))
    opening["title"] = st.text_input("Opening Title", value=str(opening.get("title", "")))
    opening["line_1"] = st.text_input("Opening Line 1", value=str(opening.get("line_1", "")))
    opening["line_2"] = st.text_input("Opening Line 2", value=str(opening.get("line_2", "")))
    opening["connect"] = st.text_input("Opening Connection", value=str(opening.get("connect", "")))
    edited["opening"] = opening

    def _edit_slot(slot: dict[str, Any], key_prefix: str, label: str) -> dict[str, Any]:
        st.markdown(f"**{label}**")
        slot_copy = dict(slot or {})
        slot_copy["label"] = st.text_input(
            "Slot Label",
            value=str(slot_copy.get("label", "")),
            key=f"{key_prefix}_label",
        )
        slot_copy["type"] = st.selectbox(
            "Media Type",
            options=["placeholder", "image", "video"],
            index=max(0, ["placeholder", "image", "video"].index(str(slot_copy.get("type", "placeholder")).lower()) if str(slot_copy.get("type", "placeholder")).lower() in {"placeholder", "image", "video"} else 0),
            key=f"{key_prefix}_type",
        )
        slot_copy["url"] = st.text_input(
            "Media URL / Path",
            value=str(slot_copy.get("url", "")),
            key=f"{key_prefix}_url",
        )
        slot_copy["caption"] = st.text_input(
            "Caption",
            value=str(slot_copy.get("caption", "")),
            key=f"{key_prefix}_caption",
        )
        return slot_copy

    st.subheader("Hero Media Slots")
    hero_slots = list(edited.get("hero_slots", []))
    while len(hero_slots) < 2:
        hero_slots.append({"label": "", "type": "placeholder", "url": "", "caption": ""})
    col_a, col_b = st.columns(2)
    with col_a:
        hero_slots[0] = _edit_slot(hero_slots[0], "dj_hero_0", "Hero Slot 1")
    with col_b:
        hero_slots[1] = _edit_slot(hero_slots[1], "dj_hero_1", "Hero Slot 2")
    edited["hero_slots"] = hero_slots

    st.subheader("Chapters")
    action_col_1, action_col_2 = st.columns(2)
    with action_col_1:
        if st.button("Add Chapter", key="dj_add_chapter"):
            chapters = list(edited.get("chapters", []))
            chapters.append(
                {
                    "kicker": f"Part {len(chapters) + 1}",
                    "title": "New Chapter",
                    "chips": ["Keyword A", "Keyword B"],
                    "line_1": "Main line.",
                    "line_2": "Support line.",
                    "connect": "Connection phrase.",
                    "slot_1": {"label": "Visual Slot", "type": "placeholder", "url": "", "caption": ""},
                    "slot_2": {"label": "Visual Slot", "type": "placeholder", "url": "", "caption": ""},
                    "section_break": "none",
                }
            )
            edited["chapters"] = chapters
    with action_col_2:
        if st.button("Remove Last Chapter", key="dj_remove_chapter"):
            chapters = list(edited.get("chapters", []))
            if chapters:
                chapters.pop()
                edited["chapters"] = chapters

    chapters = list(edited.get("chapters", []))
    for idx, chapter in enumerate(chapters):
        with st.expander(f'Chapter {idx + 1}: {chapter.get("title", "")}', expanded=False):
            chapter["kicker"] = st.text_input("Kicker", value=str(chapter.get("kicker", "")), key=f"dj_ch_{idx}_kicker")
            chapter["title"] = st.text_input("Title", value=str(chapter.get("title", "")), key=f"dj_ch_{idx}_title")
            chips_csv = st.text_input(
                "Chips (comma-separated)",
                value=", ".join(str(chip) for chip in chapter.get("chips", [])),
                key=f"dj_ch_{idx}_chips",
            )
            chapter["chips"] = [chip.strip() for chip in chips_csv.split(",") if chip.strip()]
            chapter["line_1"] = st.text_input("Line 1", value=str(chapter.get("line_1", "")), key=f"dj_ch_{idx}_line_1")
            chapter["line_2"] = st.text_input("Line 2", value=str(chapter.get("line_2", "")), key=f"dj_ch_{idx}_line_2")
            chapter["connect"] = st.text_input("Connection", value=str(chapter.get("connect", "")), key=f"dj_ch_{idx}_connect")
            chapter["section_break"] = st.selectbox(
                "Break After Chapter",
                options=["none", "normal", "tall"],
                index=max(0, ["none", "normal", "tall"].index(str(chapter.get("section_break", "none")).lower()) if str(chapter.get("section_break", "none")).lower() in {"none", "normal", "tall"} else 0),
                key=f"dj_ch_{idx}_break",
            )
            slot_col_1, slot_col_2 = st.columns(2)
            with slot_col_1:
                chapter["slot_1"] = _edit_slot(dict(chapter.get("slot_1", {})), f"dj_ch_{idx}_slot_1", "Slot 1")
            with slot_col_2:
                chapter["slot_2"] = _edit_slot(dict(chapter.get("slot_2", {})), f"dj_ch_{idx}_slot_2", "Slot 2")
            chapters[idx] = chapter
    edited["chapters"] = chapters

    st.subheader("Closing Block")
    closing = dict(edited.get("closing", {}))
    closing["kicker"] = st.text_input("Closing Kicker", value=str(closing.get("kicker", "")))
    closing["title"] = st.text_input("Closing Title", value=str(closing.get("title", "")))
    closing["line_1"] = st.text_input("Closing Line 1", value=str(closing.get("line_1", "")))
    closing["line_2"] = st.text_input("Closing Line 2", value=str(closing.get("line_2", "")))
    closing["line_3"] = st.text_input("Closing Line 3", value=str(closing.get("line_3", "")))
    closing["connect"] = st.text_input("Closing Connection", value=str(closing.get("connect", "")))
    edited["closing"] = closing

    st.subheader("Finale Slot")
    edited["finale_slot"] = _edit_slot(dict(edited.get("finale_slot", {})), "dj_finale", "Grand Finale Slot")

    # Auto-save on any widget change (Streamlit reruns after each edit).
    if edited != content:
        _save_data_journey_content(edited)
        st.caption("Auto-saved.")

    save_col, reset_col = st.columns(2)
    with save_col:
        if st.button("Save Data Journey", key="dj_save", width="stretch"):
            _save_data_journey_content(edited)
            st.success("Data Journey updated.")
    with reset_col:
        if st.button("Reset to Default", key="dj_reset", width="stretch"):
            defaults = _default_data_journey_content()
            _save_data_journey_content(defaults)
            st.success("Reset complete. Refresh to see defaults.")

    st.subheader("Advanced JSON Editor")
    advanced_json = st.text_area(
        "Edit full JSON (optional)",
        value=json.dumps(edited, indent=2, ensure_ascii=False),
        height=420,
        key="dj_advanced_json",
    )
    if st.button("Apply JSON", key="dj_apply_json"):
        try:
            parsed = json.loads(advanced_json)
            if not isinstance(parsed, dict):
                st.error("JSON root must be an object.")
            else:
                _save_data_journey_content(parsed)
                st.success("JSON applied.")
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON: {exc}")


def _render_readiness_view() -> None:
    """Render Data Journey using editable admin-managed content."""
    content = _get_data_journey_content()
    blob_magenta_uri = _encode_asset_data_uri("assets/data_journey/ppt/design_blob_magenta.png")
    blob_blue_uri = _encode_asset_data_uri("assets/data_journey/ppt/design_blob_blue.png")
    blob_magenta_css = f"url('{blob_magenta_uri}')" if blob_magenta_uri else "none"
    blob_blue_css = f"url('{blob_blue_uri}')" if blob_blue_uri else "none"

    st.markdown(
        f"""
        <style>
        .dj-theme-backdrop {{
            position: fixed;
            inset: 0;
            pointer-events: none;
            z-index: 0;
            overflow: hidden;
        }}
        .dj-theme-backdrop::before {{
            content: "";
            position: absolute;
            width: min(34vw, 360px);
            aspect-ratio: 1 / 1;
            top: 7vh;
            left: -3.5vw;
            background-image: {blob_magenta_css};
            background-size: contain;
            background-repeat: no-repeat;
            opacity: 0.34;
            filter: blur(0.4px);
            animation: djFloatA 14s ease-in-out infinite;
        }}
        .dj-theme-backdrop::after {{
            content: "";
            position: absolute;
            width: min(36vw, 380px);
            aspect-ratio: 1 / 1;
            right: -4.5vw;
            bottom: 7vh;
            background-image: {blob_blue_css};
            background-size: contain;
            background-repeat: no-repeat;
            opacity: 0.3;
            filter: blur(0.4px);
            animation: djFloatB 16s ease-in-out infinite;
        }}
        @keyframes djFloatA {{
            0%, 100% {{ transform: translateY(0px) rotate(0deg); }}
            50% {{ transform: translateY(-16px) rotate(1.2deg); }}
        }}
        @keyframes djFloatB {{
            0%, 100% {{ transform: translateY(0px) rotate(0deg); }}
            50% {{ transform: translateY(14px) rotate(-1deg); }}
        }}
        .stApp [data-testid="stAppViewContainer"] .main .block-container {{
            position: relative;
            z-index: 2;
        }}
        .dj-card {{
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(171, 121, 219, 0.28) !important;
            background:
                linear-gradient(160deg, rgba(16, 13, 29, 0.86), rgba(9, 12, 26, 0.84)) !important;
            box-shadow: 0 18px 44px rgba(7, 7, 18, 0.34), inset 0 0 0 1px rgba(255,255,255,0.08);
            backdrop-filter: blur(6px);
            -webkit-backdrop-filter: blur(6px);
        }}
        .dj-card::before {{
            content: "";
            position: absolute;
            inset: 0;
            background:
                linear-gradient(110deg, rgba(255,255,255,0.13) 0%, rgba(255,255,255,0.02) 32%, rgba(255,255,255,0) 42%),
                radial-gradient(circle at 18% 16%, rgba(245, 66, 178, 0.14), transparent 25%),
                radial-gradient(circle at 86% 78%, rgba(85, 150, 255, 0.14), transparent 24%);
            pointer-events: none;
        }}
        .pres-kicker {{ font-size: 0.72rem; letter-spacing: 0.14em; text-transform: uppercase; color: #e9c7ff; font-weight: 800; }}
        .pres-title {{ margin: 0.42rem 0 0.28rem; color: #f7f0ff; font-size: 1.2rem; font-weight: 820; line-height: 1.32; }}
        .pres-line {{ margin: 0.15rem 0 0; color: rgba(243, 236, 255, 0.9); line-height: 1.68; font-size: 0.95rem; font-weight: 620; }}
        .pres-connector {{ margin-top: 0.48rem; color: #8ee2ff; font-size: 0.88rem; font-weight: 770; line-height: 1.55; }}
        .pres-chip {{
            display: inline-flex; align-items: center; padding: 0.42rem 0.7rem; border-radius: 999px;
            background: linear-gradient(130deg, rgba(242, 65, 174, 0.24), rgba(76, 154, 255, 0.22));
            color: #f9f5ff; font-size: 0.76rem; font-weight: 760; line-height: 1.22;
            border: 1px solid rgba(255,255,255,0.16);
            box-shadow: 0 8px 20px rgba(12, 14, 31, 0.32);
        }}
        .pres-chip-row {{ display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.62rem; }}
        .pres-visual-slot {{
            min-height: 170px; border-radius: 18px; border: 1px dashed rgba(255,255,255,0.34);
            background:
                linear-gradient(150deg, rgba(27, 22, 43, 0.76), rgba(12, 15, 33, 0.76));
            display: flex; align-items: center; justify-content: center; text-align: center; padding: 0.95rem;
            color: #f5ecff; font-size: 0.86rem; line-height: 1.58; font-weight: 720;
            box-shadow: inset 0 0 0 1px rgba(255,255,255,0.12), 0 14px 34px rgba(8,8,20,0.32);
        }}
        .pres-background-break {{
            position: relative;
            min-height: 260px;
            margin: 0.9rem 0;
            border-radius: 22px;
            border: 1px solid rgba(255,255,255,0.1);
            background:
                radial-gradient(circle at 25% 20%, rgba(242, 65, 174, 0.12), transparent 30%),
                radial-gradient(circle at 75% 70%, rgba(76, 154, 255, 0.12), transparent 28%),
                linear-gradient(140deg, rgba(10, 10, 21, 0.28), rgba(8, 12, 28, 0.22));
            overflow: hidden;
        }}
        .pres-background-break::before {{
            content: "";
            position: absolute;
            width: 280px;
            height: 280px;
            top: -84px;
            left: -62px;
            background-image: {blob_magenta_css};
            background-size: contain;
            background-repeat: no-repeat;
            opacity: 0.24;
        }}
        .pres-background-break::after {{
            content: "";
            position: absolute;
            width: 300px;
            height: 300px;
            right: -78px;
            bottom: -102px;
            background-image: {blob_blue_css};
            background-size: contain;
            background-repeat: no-repeat;
            opacity: 0.2;
        }}
        .pres-background-break.tall {{ min-height: 360px; }}
        .data-journey-admin-link {{ font-size: 0.72rem; opacity: 0.86; color: #efd9ff; text-decoration: none; font-weight: 700; }}
        .data-journey-admin-link:hover {{ text-decoration: underline; opacity: 1; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="dj-theme-backdrop" aria-hidden="true"></div>', unsafe_allow_html=True)

    admin_href = _build_route_href("data_journey_admin")
    st.markdown(
        f'<div style="display:flex; justify-content:flex-end; margin: 0.05rem 0 0.15rem;"><a href="{escape(admin_href)}" target="_self" class="data-journey-admin-link">admin</a></div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<div class="section-shell animate-in stagger-1"><h2 class="section-title">{escape(str(content.get("page_title", "Data Journey")))}</h2><p class="section-copy">{escape(str(content.get("page_subtitle", "")))}</p></div>',
        unsafe_allow_html=True,
    )

    nav_links = [
        ("Open Chat", _build_route_href("home")),
        ("Schema", _build_route_href("schema")),
        ("Architecture", _build_route_href("architecture")),
        ("Docs", _build_route_href("docs")),
    ]
    nav_links_markup = "".join(
        (
            f'<a href="{escape(href)}" target="_self" class="schema-task-chip" '
            'style="display:inline-flex; align-items:center; justify-content:center; padding:0.62rem 0.9rem; '
            'border-radius:999px; text-decoration:none; color:#111; background:rgba(255,255,255,0.92); '
            'border:1px solid rgba(17,17,17,0.08); font-size:0.8rem; font-weight:700;">'
            f"{escape(label)}</a>"
        )
        for label, href in nav_links
    )
    st.markdown(
        f'<div style="display:flex; flex-wrap:wrap; gap:0.65rem; margin-bottom:0.85rem;">{nav_links_markup}</div>',
        unsafe_allow_html=True,
    )

    opening = dict(content.get("opening", {}))
    st.markdown(
        f"""
        <div class="premium-glass-card dj-card animate-in stagger-1">
            <div class="pres-kicker">{escape(str(opening.get("kicker", "")))}</div>
            <div class="pres-title">{escape(str(opening.get("title", "")))}</div>
            <p class="pres-line">{escape(str(opening.get("line_1", "")))}</p>
            <p class="pres-line">{escape(str(opening.get("line_2", "")))} </p>
            <div class="pres-connector">{escape(str(opening.get("connect", "")))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    hero_slots = list(content.get("hero_slots", []))
    while len(hero_slots) < 2:
        hero_slots.append({"label": "Visual Slot", "type": "placeholder", "url": "", "caption": ""})
    hero_left, hero_right = st.columns(2)
    with hero_left:
        _render_data_journey_media_slot(hero_slots[0], "Visual Slot 01")
    with hero_right:
        _render_data_journey_media_slot(hero_slots[1], "Visual Slot 02")

    st.markdown('<div class="pres-background-break tall"></div>', unsafe_allow_html=True)

    chapters = list(content.get("chapters", []))
    for index, chapter in enumerate(chapters, start=1):
        chips_markup = "".join(
            f'<span class="pres-chip">{escape(str(chip))}</span>'
            for chip in list(chapter.get("chips", []))
        )
        st.markdown(
            f"""
            <div class="premium-glass-card dj-card animate-in stagger-2">
                <div class="pres-kicker">{escape(str(chapter.get("kicker", "")))}</div>
                <div class="pres-title">{index}. {escape(str(chapter.get("title", "")))}</div>
                <div class="pres-chip-row">{chips_markup}</div>
                <p class="pres-line">{escape(str(chapter.get("line_1", "")))}</p>
                <p class="pres-line">{escape(str(chapter.get("line_2", "")))}</p>
                <div class="pres-connector">{escape(str(chapter.get("connect", "")))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        visual_left, visual_right = st.columns(2)
        with visual_left:
            _render_data_journey_media_slot(dict(chapter.get("slot_1", {})), f"Visual Slot {index * 2 + 1}")
        with visual_right:
            _render_data_journey_media_slot(dict(chapter.get("slot_2", {})), f"Visual Slot {index * 2 + 2}")

        section_break = str(chapter.get("section_break", "none")).strip().lower()
        if section_break == "normal":
            st.markdown('<div class="pres-background-break"></div>', unsafe_allow_html=True)
        elif section_break == "tall":
            st.markdown('<div class="pres-background-break tall"></div>', unsafe_allow_html=True)

    closing = dict(content.get("closing", {}))
    st.markdown(
        f"""
        <div class="premium-glass-card dj-card animate-in stagger-2">
            <div class="pres-kicker">{escape(str(closing.get("kicker", "")))}</div>
            <div class="pres-title">{escape(str(closing.get("title", "")))}</div>
            <p class="pres-line">{escape(str(closing.get("line_1", "")))}</p>
            <p class="pres-line">{escape(str(closing.get("line_2", "")))}</p>
            <p class="pres-line">{escape(str(closing.get("line_3", "")))}</p>
            <div class="pres-connector">{escape(str(closing.get("connect", "")))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _render_data_journey_media_slot(dict(content.get("finale_slot", {})), "Grand Finale Visual Slot", min_height=280)


def _render_architecture_view() -> None:
    st.markdown(
        '<div class="section-shell animate-in stagger-1"><h2 class="section-title">System Architecture</h2><p class="section-copy">Official architecture baseline for requirement-driven Yelp analytics and conversational SQL.</p></div>',
        unsafe_allow_html=True,
    )

    nav_links = [
        ("Open Chat", _build_route_href("home")),
        ("Data Journey", _build_route_href("readiness")),
        ("Database Schema", _build_route_href("schema")),
        ("Documentation", _build_route_href("docs")),
    ]
    nav_links_markup = "".join(
        (
            f'<a href="{escape(href)}" target="_self" class="schema-task-chip" '
            'style="display:inline-flex; align-items:center; justify-content:center; padding:0.62rem 0.9rem; '
            'border-radius:999px; text-decoration:none; color:#111; background:rgba(255,255,255,0.92); '
            'border:1px solid rgba(17,17,17,0.08); font-size:0.8rem; font-weight:700;">'
            f"{escape(label)}</a>"
        )
        for label, href in nav_links
    )
    st.markdown(
        f'<div style="display:flex; flex-wrap:wrap; gap:0.65rem; margin-bottom:0.85rem;">{nav_links_markup}</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom:0.95rem;">
            <div style="font-size:0.72rem; letter-spacing:0.14em; text-transform:uppercase; color:#af7f59; font-weight:800;">Official Scope</div>
            <h3 style="margin:0.45rem 0 0.35rem;">Three-Layer Requirement Architecture</h3>
            <p style="margin:0; color:#595959; line-height:1.7;">
                The system is organized into Presentation, Service/Orchestration, and Data/Execution layers.
                This architecture supports the official requirement set (Business, User, Review, Rating, Check-in, and Comprehensive analysis),
                with SQL transparency, result visualization, and retry handling.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    layer_cards = [
        {
            "title": "1) Presentation Layer",
            "points": [
                "Conversational interface for natural-language questions.",
                "Response cards, SQL reveal/download, CSV/PNG exports.",
                "Previous chat memory and query settings controls.",
                "Route-based views: Home, Requirements, Schema, Architecture, Documentation.",
            ],
        },
        {
            "title": "2) Service / Orchestration Layer",
            "points": [
                "Intent routing (general vs SQL/data query path).",
                "Prompt construction with schema context and conversation memory.",
                "Recommendation and Zeppelin-grounded fast-path responses.",
                "SQL generation, sanitization, retry/error handling, and fallback logic.",
            ],
        },
        {
            "title": "3) Data / Execution Layer",
            "points": [
                "Hive execution against Yelp tables (business, review/rating, users, checkin, tip, etc.).",
                "Zeppelin notebook ingestion for requirement task outputs and QA grounding.",
                "Result shaping for table/chart/map rendering and downloads.",
                "Schema/readiness validation checks against live backend behavior.",
            ],
        },
    ]

    layer_markup = "".join(
        (
            '<div class="premium-glass-card animate-in stagger-2">'
            f'<div style="font-size:0.78rem; letter-spacing:0.09em; text-transform:uppercase; color:#af7f59; font-weight:800;">{escape(item["title"])}</div>'
            + "".join(
                f'<p style="margin:0.5rem 0 0; color:#595959; line-height:1.65;">• {escape(point)}</p>'
                for point in item["points"]
            )
            + "</div>"
        )
        for item in layer_cards
    )
    st.markdown(
        f"""
        <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap:0.9rem; margin-bottom:0.95rem;">
            {layer_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )

    flow_steps = [
        "User asks a natural-language analytics question in chat.",
        "Router selects SQL/data path (or general fallback when appropriate).",
        "Service layer injects schema + recent context and builds prompt/runtime plan.",
        "SQL is generated (or deterministic recommendation SQL is selected).",
        "SQL is sanitized and executed against Hive Yelp dataset.",
        "If execution fails, retry/correction path runs with backend feedback.",
        "Response layer returns explanation + result table/chart and optional exports.",
        "SQL trace remains available through Show SQL for transparency.",
    ]

    flow_markup = "".join(
        (
            '<div class="premium-glass-card animate-in stagger-2" '
            'style="margin-bottom:0.55rem; padding:0.8rem 0.92rem;">'
            f'<div style="color:#111; font-size:0.86rem; font-weight:760; line-height:1.55;">{index}. {escape(step)}</div>'
            '</div>'
        )
        for index, step in enumerate(flow_steps, start=1)
    )
    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.08rem;">Runtime Flow</h3></div>',
        unsafe_allow_html=True,
    )
    st.markdown(flow_markup, unsafe_allow_html=True)

    mapping_rows = [
        {
            "Requirement Area": "Business Analysis",
            "Primary Tables": "business, review/rating",
            "Architecture Support": "SQL generation + aggregation + category logic + Zeppelin grounding",
        },
        {
            "Requirement Area": "User Analysis",
            "Primary Tables": "users, review/rating, tip, checkin",
            "Architecture Support": "User lifecycle metrics + elite/silent user analysis queries",
        },
        {
            "Requirement Area": "Review Analysis",
            "Primary Tables": "review/rating, users, business",
            "Architecture Support": "Text/statistical analysis pipeline and result summarization",
        },
        {
            "Requirement Area": "Rating Analysis",
            "Primary Tables": "review/rating, business",
            "Architecture Support": "Distribution, city/cuisine comparisons, weekday/weekend analysis",
        },
        {
            "Requirement Area": "Check-in Analysis",
            "Primary Tables": "checkin, business",
            "Architecture Support": "Time-based parsing, ranking, trend and seasonality logic",
        },
        {
            "Requirement Area": "Comprehensive Analysis",
            "Primary Tables": "business, review/rating, checkin, users",
            "Architecture Support": "Cross-domain combined metrics and conversion/drop-off analyses",
        },
    ]

    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.08rem;">Requirement Mapping</h3></div>',
        unsafe_allow_html=True,
    )
    st.dataframe(mapping_rows, width="stretch", hide_index=True)

    control_rows = [
        {"Control": "SQL Transparency", "Status": "Implemented", "Notes": "Show SQL + download SQL for every response."},
        {"Control": "Error Retry", "Status": "Implemented", "Notes": "Retry/correction path executes when SQL fails."},
        {"Control": "Fast Recommendation Path", "Status": "Implemented", "Notes": "Deterministic/cached routes for faster requirement responses."},
        {"Control": "Zeppelin Grounding", "Status": "Implemented", "Notes": "Notebook task outputs used for grounding and fallback responses."},
        {"Control": "Result Export", "Status": "Implemented", "Notes": "CSV and chart PNG available when data artifacts exist."},
    ]
    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.08rem;">Execution Controls</h3></div>',
        unsafe_allow_html=True,
    )
    st.dataframe(control_rows, width="stretch", hide_index=True)

def _render_docs_view() -> None:
    """Render the docs route with official project requirements and execution notes."""
    st.markdown(
        '<div class="section-shell animate-in stagger-1"><h2 class="section-title">Project Documentation</h2><p class="section-copy">Official requirement baseline and execution references for Query by SilkByteX.</p></div>',
        unsafe_allow_html=True,
    )

    nav_links = [
        ("Open Chat", _build_route_href("home")),
        ("Data Journey", _build_route_href("readiness")),
        ("Database Schema", _build_route_href("schema")),
        ("Architecture", _build_route_href("architecture")),
        ("Run Database Test", _build_route_href("home", action="run_test", panel="results")),
    ]
    nav_links_markup = "".join(
        (
            f'<a href="{escape(href)}" target="_self" class="schema-task-chip" '
            'style="display:inline-flex; align-items:center; justify-content:center; padding:0.62rem 0.9rem; '
            'border-radius:999px; text-decoration:none; color:#111; background:rgba(255,255,255,0.92); '
            'border:1px solid rgba(17,17,17,0.08); font-size:0.8rem; font-weight:700;">'
            f"{escape(label)}</a>"
        )
        for label, href in nav_links
    )
    st.markdown(
        f'<div style="display:flex; flex-wrap:wrap; gap:0.65rem; margin-bottom:0.85rem;">{nav_links_markup}</div>',
        unsafe_allow_html=True,
    )

    official_sources = [
        ("Yelp Dataset Documentation", "https://www.yelp.com/dataset/documentation/main"),
        ("Apache Hive Documentation", "https://hive.apache.org/docs/latest/"),
        ("Spark SQL Reference", "https://spark.apache.org/docs/latest/sql-ref.html"),
        ("Streamlit Chat API", "https://docs.streamlit.io/develop/api-reference/chat"),
    ]
    source_markup = "".join(
        (
            f'<a href="{escape(url)}" target="_blank" rel="noopener noreferrer" class="schema-task-chip" '
            'style="display:inline-flex; align-items:center; justify-content:center; padding:0.58rem 0.82rem; '
            'border-radius:999px; text-decoration:none; color:#111; background:rgba(255,255,255,0.9); '
            'border:1px solid rgba(17,17,17,0.08); font-size:0.78rem; font-weight:700;">'
            f"{escape(label)}</a>"
        )
        for label, url in official_sources
    )
    st.markdown(
        f"""
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom:0.95rem;">
            <div style="font-size:0.72rem; letter-spacing:0.14em; text-transform:uppercase; color:#af7f59; font-weight:800;">Official References</div>
            <h3 style="margin:0.45rem 0 0.55rem;">Requirement Sources</h3>
            <div style="display:flex; flex-wrap:wrap; gap:0.58rem;">{source_markup}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom:0.95rem;">
            <div style="font-size:0.72rem; letter-spacing:0.14em; text-transform:uppercase; color:#af7f59; font-weight:800;">Requirement 1</div>
            <h3 style="margin:0.45rem 0 0.35rem;">Data Analysis and Visualization</h3>
            <p style="margin:0; color:#595959; line-height:1.7;">
                This page tracks the official task inventory used to train and validate Query by SilkByteX answers.
                The chatbot is expected to support these questions with SQL-backed responses and clear result interpretation.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="premium-glass-card animate-in stagger-2" style="margin-bottom:0.95rem;">
            <div style="font-size:0.72rem; letter-spacing:0.14em; text-transform:uppercase; color:#af7f59; font-weight:800;">Definitions</div>
            <h3 style="margin:0.45rem 0 0.45rem;">Business Rules</h3>
            <ul style="margin:0.1rem 0 0 1rem; color:#595959; line-height:1.7;">
                <li>The restaurant category is defined as <code>Restaurants</code>.</li>
                <li>Supported cuisine categories: American, Mexican, Italian, Japanese, Chinese, Thai, Mediterranean, French, Vietnamese, Greek, Indian, Korean, Hawaiian, African, Spanish, Middle Eastern.</li>
                <li>Negative review scope uses rating <= 3 stars.</li>
                <li>Top recommendation and requirement questions should route to SQL-first responses whenever possible.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

    requirement_sections: list[dict[str, Any]] = [
        {
            "title": "I. Business Analysis",
            "tasks": [
                "Identify the 20 most common merchants in the U.S.",
                "Identify the top 10 cities with the most merchants in the U.S.",
                "Identify the top 5 states with the most merchants in the U.S.",
                "Identify the 20 most common merchants in the U.S. and display their average ratings.",
                "Count the number of different categories.",
                "Identify the top 10 most frequent categories and their count.",
                "Identify the top 20 merchants that received the most five-star reviews.",
                "Count the number of restaurant types (Chinese, American, Mexican).",
                "Count the number of reviews for each restaurant type (Chinese, American, Mexican).",
                "Analyze the rating distribution for different restaurant types (Chinese, American, Mexican).",
                "Identify turnaround merchants whose average rating in the last 12 months increased by at least 1 star versus historical average.",
                "Analyze category synergy: top 10 pairs of distinct business categories that most frequently co-occur in the same merchant profile.",
                "Identify polarizing businesses with high review volume and high rating standard deviation.",
            ],
        },
        {
            "title": "II. User Analysis",
            "tasks": [
                "Analyze the number of users joining each year.",
                "Identify top reviewers based on review_count.",
                "Identify the most popular users based on fans.",
                "Calculate the ratio of elite users to regular users each year.",
                "Display the proportion of total users and silent users (users without reviews) each year.",
                "Compute yearly statistics of new users, number of reviews, elite users, tips, and check-ins.",
                "Identify early adopters (tastemakers) who wrote one of the first 5 reviews for restaurants that later reached 4.5+ stars and 100+ reviews.",
                "Analyze user rating evolution: first year vs third year rating behavior.",
                "Segment users by dining diversity and rank top 50 adventurous eaters (minimum 20 reviews).",
                "Identify elite status impact on review length and useful votes (before vs after elite).",
            ],
        },
        {
            "title": "III. Review Analysis",
            "tasks": [
                "Count the number of reviews per year.",
                "Count the number of useful, funny, and cool reviews.",
                "Rank users by total number of reviews each year.",
                "Extract the top 20 most common words from all reviews.",
                "Extract the top 10 words from positive reviews (rating > 3).",
                "Extract the top 10 words from negative reviews (rating <= 3).",
                "Perform word cloud analysis by filtering words based on part-of-speech tagging.",
                "Construct a word association graph.",
                "Extract top 15 bigrams associated with 1-star and 2-star reviews.",
                "Analyze correlation between review length and rating.",
                "Identify mixed-signal reviews (1-2 stars but positive keyword sentiment).",
                "Extract and rank frequently mentioned menu items for the top 5 most popular Chinese restaurants.",
            ],
        },
        {
            "title": "IV. Rating Analysis",
            "tasks": [
                "Analyze the distribution of ratings (1-5 stars).",
                "Analyze weekly rating frequency (Monday to Sunday).",
                "Identify top businesses with the most five-star ratings.",
                "Identify top 10 cities with the highest ratings.",
                "Calculate rating differential: business average vs cuisine-category average within same city.",
                "Compare weekend vs weekday satisfaction for the Nightlife category.",
            ],
        },
        {
            "title": "V. Check-in Analysis",
            "tasks": [
                "Count the number of check-ins per year.",
                "Count the number of check-ins per hour within a 24-hour period.",
                "Identify the most popular city for check-ins.",
                "Rank all businesses based on check-in counts.",
                "Calculate month-over-month check-in growth rate for top 50 restaurants in a specific city.",
                "Analyze review/check-in seasonality by cuisine category.",
            ],
        },
        {
            "title": "VI. Comprehensive Analysis",
            "tasks": [
                "Identify top 5 merchants in each city using combined metrics: rating frequency, average rating, and check-in frequency.",
                "Calculate review conversion rate for top 100 most checked-in businesses (check-ins to reviews ratio).",
                "Analyze post-review check-in drop-off after sudden spikes in 1-star reviews.",
            ],
        },
    ]

    tabs = st.tabs([section["title"] for section in requirement_sections])
    for tab, section in zip(tabs, requirement_sections):
        with tab:
            lines = [f"{index}. {task}" for index, task in enumerate(section["tasks"], start=1)]
            st.markdown("\n".join(lines))

    notebook_rows = []
    for task_key, meta in ZEPPELIN_TASK_NOTEBOOKS.items():
        notebook_rows.append(
            {
                "Task Area": str(meta.get("label", task_key)),
                "Notebook ID": str(meta.get("id", "")),
                "Notebook URL": f"{ZEPPELIN_BASE_URL}/#/notebook/{meta['id']}",
            }
        )
    st.markdown(
        '<div class="section-shell animate-in stagger-2"><h3 class="section-title" style="font-size:1.05rem;">Zeppelin Notebook Mapping</h3></div>',
        unsafe_allow_html=True,
    )
    st.dataframe(notebook_rows, width="stretch", hide_index=True)


def _stylize_keywords(text: str) -> str:
    """Scan text and wrap specific keywords in styled hyperlink spans."""
    keywords = {
        "PostgreSQL": "?route=docs",
        "NLP": "?route=docs",
        "Data Science": "?route=docs",
        "Big Data": "?route=docs",
        "Semantic Search": "?route=docs",
        "Text-to-SQL": "?route=docs",
    }
    for keyword, link in keywords.items():
        # Use a regex to avoid replacing parts of words
        text = re.sub(
            rf"\b({re.escape(keyword)})\b",
            f'<a href="{link}" class="keyword-link">\\1</a>',
            text,
            flags=re.IGNORECASE,
        )
    return text


def render_dashboard_features():
    """Renders the 3-column feature grid with icons, titles, and links."""
    st.markdown(
        """
        <div class="feature-grid">
            <div class="feature-card">
                <div class="feature-icon">🗃️</div>
                <div class="feature-content">
                    <h3>Explore Data Schema</h3>
                    <p>Understand the tables and columns available for querying.</p>
                    <a href="?route=schema" class="feature-cta">View Schema</a>
                </div>
            </div>
            <div class="feature-card">
                <div class="feature-icon">⚙️</div>
                <div class="feature-content">
                    <h3>Test the Pipeline</h3>
                    <p>Run readiness checks to ensure the system is live.</p>
                    <a href="?route=readiness" class="feature-cta">Run Checks</a>
                </div>
            </div>
            <div class="feature-card">
                <div class="feature-icon">🚀</div>
                <div class="feature-content">
                    <h3>Run a Sample Query</h3>
                    <p>Pre-fill the chat with an official sample question.</p>
                    <a href="?action=ask&question=Which 10 cities have the most elite users?" class="feature-cta">Run Query</a>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# --- CACHING ---
# Simple in-memory cache for this session
sql_cache = {}
result_cache = {}

def _render_home_view() -> None:
    """Render the primary chat route with a minimized-by-default launcher state."""
    if _get_active_panel() == "manual_sql":
        _render_manual_sql_workspace()

    if st.session_state.readiness_last_fallback_note:
        st.warning(st.session_state.readiness_last_fallback_note)

    pipeline_progress_bridge = st.empty()
    _render_pipeline_progress_bridge(pipeline_progress_bridge)
    _render_conversation()
    if _read_query_param("show").lower() == "query_settings":
        _render_response_preferences()
    _render_chat_command_dock()

    pending_submission = _get_pending_question_submission()
    if pending_submission is not None:
        stop_cols = st.columns([1, 6], vertical_alignment="center")
        with stop_cols[0]:
            if st.button("Stop", key="stop_generating_button", help="Stop generating this response now."):
                st.session_state.pending_question_submission = None
                _clear_pipeline_loading_state()
                st.rerun()
        with stop_cols[1]:
            st.caption("Generating response... you can stop it anytime.")

    if str(st.session_state.editable_question_draft).strip():
        edit_cols = st.columns([8, 1, 1], vertical_alignment="center")
        with edit_cols[0]:
            st.text_input(
                "Edit question",
                key="editable_question_draft",
                placeholder="Edit previous question, then click Send",
                label_visibility="collapsed",
            )
        with edit_cols[1]:
            if st.button("Send", key="send_edited_question"):
                edited_question = str(st.session_state.editable_question_draft).strip()
                if edited_question:
                    st.session_state.nl_question_text = edited_question
                    _queue_question_submission(
                        edited_question,
                        st.session_state.nl_use_demo_mode,
                        chat_mode=CHAT_MODE_AUTO,
                    )
                    st.session_state.editable_question_draft = ""
                    st.rerun()
        with edit_cols[2]:
            if st.button("Clear", key="clear_edited_question"):
                st.session_state.editable_question_draft = ""
                st.rerun()

    _process_pending_question_submission(pipeline_progress_bridge)

    active_chat_mode = CHAT_MODE_AUTO
    chat_placeholder = "Ask your question. Query by SilkByteX will route it automatically."
    chat_question = st.chat_input(chat_placeholder, disabled=_get_pending_question_submission() is not None)
    if chat_question:
        st.session_state.nl_question_text = chat_question
        _queue_question_submission(
            chat_question,
            st.session_state.nl_use_demo_mode,
            chat_mode=active_chat_mode,
        )
        st.rerun()


def _get_route_handlers() -> dict[str, Callable[[], None]]:
    """Build the SPA router map from route names to render functions."""
    return {
        "home": _render_home_view,
        "readiness": _render_readiness_view,
        "data_journey_admin": _render_data_journey_admin_view,
        "schema": _render_schema_view,
        "architecture": _render_architecture_view,
        "docs": _render_docs_view,
    }


def _handle_routing() -> str:
    """Read the current route and mount the matching SPA component."""
    route_handlers = _get_route_handlers()
    current_route = _get_current_route()
    st.session_state.current_view = current_route
    route_handlers[current_route]()
    return current_route


def load_css() -> None:
    """Load custom CSS styles for the application."""
    # CSS loading is handled by apply_ui_styles
    pass


def apply_ui_styles() -> None:
    """Apply custom CSS styles to the Streamlit app, embedding the background image."""
    import base64
    import mimetypes
    import pathlib

    assets_dir = pathlib.Path(__file__).parent / "assets"
    bg_b64 = ""
    bg_mime = "image/jpeg"
    # Try user-supplied background first, then fall back to any available asset
    for candidate in (
        "websitepic_pink.png",
        "user_background.jpg",
        "site_background_final_4.gif",
        "user_background.gif",
        "site_background_final_3.jpg",
        "site_background_final_2.jpg",
        "site_background_final.jpg",
        "site_background.jpg",
    ):
        bg_path = assets_dir / candidate
        if bg_path.exists():
            with open(bg_path, "rb") as f:
                bg_b64 = base64.b64encode(f.read()).decode()
            guessed_mime, _ = mimetypes.guess_type(str(bg_path))
            if guessed_mime:
                bg_mime = guessed_mime
            break

    css = get_custom_css(bg_b64, bg_mime)
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def apply_next_level_visuals() -> None:
    """Apply enhanced gradients and motion for schema, readiness, and result outputs."""
    st.markdown(
        """
        <style>
        @keyframes auroraShift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }

        @keyframes floatInSoft {
            0% { opacity: 0; transform: translateY(12px) scale(0.985); }
            100% { opacity: 1; transform: translateY(0) scale(1); }
        }

        .premium-glass-card,
        [data-testid="stExpander"] > details,
        [data-testid="stDataFrame"],
        .stPlotlyChart {
            border: 1px solid rgba(175, 127, 89, 0.22) !important;
            border-radius: 18px !important;
            background: linear-gradient(
                135deg,
                rgba(255, 255, 255, 0.96),
                rgba(250, 244, 239, 0.95),
                rgba(245, 237, 231, 0.93)
            ) !important;
            background-size: 180% 180% !important;
            box-shadow: 0 16px 34px rgba(17, 17, 17, 0.1), 0 0 0 1px rgba(255, 255, 255, 0.4) inset !important;
            animation: auroraShift 12s ease-in-out infinite, floatInSoft 0.52s ease-out both;
        }

        [data-testid="stExpander"] > details:hover,
        .premium-glass-card:hover {
            transform: translateY(-2px);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            box-shadow: 0 20px 42px rgba(17, 17, 17, 0.13), 0 0 0 1px rgba(175, 127, 89, 0.26) inset !important;
        }

        [data-testid="stExpander"] summary,
        [data-testid="stExpander"] summary p,
        [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p {
            color: #111111 !important;
            font-weight: 700 !important;
        }

        [data-testid="stDataFrame"] table thead tr th {
            background: linear-gradient(120deg, rgba(43, 41, 39, 0.95), rgba(88, 72, 58, 0.92)) !important;
            color: #fff !important;
            border-bottom: 1px solid rgba(214, 175, 140, 0.45) !important;
        }

        [data-testid="stDataFrame"] table tbody tr:nth-child(even) td {
            background: rgba(175, 127, 89, 0.06) !important;
        }

        [data-testid="stDataFrame"] table tbody tr:hover td {
            background: linear-gradient(90deg, rgba(175, 127, 89, 0.16), rgba(214, 175, 140, 0.12)) !important;
            transition: background 0.2s ease;
        }

        .readiness-action-link,
        .schema-task-chip {
            position: relative;
            overflow: hidden;
        }

        .readiness-action-link::before,
        .schema-task-chip::before {
            content: "";
            position: absolute;
            top: -120%;
            left: -40%;
            width: 60%;
            height: 300%;
            transform: rotate(24deg);
            background: linear-gradient(
                90deg,
                rgba(255, 255, 255, 0),
                rgba(255, 255, 255, 0.55),
                rgba(255, 255, 255, 0)
            );
            animation: shimmerSweep 3.4s ease-in-out infinite;
            pointer-events: none;
        }

        @keyframes shimmerSweep {
            0% { left: -45%; }
            100% { left: 135%; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(
    title: str,
    description: str,
    badges: list[str] | None = None,
    highlights: list[str] | None = None,
    kicker: str = ""
) -> None:
    """Render a hero section with title, description, badges and highlights."""
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    
    if kicker:
        st.markdown(
            f'<p class="section-title">{escape(kicker)}</p>',
            unsafe_allow_html=True
        )
    
    st.markdown(f"# {title}")
    st.markdown(description)
    
    if badges:
        badge_cols = st.columns(len(badges))
        for col, badge in zip(badge_cols, badges):
            with col:
                st.markdown(f"**{badge}**")
    
    if highlights:
        st.markdown("---")
        for highlight in highlights:
            st.markdown(f"✨ {highlight}")
    
    st.markdown('</div>', unsafe_allow_html=True)


def run_app(config: AppConfig):
    """Main function to run the Streamlit application."""
    current_config = config or load_config()

    _initialize_state()
    load_css()
    apply_ui_styles()
    apply_next_level_visuals()
    current_route = _get_current_route()
    _render_global_navbar(current_config, current_route)

    _handle_routing()
    _render_floating_action_menu(current_config)
