# yelp_text_to_sql/golden_queries.py
GOLDEN_QUERIES = [
    # =================================================================
    # I. Business Analysis
    # =================================================================
    {
        "keywords": ["20 most common merchants", "u.s.", "us"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT name, COUNT(*) as merchant_count 
            FROM business 
            WHERE state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
            GROUP BY name 
            ORDER BY merchant_count DESC 
            LIMIT 20;
        """
    },
    {
        "keywords": ["top 10 cities", "most merchants", "u.s."],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT city, COUNT(*) AS merchant_count
            FROM business
            WHERE state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
            GROUP BY city
            ORDER BY merchant_count DESC
            LIMIT 10;
        """
    },
    {
        "keywords": ["top 5 states", "most merchants"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT state, COUNT(*) AS merchant_count
            FROM business
            GROUP BY state
            ORDER BY merchant_count DESC
            LIMIT 5;
        """
    },
    {
        "keywords": ["20 most common merchants", "average ratings"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT b.name, COUNT(b.business_id) as merchant_count, AVG(r.stars) as average_rating
            FROM business b JOIN rating r ON b.business_id = r.business_id
            WHERE b.state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
            GROUP BY b.name
            ORDER BY merchant_count DESC
            LIMIT 20;
        """
    },
    {
        "keywords": ["count", "different categories"],
        "category": "Business Analysis",
        "exact_sql": "SELECT COUNT(DISTINCT category) FROM (SELECT explode(categories) as category FROM business);"
    },
    {
        "keywords": ["top 10 most frequent categories"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT category, COUNT(*) as category_count
            FROM (SELECT explode(categories) as category FROM business)
            GROUP BY category
            ORDER BY category_count DESC
            LIMIT 10;
        """
    },
    {
        "keywords": ["top 20 merchants", "most five-star reviews"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT b.name, COUNT(r.review_id) AS five_star_reviews
            FROM business b
            JOIN rating r ON b.business_id = r.business_id
            WHERE r.stars = 5
            GROUP BY b.name
            ORDER BY five_star_reviews DESC
            LIMIT 20;
        """
    },
    {
        "keywords": ["count", "restaurant types", "chinese", "american", "mexican"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT 
                SUM(CASE WHEN array_contains(categories, 'Chinese') THEN 1 ELSE 0 END) as chinese_restaurants,
                SUM(CASE WHEN array_contains(categories, 'American') THEN 1 ELSE 0 END) as american_restaurants,
                SUM(CASE WHEN array_contains(categories, 'Mexican') THEN 1 ELSE 0 END) as mexican_restaurants
            FROM business WHERE array_contains(categories, 'Restaurants');
        """
    },
    {
        "keywords": ["count of reviews", "restaurant type", "chinese", "american", "mexican"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT 
                SUM(CASE WHEN array_contains(b.categories, 'Chinese') THEN 1 ELSE 0 END) as chinese_reviews,
                SUM(CASE WHEN array_contains(b.categories, 'American') THEN 1 ELSE 0 END) as american_reviews,
                SUM(CASE WHEN array_contains(b.categories, 'Mexican') THEN 1 ELSE 0 END) as mexican_reviews
            FROM business b JOIN rating r ON b.business_id = r.business_id
            WHERE array_contains(b.categories, 'Restaurants');
        """
    },
    {
        "keywords": ["rating distribution", "restaurant types", "chinese", "american", "mexican"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT 
                r.stars,
                SUM(CASE WHEN array_contains(b.categories, 'Chinese') THEN 1 ELSE 0 END) as chinese_rating_dist,
                SUM(CASE WHEN array_contains(b.categories, 'American') THEN 1 ELSE 0 END) as american_rating_dist,
                SUM(CASE WHEN array_contains(b.categories, 'Mexican') THEN 1 ELSE 0 END) as mexican_rating_dist
            FROM business b JOIN rating r ON b.business_id = r.business_id
            WHERE array_contains(b.categories, 'Restaurants')
            GROUP BY r.stars
            ORDER BY r.stars;
        """
    },
    {
        "keywords": ["turnaround merchants", "rating increased"],
        "category": "Business Analysis",
        "exact_sql": """
            WITH historical_avg AS (
                SELECT business_id, AVG(stars) as avg_hist_rating
                FROM rating
                WHERE date < date_sub(now(), 365)
                GROUP BY business_id
            ),
            recent_avg AS (
                SELECT business_id, AVG(stars) as avg_recent_rating
                FROM rating
                WHERE date >= date_sub(now(), 365)
                GROUP BY business_id
            )
            SELECT b.name, hist.avg_hist_rating, rec.avg_recent_rating
            FROM business b
            JOIN historical_avg hist ON b.business_id = hist.business_id
            JOIN recent_avg rec ON b.business_id = rec.business_id
            WHERE rec.avg_recent_rating >= hist.avg_hist_rating + 1.0
            ORDER BY (rec.avg_recent_rating - hist.avg_hist_rating) DESC;
        """
    },
    {
        "keywords": ["category synergy", "pairs of distinct business categories"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT cat1, cat2, COUNT(*) as co_occurrences
            FROM (
                SELECT t1.category AS cat1, t2.category AS cat2
                FROM (SELECT business_id, explode(categories) AS category FROM business) t1
                JOIN (SELECT business_id, explode(categories) AS category FROM business) t2 ON t1.business_id = t2.business_id
                WHERE t1.category < t2.category
            )
            GROUP BY cat1, cat2
            ORDER BY co_occurrences DESC
            LIMIT 10;
        """
    },
    {
        "keywords": ["polarizing businesses", "standard deviation", "love with 5 stars"],
        "category": "Business Analysis",
        "exact_sql": """
            SELECT b.name, stddev(r.stars) as rating_stddev
            FROM rating r JOIN business b ON r.business_id = b.business_id
            GROUP BY b.business_id, b.name
            HAVING count(r.stars) > 50
            ORDER BY rating_stddev DESC
            LIMIT 20;
        """
    },
    # =================================================================
    # II. User Analysis
    # =================================================================
    {
        "keywords": ["number of users joining each year"],
        "category": "User Analysis",
        "exact_sql": """
            SELECT year(yelping_since) as join_year, COUNT(user_id) as num_users
            FROM users
            GROUP BY join_year
            ORDER BY join_year;
        """
    },
    {
        "keywords": ["top reviewers", "review_count"],
        "category": "User Analysis",
        "exact_sql": """
            SELECT name, review_count
            FROM users
            ORDER BY review_count DESC
            LIMIT 20;
        """
    },
    {
        "keywords": ["most popular users", "fans"],
        "category": "User Analysis",
        "exact_sql": """
            SELECT name, fans
            FROM users
            ORDER BY fans DESC
            LIMIT 20;
        """
    },
    {
        "keywords": ["ratio of elite users to regular users"],
        "category": "User Analysis",
        "exact_sql": """
            SELECT
                join_year,
                SUM(is_elite) / COUNT(user_id) as elite_ratio
            FROM (
                SELECT
                    year(yelping_since) as join_year,
                    user_id,
                    CASE WHEN size(elite) > 0 THEN 1 ELSE 0 END as is_elite
                FROM users
            )
            GROUP BY join_year
            ORDER BY join_year;
        """
    },
    {
        "keywords": ["proportion of total users and silent users"],
        "category": "User Analysis",
        "exact_sql": """
            SELECT
                u_join.join_year,
                COUNT(u_join.user_id) as total_users,
                SUM(CASE WHEN r.user_id IS NULL THEN 1 ELSE 0 END) as silent_users
            FROM (SELECT user_id, year(yelping_since) as join_year FROM users) u_join
            LEFT JOIN (SELECT DISTINCT user_id FROM rating) r ON u_join.user_id = r.user_id
            GROUP BY u_join.join_year
            ORDER BY u_join.join_year;
        """
    },
    # =================================================================
    # III. Review Analysis
    # =================================================================
    {
        "keywords": ["number of reviews per year"],
        "category": "Review Analysis",
        "exact_sql": """
            SELECT year(date) as review_year, COUNT(review_id) as num_reviews
            FROM rating
            GROUP BY review_year
            ORDER BY review_year;
        """
    },
    {
        "keywords": ["count", "useful", "funny", "cool", "reviews"],
        "category": "Review Analysis",
        "exact_sql": """
            SELECT 
                SUM(useful) as total_useful,
                SUM(funny) as total_funny,
                SUM(cool) as total_cool
            FROM rating;
        """
    },
    {
        "keywords": ["correlation between review length and rating"],
        "category": "Review Analysis",
        "exact_sql": """
            SELECT 
                stars,
                AVG(length(text)) as avg_review_length
            FROM rating
            GROUP BY stars
            ORDER BY stars;
        """
    },
    # =================================================================
    # IV. Rating Analysis
    # =================================================================
    {
        "keywords": ["distribution of ratings", "1-5 stars"],
        "category": "Rating Analysis",
        "exact_sql": """
            SELECT stars, COUNT(review_id) as rating_count
            FROM rating
            GROUP BY stars
            ORDER BY stars;
        """
    },
    {
        "keywords": ["top businesses with most five-star ratings"],
        "category": "Rating Analysis",
        "exact_sql": """
            SELECT b.name, COUNT(r.review_id) AS five_star_ratings
            FROM business b
            JOIN rating r ON b.business_id = r.business_id
            WHERE r.stars = 5
            GROUP BY b.name
            ORDER BY five_star_ratings DESC
            LIMIT 20;
        """
    },
    {
        "keywords": ["top 10 cities with highest ratings"],
        "category": "Rating Analysis",
        "exact_sql": """
            SELECT b.city, AVG(r.stars) as avg_rating
            FROM business b
            JOIN rating r ON b.business_id = r.business_id
            GROUP BY b.city
            ORDER BY avg_rating DESC
            LIMIT 10;
        """
    },
    # =================================================================
    # V. Check-in Analysis
    # =================================================================
    {
        "keywords": ["number of check-ins per year"],
        "category": "Check-in Analysis",
        "exact_sql": """
            SELECT year(date) as checkin_year, COUNT(*) as num_checkins
            FROM checkin
            GROUP BY checkin_year
            ORDER BY checkin_year;
        """
    },
    {
        "keywords": ["most popular city for check-ins"],
        "category": "Check-in Analysis",
        "exact_sql": """
            SELECT b.city, COUNT(c.business_id) as checkin_count
            FROM business b
            JOIN checkin c ON b.business_id = c.business_id
            GROUP BY b.city
            ORDER BY checkin_count DESC
            LIMIT 1;
        """
    },
    {
        "keywords": ["rank all businesses based on check-in counts"],
        "category": "Check-in Analysis",
        "exact_sql": """
            SELECT b.name, COUNT(c.business_id) as checkin_count
            FROM business b
            JOIN checkin c ON b.business_id = c.business_id
            GROUP BY b.name
            ORDER BY checkin_count DESC;
        """
    }
]
