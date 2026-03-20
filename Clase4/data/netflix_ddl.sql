-- Netflix Dataset DDL for Databricks
-- This script creates the table structure for Netflix movies and TV shows data

-- Drop table if exists (for re-runs)
DROP TABLE IF EXISTS netflix_titles;

-- Create the netflix_titles table
CREATE TABLE netflix_titles (
    show_id STRING COMMENT 'Unique identifier for each title',
    type STRING COMMENT 'Type of content (Movie or TV Show)',
    title STRING COMMENT 'Title of the movie or TV show',
    director STRING COMMENT 'Director of the movie/show',
    cast_field STRING COMMENT 'Main actors/actresses in the content',
    country STRING COMMENT 'Country where the movie/show was produced',
    date_added DATE COMMENT 'Date when the content was added to Netflix',
    release_year INT COMMENT 'Year when the movie/show was released',
    rating STRING COMMENT 'Content rating (TV-MA, PG-13, etc.)',
    duration STRING COMMENT 'Duration in minutes for movies, seasons for TV shows',
    listed_in STRING COMMENT 'Genres/categories the content belongs to',
    description STRING COMMENT 'Brief description of the content'
)
USING DELTA
COMMENT 'Netflix movies and TV shows dataset for data analysis and processing examples'
LOCATION '/tmp/netflix_titles';

-- Create some useful indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_netflix_type ON netflix_titles (type);
CREATE INDEX IF NOT EXISTS idx_netflix_release_year ON netflix_titles (release_year);
CREATE INDEX IF NOT EXISTS idx_netflix_country ON netflix_titles (country);

-- Insert sample data
INSERT INTO netflix_titles VALUES
('s1', 'Movie', 'Dick Johnson Is Dead', 'Kirsten Johnson', '', 'United States', CAST('2021-09-25' AS DATE), 2020, 'PG-13', '90 min', 'Documentaries', 'As her father nears the end of his life, filmmaker Kirsten Johnson stages his death in inventive ways to help them both face the inevitable.'),
('s2', 'TV Show', 'Blood & Water', '', 'Ama Qamata, Khosi Ngema, Gail Mabalane', 'South Africa', CAST('2021-09-24' AS DATE), 2021, 'TV-MA', '2 Seasons', 'TV Dramas, TV Mysteries', 'After crossing paths at a private school in Cape Town, a teen tries to secure a scholarship at the school her sister attends.'),
('s3', 'TV Show', 'Ganglands', '', 'Sami Bouajila, Tracy Gotoas, Samuel Jouy', 'France', CAST('2021-09-24' AS DATE), 2021, 'TV-MA', '1 Season', 'Crime TV Shows, Action & Adventure', 'A drug dealer attempts to pull one final deal to get out of the business for good.'),
('s4', 'TV Show', 'Jailbirds New Orleans', '', '', 'United States', CAST('2021-09-24' AS DATE), 2021, 'TV-MA', '1 Season', 'Docuseries, Reality TV', 'Featuring unprecedented access inside the Orleans Justice Center, this series follows inmates as they navigate the system.'),
('s5', 'TV Show', 'Kota Factory', '', 'Mayur More, Jitendra Kumar, Ranjan Raj', 'India', CAST('2021-09-24' AS DATE), 2021, 'TV-14', '2 Seasons', 'International TV Shows, TV Comedies', 'In a city of coaching centers known to train India\'s finest collegiate minds, an earnest but unexceptional student and his friends navigate campus life.'),
('s6', 'Movie', 'Midnight Mass', 'Mike Flanagan', 'Kate Siegel, Zach Gilford, Hamish Linklater', 'United States', CAST('2021-09-24' AS DATE), 2021, 'TV-MA', '7 h 4 min', 'Horror Movies, Thrillers', 'The arrival of a charismatic young priest brings glorious miracles, ominous mysteries, and renewed religious fervor to a dying island community.'),
('s7', 'Movie', 'My Little Pony: A New Generation', '', '', 'United States', CAST('2021-09-24' AS DATE), 2021, 'PG', '91 min', 'Children & Family Movies, Music & Musicals', 'After the time of the Mane 6, Sunny and her new friends must unite to bring magic back to Equestria.'),
('s8', 'Movie', 'Sankofa', 'Haile Gerima', 'Kofi Ghanaba, Oyafunmike Ogunlano', 'Ghana, United States, Burkina Faso', CAST('2021-09-24' AS DATE), 1993, 'R', '125 min', 'Dramas, International Movies', 'An African-American woman travels back in time to a plantation on a quest to understand her roots.'),
('s9', 'Movie', 'The Starling', 'Theodore Melfi', 'Melissa McCarthy, Chris O\'Dowd', 'United States', CAST('2021-09-24' AS DATE), 2021, 'PG-13', '104 min', 'Comedies, Dramas', 'A married couple suffers a hardship, leading Jack to head off to deal with his grief while Lilly remains in the "real" world.'),
('s10', 'TV Show', 'Vendetta: Truth, Lies and The Mafia', '', '', 'Italy', CAST('2021-09-24' AS DATE), 2019, 'TV-MA', '1 Season', 'Docuseries, Crime TV Shows', 'The story of a Sicilian family fighting the Mafia from the inside.'),
('s11', 'Movie', 'Dhoom', 'Sanjay Gadhvi', 'Abhishek Bachchan, Uday Chopra', 'India', CAST('2021-09-23' AS DATE), 2004, 'PG-13', '129 min', 'Action & Adventure, International Movies', 'A mysterious thief steals valuable artifacts and a Mumbai police officer teams up with his partner to catch him.'),
('s12', 'Movie', 'The Disciple', 'Chaitanya Tamhane', 'Aditya Modak, Arun Dravid', 'India', CAST('2021-09-23' AS DATE), 2020, 'PG-13', '127 min', 'Dramas, International Movies', 'A classical singer struggles with his art and his personal life while pursuing his passion for Indian classical music.'),
('s13', 'Movie', 'The Social Dilemma', 'Jeff Orlowski', 'Tristan Harris, Jeff Seibert, Bailey Richardson', 'United States', CAST('2021-09-23' AS DATE), 2020, 'PG-13', '94 min', 'Documentaries, Social & Cultural Docs', 'Tech experts sound the alarm on the dangerous impact of social networking, which Big Tech uses to extract data for profit.'),
('s14', 'TV Show', 'The White Tiger', 'Ramin Bahrani', 'Adarsh Gourav, Rajkummar Rao, Priyanka Chopra', 'India, United States', CAST('2021-09-23' AS DATE), 2021, 'R', '125 min', 'Dramas, International Movies', 'An ambitious Indian driver uses his wit and cunning to escape from poverty and rise to the top.'),
('s15', 'Movie', 'Chicago Party Aunt', '', '', 'United States', CAST('2021-09-17' AS DATE), 2021, 'TV-14', '1 Season', 'Animation, Comedies', 'An alcoholic aunt is forced to raise her nephew after his parents abandon him.'),
('s16', 'Movie', 'The Crown', 'Peter Morgan', 'Claire Foy, Matt Smith, Olivia Colman', 'United Kingdom', CAST('2016-11-04' AS DATE), 2016, 'TV-MA', '4 Seasons', 'TV Dramas, Historical TV Shows', 'Follows the political rivalries and romance of Queen Elizabeth II\'s reign and the events that shaped the second half of the 20th century.'),
('s17', 'Movie', 'Stranger Things', 'Duffer Brothers', 'Millie Bobby Brown, Finn Wolfhard', 'United States', CAST('2016-07-15' AS DATE), 2016, 'TV-14', '4 Seasons', 'Sci-Fi TV, Teen TV Shows', 'When a young boy disappears, his mother, a police chief, and his friends must confront terrifying forces in order to get him back.'),
('s18', 'Movie', 'The Queen\'s Gambit', 'Scott Frank', 'Anya Taylor-Joy, Bill Camp', 'United States', CAST('2020-10-23' AS DATE), 2020, 'TV-MA', '1 Season', 'TV Dramas, Limited Series', 'Orphaned Beth Harmon discovers and masters the game of chess in 1950s Kentucky.'),
('s19', 'Movie', 'Bridgerton', 'Chris Van Dusen', 'Phoebe Dynevor, Regé-Jean Page', 'United States', CAST('2020-12-25' AS DATE), 2020, 'TV-MA', '2 Seasons', 'TV Dramas, Romance TV Shows', 'Wealthy Regency-era English families vie for social standing and marriages in this period drama.'),
('s20', 'Movie', 'Money Heist', 'Álex Pina', 'Úrsula Corberó, Álvaro Morte', 'Spain', CAST('2017-12-20' AS DATE), 2017, 'TV-MA', '5 Seasons', 'Crime TV Shows, International TV Shows', 'A criminal mastermind and his team of thieves execute heists on the Royal Mint of Spain.');

-- Verify data insertion
SELECT COUNT(*) as total_records FROM netflix_titles;

-- Show sample data
SELECT * FROM netflix_titles LIMIT 5;
