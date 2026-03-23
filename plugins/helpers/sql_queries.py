class SqlQueries:
    create_tables = [
        """
        DROP TABLE IF EXISTS staging_events;
        """,
        """
        CREATE TABLE staging_events (
            artist VARCHAR(65535),
            auth VARCHAR(256),
            firstname VARCHAR(256),
            gender VARCHAR(1),
            iteminsession INTEGER,
            lastname VARCHAR(256),
            length FLOAT,
            level VARCHAR(64),
            location VARCHAR(65535),
            method VARCHAR(32),
            page VARCHAR(256),
            registration FLOAT,
            sessionid INTEGER,
            song VARCHAR(65535),
            status INTEGER,
            ts BIGINT,
            useragent VARCHAR(65535),
            userid INTEGER
        );
        """,
        """
        DROP TABLE IF EXISTS staging_songs;
        """,
        """
        CREATE TABLE staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR(256),
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR(65535),
            artist_name VARCHAR(65535),
            song_id VARCHAR(256),
            title VARCHAR(65535),
            duration FLOAT,
            year INTEGER
        );
        """,
        """
        DROP TABLE IF EXISTS songplays;
        """,
        """
        CREATE TABLE songplays (
            songplay_id VARCHAR(32) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            userid INTEGER,
            level VARCHAR(64),
            song_id VARCHAR(256),
            artist_id VARCHAR(256),
            sessionid INTEGER,
            location VARCHAR(65535),
            useragent VARCHAR(65535)
        );
        """,
        """
        DROP TABLE IF EXISTS users;
        """,
        """
        CREATE TABLE users (
            userid INTEGER PRIMARY KEY,
            firstname VARCHAR(256),
            lastname VARCHAR(256),
            gender VARCHAR(1),
            level VARCHAR(64)
        );
        """,
        """
        DROP TABLE IF EXISTS songs;
        """,
        """
        CREATE TABLE songs (
            song_id VARCHAR(256) PRIMARY KEY,
            title VARCHAR(65535),
            artist_id VARCHAR(256),
            year INTEGER,
            duration FLOAT
        );
        """,
        """
        DROP TABLE IF EXISTS artists;
        """,
        """
        CREATE TABLE artists (
            artist_id VARCHAR(256) PRIMARY KEY,
            artist_name VARCHAR(65535),
            artist_location VARCHAR(65535),
            artist_latitude FLOAT,
            artist_longitude FLOAT
        );
        """,
        """
        DROP TABLE IF EXISTS time;
        """,
        """
        CREATE TABLE time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday INTEGER
        );
        """
    ]

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)