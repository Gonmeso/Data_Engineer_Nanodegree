# Data Modeling With Apache Cassandra

A Data Engineering project focused on learning the basics of Data Modeling using cassandras as the main storage tool, performing ETL's using python.

## Getting Started

The first step is to clone the repository `git clone https://github.com/Gonmeso/Data_Engineer_Nanodegree.git` and then cd into the project `cd 02_data_modeling_with_apache_cassandra`

### Prerequisites

To make this project work you will need the following tools:

```
Docker
Docker-Compose
```

### Folder structure

The project is structured as follows

```
├── README.md
├── cassandra
│   ├── Readme.md
│   ├── data
├── dev_env
│   ├── Dockerfile
│   ├── Project_1B_Project_Template.ipynb
│   ├── event_data
│   │   ├── 2018-11-01-events.csv
│   │   ├── 2018-11-02-events.csv
│   │   ├── ...
│   │   ├── ...
│   │   └── 2018-11-30-events.csv
│   ├── event_datafile_new.csv
│   └── requirements.txt
└── docker-compose.yml
```

### Running the project

To run the project the only thing you need to to is execute `docker-compose up` and the next steps will be performed:

1. Environment anc cassandra will be up
2. Go to localhost:8888 to opne jupyterlab
3. Open `Project_1B_ Project_Template.ipynb`
4. Select `Run`-> `Run all cells`
5. By executing all cells the `event_datafile_new.csv` will be generated
6. The queries will be executed


### Queries results

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

**Query**

```sql
SELECT artist, song, length FROM music_library
WHERE sessionId = 338 AND itemInSession = 4;
```

**Result**
```
0. Row ->  Artist: Faithless, Song: Music Matters (Mark Knight Dub), Length: 495.30731201171875
``` 

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

**Query**

```sql
SELECT artist, song, firstName, lastName FROM user_library
WHERE userId = 10 AND sessionId = 182;
```

**Result**
```
0. Row ->  Artist: Down To The Bone, Song: Keep On Keepin' On, User: Sylvie Cruz
1. Row ->  Artist: Three Drives, Song: Greece 2000, User: Sylvie Cruz
2. Row ->  Artist: Sebastien Tellier, Song: Kilometer, User: Sylvie Cruz
3. Row ->  Artist: Lonnie Gordon, Song: Catch You Baby (Steve Pitron & Max Sanna Radio Edit), User: Sylvie Cruz
``` 

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

**Query**

```sql
SELECT firstName, lastName FROM songs_per_user
WHERE song = 'All Hands Against His Own';
```

**Result**
```
0. Row -> User: Jacqueline Lynch
1. Row -> User: Sara Johnson
2. Row -> User: Tegan Levine
``` 
## Authors

* **Gonzalo Mellizo-Soto Díaz**

## Acknowledgments

* Thanks to Udacity for the project!