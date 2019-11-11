# NewsTrace

NewsTrace is a platform for merging stream of tweets with trustworthy news sources. It is a project finished in 4 weeks during the Insight Data Science program, where I am a Data Engineering Fellow.

## Motivation
Mobile phone users are reading news form social platforms like Facebook and Twitter. However, survey shows that ~57% of these users are expecting the news to be "Largely Inaccurate".

![Image of Survey](https://www.journalism.org/wp-content/uploads/sites/8/2018/09/PJ_2018.09.10_social-media-news_0-01.png)

As a result, my goals for this project are:

- Find a Trustworthy collection of news sources for referencing
- Enable Cross-platform searches
- Relate social media posts like Twitter with detailed info on the source
## Solution
I used the following data sources:
-  [GDELT 2.0](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/) Events and Mentions database as a trustable news collection.
-  [Twitter Irish News](https://figshare.com/articles/Insight4news_Irish_news_related_tweet_collection_15_07_2015-24_05_2017/7932422) As a news-focused collection of tweets for stream simulation.

Firstly, every URL link appeared in tweets are extracted for querying. The GDELT events are then processed to generate useful information like keywords, relevant sources & timespan, during which the EID is used to relate extra info from the mentions table to the main stream of events. Finally, the processed collection are indexed and ready for query in DB.

![Image of Tech stack](https://i.ibb.co/ZTXSB2G/temp.jpg)

- Spark: Apache Spark is used for 2 tasks:
  - `filter_events`: Extract URL and Event ID from raw data and re-arrange time-windowed tables
  - `filter_mentions` : Generate details of events into tables with similiar windowing. Prepare data for faster joining and indexing.
- Kafka: Kafka is also used for two tasks:
  - Ingest simulated stream of tweets
  - Hold enriched tweets (original post + info on source) returned by consumer/producer, to be consumed by Flask webapp

- MySQL: Store two collections of table during spark processing, and another collection table for querying

## Links
- [Project Demo](http://datatoday.net)
- [Presentation Slides](https://docs.google.com/presentation/d/14LDfiF6E8naFyJpE6SXMYH83CldSOSlHg7fYMyfk4Tg/edit?usp=sharing)
