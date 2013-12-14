User guide
===========

Local tweet collection management
---------------------------------

It is common to store a collection of tweets in compressed files
grouped by hours. For example:

.. code-block:: bash

    $ tree ./tweets
    ./tweets
    ├── 2012-04-19-00.gz
    ├── 2012-04-20-00.gz
    ├── 2012-04-21-00.gz
    ├── 2012-04-21-08.gz
    ├── 2012-04-21-09.gz
    └── 2012-04-21-10.gz

Where each line in the files is the json representation of a
tweet. The first two tweets in my collection look like:

::

    $ zcat ./tweets/2012-04-19-00.gz | head -n 2
    {"text":"100 days until summer Olympics","id_str":"192764446173708291","coordinates":null,"created_at":"Thu Apr 19 00:00:00 +0000 2012","in_reply_to_status_id_str":null,"favorited":false,"source":"web","in_reply_to_user_id_str":null,"entities":{"urls":[],"user_mentions":[],"hashtags":[]},"contributors":null,"place":null,"in_reply_to_screen_name":null,"in_reply_to_status_id":null,"geo":null,"user":{"is_translator":false,"statuses_count":861,"time_zone":"Quito","profile_background_color":"db4c39","id_str":"395132292","follow_request_sent":null,"verified":false,"profile_background_tile":true,"created_at":"Fri Oct 21 05:40:09 +0000 2011","profile_sidebar_fill_color":"48dbaa","default_profile_image":false,"notifications":null,"friends_count":128,"url":null,"description":"","favourites_count":0,"profile_sidebar_border_color":"e2e83f","followers_count":114,"profile_image_url":"http:\/\/a0.twimg.com\/profile_images\/1807429969\/Spring_2012_009_WarmingFilter_1_normal.jpg","screen_name":"MEL0L407","profile_use_background_image":true,"profile_background_image_url_https":"https:\/\/si0.twimg.com\/profile_background_images\/500309685\/056.JPG","location":"Floridaa","contributors_enabled":false,"lang":"en","geo_enabled":false,"profile_text_color":"0a090a","protected":false,"profile_image_url_https":"https:\/\/si0.twimg.com\/profile_images\/1807429969\/Spring_2012_009_WarmingFilter_1_normal.jpg","listed_count":0,"profile_background_image_url":"http:\/\/a0.twimg.com\/profile_background_images\/500309685\/056.JPG","name":"Melissa Townsend","profile_link_color":"7a0c41","id":395132292,"default_profile":false,"show_all_inline_media":false,"following":null,"utc_offset":-18000},"retweeted":false,"id":192764446173708291,"retweet_count":0,"in_reply_to_user_id":null,"truncated":false}
    {"text":"Maeva et...? #ForeverAlone","id_str":"192764447666864129","coordinates":null,"created_at":"Thu Apr 19 00:00:00 +0000 2012","in_reply_to_status_id_str":null,"favorited":false,"source":"web","in_reply_to_user_id_str":null,"entities":{"urls":[],"user_mentions":[],"hashtags":[{"text":"ForeverAlone","indices":[13,26]}]},"contributors":null,"place":{"bounding_box":{"type":"Polygon","coordinates":[[[2.3894531,48.8832118],[2.4279991,48.8832118],[2.4279991,48.9180446],[2.3894531,48.9180446]]]},"place_type":"city","country":"France","url":"http:\/\/api.twitter.com\/1\/geo\/id\/35d2c646704fa4a1.json","country_code":"FR","attributes":{},"full_name":"Pantin, Seine-Saint-Denis","name":"Pantin","id":"35d2c646704fa4a1"},"in_reply_to_screen_name":null,"in_reply_to_status_id":null,"geo":null,"user":{"is_translator":false,"statuses_count":25433,"time_zone":"Paris","profile_background_color":"C0DEED","id_str":"379912464","follow_request_sent":null,"verified":false,"profile_background_tile":true,"created_at":"Sun Sep 25 19:26:25 +0000 2011","profile_sidebar_fill_color":"DDEEF6","default_profile_image":false,"notifications":null,"friends_count":179,"url":null,"description":"Tu m'as pas encore follow ? #RickRossSurToi !  \r\nMake people laugh, nigga that's my motto\r\n#TeamCuisseDodue #TeamSkinnyNigga","favourites_count":22,"profile_sidebar_border_color":"C0DEED","followers_count":236,"profile_image_url":"http:\/\/a0.twimg.com\/profile_images\/1839059455\/IMG-20120218-00089_normal.jpg","screen_name":"JulianSKEETER","profile_use_background_image":true,"profile_background_image_url_https":"https:\/\/si0.twimg.com\/profile_background_images\/528094149\/Women-Ruined-My-life-shirt.jpg","location":"Rack city","contributors_enabled":false,"lang":"fr","geo_enabled":true,"profile_text_color":"333333","protected":false,"profile_image_url_https":"https:\/\/si0.twimg.com\/profile_images\/1839059455\/IMG-20120218-00089_normal.jpg","listed_count":1,"profile_background_image_url":"http:\/\/a0.twimg.com\/profile_background_images\/528094149\/Women-Ruined-My-life-shirt.jpg","name":"Julian Freemann","profile_link_color":"0084B4","id":379912464,"default_profile":false,"show_all_inline_media":false,"following":null,"utc_offset":3600},"retweeted":false,"id":192764447666864129,"retweet_count":0,"in_reply_to_user_id":null,"truncated":false}

Showing the collection to humans
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``poultry show`` is a command which represents the tweets in the human
readable form:

::

    $ zcat ./tweets/2012-04-19-00.gz | head -n 2 | poultry show
    MEL0L407: 100 days until summer Olympics
    https://twitter.com/#!/MEL0L407/status/192764446173708291
    2012-04-19 00:00:00

    JulianSKEETER: Maeva et...? #ForeverAlone
    https://twitter.com/#!/JulianSKEETER/status/192764447666864129
    2012-04-19 00:00:00

in this case ``poultry`` has read the input from the standard input. It
also can read tweets from files in a directory. ``-i`` option
specifies which directory has to be processed by ``poultry``.

::

    $ poultry show -i ./tweets
    MEL0L407: 100 days until summer Olympics
    https://twitter.com/#!/MEL0L407/status/192764446173708291
    2012-04-19 00:00:00

    JulianSKEETER: Maeva et...? #ForeverAlone
    https://twitter.com/#!/JulianSKEETER/status/192764447666864129
    2012-04-19 00:00:00

    ... many other tweets from the files in tweets/ ...

Grouping the collection by time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes it is necessary to group a tweet collection to files by
tweet's creation time. ``poultry group`` groups the tweets (either from
the standard input or from the input directory to chunks which are written to files.

::

    $ poultry group -t 'by_day/%Y-%m-%d.gz' -i ./tweets
    by_day/2012-04-19.gz
    by_day/2012-04-20.gz
    by_day/2012-04-21.gz

``-t`` defines the template. The default template is
``%Y-%m-%d-%H.gz`` which groups the tweets by hour and stores them in
files in the current directory.

Filter the collection
---------------------

It is possible to filter the tweets of interest from the
collection. The tweets can be filtered by three predicates:

  * `follow
    <https://dev.twitter.com/docs/streaming-apis/parameters#follow>`_
    IDs of the users of interest. In the configuration file one ID per line is expected.
  * `track
    <https://dev.twitter.com/docs/streaming-apis/parameters#track>`_
    a list of phrases that a tweet should contain to be filtered. In
    the configuration file one phrase per line is expected.
  * `locations
    <https://dev.twitter.com/docs/streaming-apis/parameters#locations>`_
    a list of longitude, latitude pairs specifying a set of bounding
    boxes to filter tweets by.

An example configuration file ``./poultry.cfg``:

.. code-block:: ini

    # Filter only by one word `koninginnedag`.
    [filter:koninginnedag]
    split_template = ./koninginnedag-%Y-%m-%d.gz
    track = koninginnedag
    follow =
    locations =

    # Filter tweets with the phrase `reggae geel`, or
    # which are created by or mention the user with ID `303298444`
    [filter:reggaegeel]
    split_template = ./reggaegeel-%Y-%m-%d.gz
    track = reggae geel
    follow = 303298444
    locations =

    # It is possible to mention several phrases or users
    [filter:eurockeennes]
    split_template = ./eurockeennes-%Y-%m-%d.gz
    track = eurockeennes
            eurockéennes
    follow = 47100958
             538134842
    locations =

    # The Netherlands are defined as two rectangles.
    [filter:netherlands]
    split_template = ./netherlands-%Y-%m-%d.gz
    track =
    follow =
    locations = 3.734090,51.560411,5.667684,52.493220
                3.821980,51.934515,7.040975,53.687342

The predicates in the filter are ORed, meaning that a tweet to be
filtered has to satisfy at least one predicate.

The directories defined in the ``split_template`` have to exist.

To filter the collection run:

::

    $ bin/poultry filter -c ./poultry.cfg  -s ./tweets

Twitter Streaming API Stream capturing
======================================

.. warning:: The describing technique is not robust. For the streaming
             data collection you should use more advanced tools, for
             example ``fowler.stream``.

You can consume a stream of tweets using curl, `but you should not <https://dev.twitter.com/docs/streaming-api/concepts#Example>`_:

.. code-block:: bash

    curl https://stream.twitter.com/1/statuses/sample.json -uYOUR_TWITTER_USERNAME:YOUR_PASSWORD | fowler group
