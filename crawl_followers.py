import time
import Queue
import datetime
import threading
import logging
import sys
import pymongo


from twython import (
    Twython,
    TwythonRateLimitError,
    TwythonError,
    TwythonAuthError
)

from credentials import keys
from collections import namedtuple

# DEBUG
# import pdb

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] \
            (%(threadName)-8s) %(funcName)20s() %(message)s')


class FollowersCountdown():
    def __init__(self, num_followers):
        self._num_followers = num_followers
        self._lock = threading.Lock()

    def decrement_counter(self):
        with self._lock:
            if self._num_followers > 0:
                self._num_followers -= 1
            logging.debug("Followers left to crawl {}".format(
                self._num_followers))

    def finished(self):
        return not bool(self._num_followers)


class ErrorHandler():
    def __init__(self):
        self.retry = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if isinstance(value, TwythonRateLimitError):
            self.retry = True

            logging.debug('Retry after {}'.format(value.retry_after))
            t = datetime.datetime.fromtimestamp(float(value.retry_after))
            now = datetime.datetime.now()

            if now < t:
                dt = t - now
                logging.error(
                    'Rate limit exceeded. Sleep {} seconds'.format(dt.seconds))
                time.sleep(dt.seconds)

        elif isinstance(value, TwythonAuthError):
            logging.debug('Authentication Error')
            self.retry = False
        elif isinstance(value, TwythonError):
            if value.error_code == 404:
                logging.debug('Http 404. No need to retry')
                self.retry = False
            else:
                # wait for a bit and retry
                self.retry = True
                logging.error('Unexpected Error. Sleep {} seconds'.format(10))
                time.sleep(10)

        return True


class FollowersFetcher(threading.Thread):
    def __init__(self, queue, tw, screen_name):
        threading.Thread.__init__(self)
        self._queue = queue
        self._tw = tw
        self._screen_name = screen_name

    def run(self):
        self._enqueue_followers()

    def _enqueue_followers(self):
        logging.info('Filling queue of followers')

        for followers_chunk in self._get_followers():
            for follower_id in followers_chunk:
                logging.debug("Adding user_id:{} to queue".format(follower_id))
                self._queue.put(follower_id)

        logging.info('All followers already in queue')

    def _get_followers(self):
        cursor = -1

        while cursor != 0:
            with ErrorHandler():
                response = self._tw.get_followers_ids(
                    screen_name=self._screen_name,
                    cursor=cursor)
                cursor = response['next_cursor']
                yield response['ids']


class TweetsFetcher(threading.Thread):
    def __init__(self, queue, tw, followers_countdown, mongo_config):
        threading.Thread.__init__(self)
        self._queue = queue
        self._tw = tw
        self._countdown = followers_countdown
        self._mongo_conn = pymongo.MongoClient()
        self._db = mongo_config.db
        self._collection = mongo_config.collection

    def run(self):
        while True:
            try:
                user_id = self._queue.get(False)
                logging.info('Fetching tweets from user_id {}'.format(user_id))

                for tweet in self._get_user_tweets(user_id):
                    self._mongo_conn[self._db][self._collection].insert(tweet)

                self._countdown.decrement_counter()
            except Queue.Empty:
                logging.debug('Followers Queue is empty')
                if self._countdown.finished():
                    break
                else:
                    time.sleep(1)

    def _get_user_tweets(self, user_id):
        while True:
            timeline = []

            with ErrorHandler() as e:
                timeline = self._tw.get_user_timeline(id=user_id)

            if e.retry:
                continue

            break

        return (tweet for tweet in timeline)


def build_twitter_conn(app_key, app_secret):
    twitter = Twython(app_key, app_secret, oauth_version=2)
    ACCESS_TOKEN = twitter.obtain_access_token()
    twitter = Twython(app_key, access_token=ACCESS_TOKEN)
    return twitter


def main():
    """
    Usage: python crawl_followers.py [SCREEN_NAME] [DATABASE] [COLLECTION]

    SCREEN_NAME: Twitter account
    DATABASE: MongoDb Database
    COLLECTION: MongoDb Collection

    """
    screen_name, db, collection = sys.argv[1:]
    MongoConfig = namedtuple('MongoConfig', ['db', 'collection'])
    mongo_config = MongoConfig(db=db, collection=collection)

    # 1. Get the amount of followers
    app_key, app_secret = keys[0]
    twitter = build_twitter_conn(app_key, app_secret)

    user_info = twitter.show_user(screen_name=screen_name)
    followers_count = user_info['followers_count']
    logging.debug('Number of followers: %d' % followers_count)

    # 2. Start filling the Queue with followers to crawl
    q = Queue.Queue()

    ff = FollowersFetcher(q, twitter, screen_name)
    ff.start()

    countdown = FollowersCountdown(followers_count)

    # 3. Start worker threads to fetch followers tweets
    for i in range(len(keys)):
        app_key, app_secret = keys[i]
        twitter = build_twitter_conn(app_key, app_secret)

        tf = TweetsFetcher(q, twitter, countdown, mongo_config)
        tf.start()


if __name__ == '__main__':
    main()
