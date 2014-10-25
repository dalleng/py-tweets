from twython import TwythonStreamer

import pymongo
import sys

APP_KEY = 'app-key'
APP_SECRET = 'app-secret'
OAUTH_TOKEN = 'oauth-token'
OAUTH_TOKEN_SECRET = 'oauth-toke-secret'


class MyStreamer(TwythonStreamer):

    def __init__(self, mongo_conn, *args):
        TwythonStreamer.__init__(self, *args)
        self.conn = mongo_conn
        self.alive = True

    def on_success(self, data):
        if not self.alive:
            self.disconnect()

        try:
            if data['place']['country_code'] == 'PY':
                self.conn.insert(data)
        except KeyError:
            pass

    def on_error(self, status_code, data):
        print 'HTTP Error: {}'.format(status_code)


def main():
    """
    Usage: python stream.py [DATABASE] [COLLECTION]

    DATABASE: MongoDb Database
    COLLECTION: MongoDb Collection

    """

    db, collection = sys.argv[1:]

    client = pymongo.MongoClient()
    conn = client[db][collection]

    stream = MyStreamer(conn, APP_KEY, APP_SECRET,
                        OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    bounding_box = '-62.892728,-27.507697,-54.312378,-19.275389'

    try:
        stream.statuses.filter(locations=bounding_box)
    except KeyboardInterrupt:
        stream.alive = False
        print 'Shutting Down'


if __name__ == '__main__':
    main()
