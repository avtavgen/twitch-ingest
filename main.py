import os
import sys
from social.entity import SocialStatements
from helpers.logger import get_logger
from skafossdk import *
from twitch.twitch_processor import TwitchProcessor


# Initialize the skafos sdk
ska = Skafos()

ingest_log = get_logger('user-fetch')

if __name__ == "__main__":
    ingest_log.info('Starting job')
    if 'CLIENT_ID' in os.environ:
        api_key = os.environ['CLIENT_ID']
    else:
        ingest_log.info('Setting up api key')
        api_key = 'rzyu2ma6hklfiw6i1yjvq0oiqhop0v'
        # sys.exit('Please save a Twitch api key in your environment. ')

    if 'VIEW_COUNT' in os.environ:
        cnt = int(os.environ['VIEW_COUNT'])
    else:
        cnt = 50

    ingest_log.info('Fetching user data')
    entity = SocialStatements(ingest_log, ska.engine) # , ska.engine
    processor = TwitchProcessor(api_key, ingest_log, entity, min_viewer_count=cnt).fetch()
