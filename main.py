import os
import sys
from social.entity import SocialStatements
from helpers.logger import get_logger
from skafossdk import *
from twitch.twitch_processor import TwitchProcessor


def get_twitch():
    # test skafos sdk
    if 'CLIENT_ID' in os.environ:
        api_key = os.environ['CLIENT_ID']
    else:
        api_key = 'rzyu2ma6hklfiw6i1yjvq0oiqhop0v'
        # sys.exit('Please save a Twitch api key in your environment.')

    if 'VIEW_COUNT' in os.environ:
        cnt = int(os.environ['VIEW_COUNT'])
    else:
        cnt = 500

    # Initialize the skafos sdk
    ska = Skafos()

    ingest_log = get_logger('user-fetch')
    processor = TwitchProcessor(api_key, ingest_log, min_viewer_count=cnt).fetch()
    ti = SocialStatements(processor.info)
    ti.save(ska)


if __name__ == "__main__":
    get_twitch()
