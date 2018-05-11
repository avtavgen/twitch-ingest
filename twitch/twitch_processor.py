import requests
import sys
from time import sleep
from random import randint


class TwitchProcessor(object):
    def __init__(self, api_key, log, retry=3, min_viewer_count=150000, first=20):
        self.log = log
        self.retry = retry
        self.min_viewer_count = min_viewer_count
        self.first = first
        self.cursor = None
        self.base_url = "https://api.twitch.tv"
        self.headers = {'Client-ID': api_key, 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def _make_request(self, url, cursor=None):
        if cursor:
            url += "&after={}".format(cursor)
        retries = 0
        while retries <= self.retry:
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                self.log.info("{}".format(e))
                break
            except Exception as e:
                self.log.info("{}: Failed to make Twitch request on try {}".format(e, retries))
                retries += 1
                if retries <= self.retry:
                    self.log.info("Trying again!")
                    continue
                else:
                    sys.exit("Max retries reached")

    def _get_user_ids(self):
        self.user_ids = []
        while True:
            url = "{}/helix/streams?first={}".format(self.base_url, self.first)
            response = self._make_request(url, self.cursor)
            self.cursor = response["pagination"]["cursor"]
            for stream in response["data"]:
                self.viewer_count = stream["viewer_count"]
                self.user_ids.append(stream["user_id"])
            if self.viewer_count < self.min_viewer_count:
                self.log.info("Collected: {}".format(self.user_ids))
                break

    def _make_user_info_request(self, user_id):
        url = "{}/helix/users?id={}".format(self.base_url, user_id)
        return self._make_request(url)

    def _get_user_follows(self, user_id):
        url = "{}/helix/users/follows?to_id={}".format(self.base_url, user_id)
        return self._make_request(url)

    def _make_user_vod_request(self, login):
        url = "{}/kraken/channels/{}/videos".format(self.base_url, login)
        return self._make_request(url)

    def _make_user_broadcasts_request(self, login):
        url = "{}/kraken/channels/{}/videos?broadcasts=true".format(self.base_url, login)
        return self._make_request(url)

    def _get_user_videos(self, login):
        vod = self._make_user_vod_request(login)
        broadcasts = self._make_user_broadcasts_request(login)
        return vod["_total"] + broadcasts["_total"]

    def _get_user_info(self):
        user_data = dict()
        for user_id in self.user_ids:
            user_info = self._make_user_info_request(user_id)
            try:
                user = user_info["data"][0]
            except:
                self.log.info("Failed to fetch user info. ID: {}".format(user_id))
                continue
            user_follows = self._get_user_follows(user_id)
            user_videos = self._get_user_videos(user["login"])
            user_data["user_id"] = user_id
            user_data["name"] = user["display_name"]
            user_data["views"] = user["view_count"]
            user_data["description"] = user["description"]
            user_data["url"] = "{}/{}".format("https://www.twitch.tv", user["login"])
            user_data["followers"] = user_follows["total"]
            user_data["videos"] = user_videos
            self.log.info(user_data)
            self.info.append(user_data)
            sleep(randint(4,8))

    def fetch(self):
        self.log.info('Making request to Twitch for daily streams export')
        self.info = []
        self._get_user_ids()
        self._get_user_info()
        return self
