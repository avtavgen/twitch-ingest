from datetime import datetime

import requests
import sys
from time import sleep
from random import randint


class TwitchProcessor(object):
    def __init__(self, api_key, log, entity, retry=3, min_viewer_count=1000, first=100):
        self.log = log
        self.entity = entity
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
                sleep(30)
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
        while True:
            self.user_ids = []
            url = "{}/helix/streams?first={}".format(self.base_url, self.first)
            response = self._make_request(url, self.cursor)
            self.cursor = response["pagination"]["cursor"]
            for stream in response["data"]:
                self.viewer_count = stream["viewer_count"]
                self.user_ids.append(stream["user_id"])
            self._get_user_info(self.user_ids)
            if self.viewer_count < self.min_viewer_count:
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
        try:
            vod_total = vod["_total"]
        except:
            vod_total = 0
        try:
            br_total = broadcasts["_total"]
        except:
            br_total = 0
        return vod_total + br_total

    def _get_user_info(self, user_ids):
        self.info = []
        for user_id in user_ids:
            user_data = dict()
            img_urls = []
            user_info = self._make_user_info_request(user_id)
            try:
                user = user_info["data"][0]
            except:
                self.log.info("Failed to fetch user info. ID: {}".format(user_id))
                continue
            user_follows = self._get_user_follows(user_id)
            user_videos = self._get_user_videos(user["login"])
            user_data["uri"] = "twitch␟user␟{}".format(user_id)
            user_data["ingested"] = False
            user_data["screen_name"] = user["display_name"]
            user_data["platform_income"] = user["view_count"]
            user_data["profile"] = user["description"]
            user_data["url"] = "{}/{}".format("https://twitch.tv", user["login"])
            user_data["followers"] = user_follows["total"] if user_follows else 0
            user_data["post_count"] = user_videos if user_videos else 0
            user_data["date"] = datetime.now().strftime("%Y-%m-%d")
            img_profile = user["profile_image_url"]
            img_urls.append(img_profile)
            img_offline = user["offline_image_url"]
            img_urls.append(img_offline)
            user_data["img_urls"] = img_urls
            self.log.info(user_data)
            self.info.append(user_data)
            sleep(randint(4,10))
        self.entity.save(users=self.info)

    def fetch(self):
        self.log.info('Making request to Twitch for daily streams export')
        self._get_user_ids()
        return self
