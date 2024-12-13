# The "Source" class enables the creation of a source object based on the connector name from HTTP trigger.
# This way we can publish code once, and use it for any connectors.

from sources.admitad import Admitad
from sources.esputnik import eSputnik
from sources.rtb_house import RTBHouse
from sources.asa import AppleSearchAds
from sources.currency_rates import CurrencyRates
from sources.appsflyer import AppsFlyer
from sources.google_play import GooglePlay
from sources.tiktok import TikTok
from sources.pazaruvaj import Pazaruvaj
from sources.planfix import Planfix
from sources.yandex_direct import YandexDirect
from sources.meta_ads import MetaAds
from sources.google_ads import GoogleAds
from sources.youtube_ads import YouTubeAds
from sources.google_analytics_4 import GoogleAnalytics4

class Source:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def connector(config):
        connector_classes = {
            'admitad': Admitad,
            'esputnik': eSputnik,
            'rtb_house': RTBHouse,
            'asa': AppleSearchAds,
            'currency': CurrencyRates,
            'appsflyer': AppsFlyer,
            'google_play': GooglePlay,
            'tiktok': TikTok,
            'pazaruvaj': Pazaruvaj,
            'planfix': Planfix,
            'yandex_direct': YandexDirect,
            'meta_ads': MetaAds,
            'google_ads': GoogleAds,
            'youtube_ads': YouTubeAds,
            'google_analytics_4': GoogleAnalytics4,
        }

        if config["connector"] not in connector_classes:
            raise ValueError(f"Unsupported API type: {config['connector']}")

        connector_class = connector_classes[config["connector"]]
        return connector_class(config)
