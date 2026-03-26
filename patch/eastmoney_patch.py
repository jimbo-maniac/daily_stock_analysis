import hashlib
import random
import secrets
import threading
import time
import requests
import json
import uuid
import logging
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

original_request = requests.Session.request

ua = UserAgent()


class AuthCache:
    def __init__(self):
        self.data = None
        self.expire_at = 0
        self.lock = threading.Lock()
        self.ttl = 20


_cache = AuthCache()


class PatchSign:
    def __init__(self):
        self.patched = False

    def set_patch(self, patched):
        self.patched = patched

    def is_patched(self):
        return self.patched


_patch_sign = PatchSign()


def _get_nid(user_agent):
    """
    getEastmoney NID authorizationtoken

    Args:
        user_agent (str): userproxystring，formocknotsamebrowsehandleraccess

    Returns:
        str: returngetto NID authorizationtoken，iffetch failedthen return None

    featureDescription:
        thisfunctionviatoEastmoneyauthorizationAPI/interfacesendingrequestfromget NID token，
        foraftercontinuedataaccessauthorization。functionimplementcachemechanismfromavoidfrequentrequest。
    """
    now = time.time()
    # checkcachewhethervalid，avoid duplicaterequest
    if _cache.data and now < _cache.expire_at:
        return _cache.data
    # usethreadlockensureconcurrencysafe
    with _cache.lock:
        try:
            def generate_uuid_md5():
                """
                generating UUID andtoitsproceed MD5 hashprocessing
                :return: MD5 hash value（32digithexadecimalstring）
                """
                # generating UUID
                unique_id = str(uuid.uuid4())
                # to UUID proceed MD5 hash
                md5_hash = hashlib.md5(unique_id.encode('utf-8')).hexdigest()
                return md5_hash

            def generate_st_nvi():
                """
                generating st_nvi valuemethod
                :return: returngenerating st_nvi value
                """
                HASH_LENGTH = 4  # truncatehash valuebeforeseveraldigit

                def generate_random_string(length=21):
                    """
                    generatingspecifiedlengthrandomstring
                    :param length: stringlength，defaultas 21
                    :return: randomstring
                    """
                    charset = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
                    return ''.join(secrets.choice(charset) for _ in range(length))

                def sha256(input_str):
                    """
                    calculating SHA-256 hash value
                    :param input_str: inputstring
                    :return: hash value（hexadecimal）
                    """
                    return hashlib.sha256(input_str.encode('utf-8')).hexdigest()

                random_str = generate_random_string()
                hash_prefix = sha256(random_str)[:HASH_LENGTH]
                return random_str + hash_prefix

            url = "https://anonflow2.eastmoney.com/backend/api/webreport"
            # randomselectscreenminutedistinguishrate，increaserequestreal-ness
            screen_resolution = random.choice(['1920X1080', '2560X1440', '3840X2160'])
            payload = json.dumps({
                "osPlatform": "Windows",
                "sourceType": "WEB",
                "osversion": "Windows 10.0",
                "language": "zh-CN",
                "timezone": "Asia/Shanghai",
                "webDeviceInfo": {
                    "screenResolution": screen_resolution,
                    "userAgent": user_agent,
                    "canvasKey": generate_uuid_md5(),
                    "webglKey": generate_uuid_md5(),
                    "fontKey": generate_uuid_md5(),
                    "audioKey": generate_uuid_md5()
                }
            })
            headers = {
                'Cookie': f'st_nvi={generate_st_nvi()}',
                'Content-Type': 'application/json'
            }
            # increasetimeout，preventnolimitwaiting
            response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
            response.raise_for_status()  # to 4xx/5xx responseraise HTTPError

            data = response.json()
            nid = data['data']['nid']

            _cache.data = nid
            _cache.expire_at = now + _cache.ttl
            return nid
        except requests.exceptions.RequestException as e:
            logger.warning(f"requestEastmoneyauthorizationAPI/interfacefailed: {e}")
            _cache.data = None
            # thisAPI/interfacerequest failedwhen，planpossiblyalreadyinvalidate，aftercontinue largeprobabilitywillcontinuingfailed，becauseunable tosuccessfulget，belowtimeswillcontinuingrequest，settingsrelativelylongexpiration time，canavoidfrequentrequest
            _cache.expire_at = now + 5 * 60
            return None
        except (KeyError, json.JSONDecodeError) as e:
            logger.warning(f"parsingEastmoneyauthorizationAPI/interfaceresponse failed: {e}")
            _cache.data = None
            # thisAPI/interfacerequest failedwhen，planpossiblyalreadyinvalidate，aftercontinue largeprobabilitywillcontinuingfailed，becauseunable tosuccessfulget，belowtimeswillcontinuingrequest，settingsrelativelylongexpiration time，canavoidfrequentrequest
            _cache.expire_at = now + 5 * 60
            return None


def eastmoney_patch():
    if _patch_sign.is_patched():
        return

    def patched_request(self, method, url, **kwargs):
        # excludenon-targetdomainname
        is_target = any(
            d in (url or "")
            for d in [
                "fund.eastmoney.com",
                "push2.eastmoney.com",
                "push2his.eastmoney.com",
            ]
        )
        if not is_target:
            return original_request(self, method, url, **kwargs)
        # getonecountrandom User-Agent
        user_agent = ua.random
        # processing Headers：ensurenotdestroybusinesscodepass in headers
        headers = kwargs.get("headers", {})
        headers["User-Agent"] = user_agent
        nid = _get_nid(user_agent)
        if nid:
            headers["Cookie"] = f"nid18={nid}"
        kwargs["headers"] = headers
        # random sleep，decreaselowbyblockrisk
        sleep_time = random.uniform(1, 4)
        time.sleep(sleep_time)
        return original_request(self, method, url, **kwargs)

    # globalreplace Session  request entry
    requests.Session.request = patched_request
    _patch_sign.set_patch(True)
