import math
import json
import concurrent.futures
import time

##### workaround for requests - start ####
##### Avoid using requests & numpy libraries because spark worker image only has base python and is read-only

import typing
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message

class Response(typing.NamedTuple):
    body: str
    headers: Message
    status: int
    error_count: int = 0

    def json(self) -> typing.Any:
        """
        Decode body's JSON.

        Returns:
            Pythonic representation of the JSON object
        """
        try:
            output = json.loads(self.body)
        except json.JSONDecodeError:
            output = {} 
        return output


def request(
    url: str,
    data: str = None,
    params: dict = None,
    headers: dict = None,
    method: str = "GET",
    data_as_json: bool = True,
    error_count: int = 0,
) -> Response:
    if not url.casefold().startswith("http"):
        raise urllib.error.URLError("Incorrect and possibly insecure protocol in url")
    method = method.upper()
    request_data = None
    headers = headers or {}
    data = data or {}
    params = params or {}
    headers = headers or {}
    encoded_params = None

    if params:
        encoded_params = urllib.parse.urlencode(params, doseq=True, safe="/")

    if (method == "GET") and (params) :
        params = {**params, **data}
        data = None
        url += "?" + encoded_params 

    if data_as_json:
        request_data = data.encode("utf-8")
        headers["Content-Type"] = "application/json; charset=UTF-8"

    if (encoded_params) and (method=="POST"):
        request_data = encoded_params.encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    httprequest = urllib.request.Request(
        url, data=request_data, headers=headers, method=method
    )

    try:
        with urllib.request.urlopen(httprequest) as httpresponse:
            response = Response(
                headers=httpresponse.headers,
                status=httpresponse.status,
                body=httpresponse.read().decode(
                    httpresponse.headers.get_content_charset("utf-8")
                ),
            )
    except urllib.error.HTTPError as e:
        response = Response(
            body=str(e.reason),
            headers=e.headers,
            status=e.code,
            error_count=error_count + 1,
        )

    return response

##### workaround for requests - end ####

class ImplyAuthenticator:

    def __init__(self, org_name=None, client_id=None, client_secret=None):
        self.ORG_NAME = org_name

        self.CLIENT_ID = client_id
        self.CLIENT_SECRET = client_secret

        self.access_token = None

        # Imply OAuth API config
        self.TOKEN_ENDPOINT = f"https://id.imply.io/auth/realms/{org_name}/protocol/openid-connect/token"


    def authenticate(self):
        params = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "grant_type": "client_credentials",
        }
        response = request(
          url=self.TOKEN_ENDPOINT,
          params = params,
          data_as_json = False,
          method = "POST")
        self.access_token = response.json()['access_token']


    def get_access_token(self):
        return self.access_token

    def get_headers(self):
        return {
            "Authorization": "Bearer {token}".format(token=self.access_token)
        }


class ImplyEventApi:

    def __init__(self, auth: ImplyAuthenticator, table_id: str = None):
        self.auth = auth
        self.TABLE_ID = table_id
        self.EVENTS_ENDPOINT = f"https://api.imply.io/v1/events/{table_id}"

    def push_list(self, messages: list, split_size=750000, threads=5) -> list:

        responses = []
        input_length = len(",".join(messages))
        num_splits = math.ceil(input_length/split_size)
        n = math.floor(len(messages)/num_splits) 
        splits = [messages[i:i + n] for i in range(0, len(messages), n)]
        payloads = []

        for i, split in enumerate(splits):
            payload = ""
            for message in split:
                payload += message
                payload += "\n"

            payloads.append(payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            responses = pool.map(self.push, payloads)

        return responses

    def push(self, payload: str) -> Response:
        start = int(time.time())
        response = request(url=self.EVENTS_ENDPOINT,
                           data=payload,
                           headers=self.auth.get_headers(),
                           method="POST"
                          )
        end = int(time.time())
        print(f"=============>>>>>>>>>> Request took {end-start} seconds to push {len(payload)} bytes")
        print(f"response:status:{response.status}\nheaders:{response.headers}\nbody:{response.json()}")
        return response

