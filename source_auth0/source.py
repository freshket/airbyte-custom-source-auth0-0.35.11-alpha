#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import parse

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from source_auth0.utils import get_api_endpoint, initialize_authenticator


class Users(HttpStream):
    api_version = "v2"
    page_size = 50
    current_page = 0

    primary_key = "user_id"

    def __init__(self, url_base: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_endpoint = get_api_endpoint(url_base, self.api_version)

    def path(self, **kwargs) -> str:
        return "users"

    @property
    def url_base(self) -> str:
        return self.api_endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:

        # NOTE: [limitation of auth0]
        # current page start at 0 and we can't fetch over 1,000 records
        if (self.current_page + 1) * self.page_size >= 1000:
            return None

        self.current_page = self.current_page + 1

        return {
            "page": self.current_page,
            "per_page": self.page_size,
        }

    def request_params(
        self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {
            "page": 0,
            "per_page": self.page_size,
            "include_totals": "false",
            **(next_page_token or {}),
        }

        print("[DEBUG][Users] params", params)

        return params

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        records = response.json()

        print("[DEBUG][Users] records", records)

        output_records = []

        for record in response.json():
            output_records.append({
                "id": record.get("user_id"),
                "email": record.get("email"),
                "name": record.get("name"),
                "profile_image_url": record.get("picture"),
            })

        return output_records


class CommercialUsers(HttpStream):
    api_version = "v2"
    page_size = 50
    role_ids = []
    current_role_index = 0
    current_page = 0
    available_ids: dict = {}

    primary_key = "user_id"

    def __init__(self, url_base: str, role_ids: list, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_endpoint = get_api_endpoint(url_base, self.api_version)
        self.role_ids = role_ids

    def path(self, **kwargs) -> str:
        role_id = self.role_ids[self.current_role_index]
        path = f"roles/{role_id}/users"

        print("[DEBUG][CommercialUsers] path", path)

        return path

    @property
    def url_base(self) -> str:
        return self.api_endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:

        # NOTE: [limitation of auth0]
        # current page start at 0 and we can't fetch over 1,000 records
        if (self.current_page + 1) * self.page_size >= 1000:
            return None

        body = response.json()

        if len(body) == 0:
            role_ids_len = len(self.role_ids)

            if role_ids_len > 0 and self.current_role_index < role_ids_len - 1:
                self.current_role_index = self.current_role_index + 1
                self.current_page = 0

                return {
                    "page": self.current_page,
                    "per_page": self.page_size,
                }

            return None

        self.current_page = self.current_page + 1

        return {
            "page": self.current_page,
            "per_page": self.page_size,
        }

    def request_params(
        self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {
            "page": 0,
            "per_page": self.page_size,
            "include_totals": "false",
            **(next_page_token or {}),
        }

        print("[DEBUG][CommercialUsers] params", params)

        return params

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        records = response.json()

        print("[DEBUG][CommercialUsers] records", records)

        output_records = []

        for record in response.json():
            id = record.get(self.primary_key)

            if id in self.available_ids:
                continue

            self.available_ids[id] = True
            output_records.append({
                "id": record.get("user_id"),
                "email": record.get("email"),
                "name": record.get("name"),
                "profile_image_url": record.get("picture"),
            })

        return output_records


# Source
class SourceAuth0(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        try:
            auth = initialize_authenticator(config)
            api_endpoint = get_api_endpoint(config.get("base_url"), "v2")
            url = parse.urljoin(api_endpoint, "users")
            response = requests.get(
                url,
                params={"per_page": 1},
                headers=auth.get_auth_header(),
            )

            if response.status_code == requests.codes.ok:
                return True, None

            return False, response.json()
        except Exception:
            return False, "Failed to authenticate with the provided credentials"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Users(**{
                "authenticator": initialize_authenticator(config),
                "url_base": config.get("base_url"),
            }),
            CommercialUsers(**{
                "authenticator": initialize_authenticator(config),
                "url_base": config.get("base_url"),
                "role_ids": config.get("commercial_role_ids"),
            }),
        ]
