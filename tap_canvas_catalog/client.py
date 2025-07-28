"""REST client handling, including CanvasCatalogStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
import backoff

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

def fetch_catalog_source_schema(config: dict) -> list[dict]:
    """Fetch stream schema definitions from Hotglue."""
    api_url = config.get("api_url")
    api_key = config.get("api_key")

    if not api_url or not api_key:
        raise ValueError("Missing 'api_url' or 'api_key' in config")

    url = api_url.rstrip("/") + "/integrations/hotglue/source-schema"
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

class CanvasCatalogStream(RESTStream):
    """CanvasCatalog stream class."""

    # OR use a dynamic url_base:
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        api_url = self.config.get("api_url", "")
        if not api_url:
            raise ValueError("api_url is not set in the configuration")
        return api_url

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        api_key = self.config.get("api_key", "")
        if not api_key:
            raise ValueError("api_key is not set in the configuration")

        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f'Token token="{api_key}"',
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        next_page_token = previous_token or 1

        all_matches = list(extract_jsonpath(
                self.records_jsonpath, response.json()
        ))

        if len(all_matches) > 0:
            return next_page_token + 1

        return None

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        return params

    def backoff_wait_generator(self):
        return backoff.expo(base=2, factor=5)

    def backoff_max_tries(self) -> int:
        return 7

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # if self.replication_key:
        #     replication_date = self.get_starting_timestamp(context)
        #     if replication_date:
        #         updated_at = dateutil.parser.parse(row.get(self.replication_key))

        #         if updated_at > replication_date:
        #             return row

        return row
