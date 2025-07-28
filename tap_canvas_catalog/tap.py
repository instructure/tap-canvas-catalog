"""CanvasCatalog tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_canvas_catalog.client import fetch_catalog_source_schema
from tap_canvas_catalog.streams import create_stream_class

class TapCanvasCatalog(Tap):
    """CanvasCatalog tap class."""

    name = "tap-canvas-catalog"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            secret=False,
            description="The URL for the Canvas Catalog API",
        ),
        th.Property(
            "account_id",
            th.NumberType,
            required=False,
            secret=False,
            description="The sub-account id to pull data from",
        ),
        th.Property(
            "include_soft_deleted",
            th.BooleanType,
            required=False,
            secret=False,
            description="Flag for including soft-deleted records",
        ),
    ).to_dict()

    def discover_streams(self) -> list:
        schemas = fetch_catalog_source_schema(self.config)

        streams = []
        for schema in schemas:
            stream_name = schema["stream_name"]
            properties = schema["properties"]
            dynamic_stream_class = create_stream_class(stream_name, properties)
            streams.append(dynamic_stream_class(self))
        return streams


if __name__ == "__main__":
    TapCanvasCatalog.cli()
