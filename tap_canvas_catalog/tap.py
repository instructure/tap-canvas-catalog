"""CanvasCatalog tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_canvas_catalog import streams


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
    ).to_dict()

    def discover_streams(self) -> list[streams.CanvasCatalogStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapCanvasCatalog.cli()
