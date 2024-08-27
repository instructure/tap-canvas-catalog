"""Stream type classes for tap-canvas-catalog."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_canvas_catalog.client import CanvasCatalogStream

class UsersStream(CanvasCatalogStream):
    name = "users"
    path = "/integrations/hotglue/sources/users"
    primary_keys = ["id"]
    records_jsonpath = "$.users[*]"
    replication_key = "updated_at"

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["accountId"] = 1

        # log params
        self.logger.info(f"get_url_params: {params}")

        return params

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
        ),
        th.Property(
            "root_account_id",
            th.IntegerType,
        ),
        th.Property(
            "canvas_user_id",
            th.IntegerType,
        ),
        th.Property(
            "canvas_root_account_uuid",
            th.StringType,
        ),
        th.Property(
            "user_name",
            th.StringType,
        ),
        th.Property(
            "first_name",
            th.StringType,
        ),
        th.Property(
            "last_name",
            th.StringType,
        ),
        th.Property(
            "email_address",
            th.StringType,
        ),
        th.Property(
            "custom_fields",
            th.CustomType({"type": ["object", "string"]}),
        ),
        th.Property(
            "created_at",
            th.DateTimeType,
        ),
        th.Property(
            "updated_at",
            th.DateTimeType,
        ),
        th.Property(
            "time_zone",
            th.DateTimeType,
        ),
    ).to_dict()
