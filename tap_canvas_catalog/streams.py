"""Stream type classes for tap-canvas-catalog."""

from __future__ import annotations

from typing import Any

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_canvas_catalog.client import CanvasCatalogStream

TYPE_MAP = {
    "bigint": th.IntegerType,
    "character varying": th.StringType,
    "timestamp without time zone": th.DateTimeType,
    "json": th.CustomType({"type": ["object", "string", "array"]}),
    "integer": th.IntegerType,
    "boolean": th.BooleanType,
    "numeric": th.NumberType,
    "text": th.StringType,
    "jsonb": th.CustomType({"type": ["object", "string", "array"]}),
}

def create_stream_class(stream_name, properties):
    schema_props = []
    for prop in properties:
        th_type = TYPE_MAP.get(prop["property_type"].lower(), th.StringType)
        schema_props.append(th.Property(prop["property_name"], th_type))
    source_schema = th.PropertiesList(*schema_props).to_dict()

    class DynamicStream(CanvasCatalogStream):
        name = stream_name
        path = f"/integrations/hotglue/sources/{stream_name}"
        primary_keys = ["id"]
        records_jsonpath = f"$.{stream_name}[*]"
        replication_key = "updated_at"
        schema = source_schema

        def get_url_params(
            self,
            context: dict | None,
            next_page_token: Any | None,
        ) -> dict[str, Any]:
            params = super().get_url_params(context, next_page_token)
            params["account_id"] = self.config.get("account_id")

            include_soft_deleted = self.config.get("include_soft_deleted")
            if include_soft_deleted is not None:
                params["include_soft_deleted"] = include_soft_deleted

            # log params
            self.logger.info(f"get_url_params: {params}")

            return params

    return DynamicStream
