"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path

from urllib.parse import parse_qsl
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import time

from singer_sdk import metrics

from tap_klaviyo.client import KlaviyoStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class MetricAggregatesStream(KlaviyoStream):
    """Set up to aggregate the last three months of performance"""

    name = "campaignmetricaggregates"
    path = "/metric-aggregates"
    primary_keys = ["CampaignName", "Date"]
    replication_key = ""
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    # Give a default aggregate to "count" and aggregate_by of "Campaign Name". Change
    # this if you want a stream to use "sum_value" or "unique" instead, or if you want
    # to aggregate by "attributed_flow"
    aggregate = "count"
    aggregate_by = ["Campaign Name"]

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        now = datetime.now()
        # Twelve months is as far back as you can go for the metric-aggregates endpoint
        last_twelve_months = datetime.now() - relativedelta(months=12)
        start_date = last_twelve_months.strftime("%Y-%m-%dT%H:%M:%S")
        end_date = now.strftime("%Y-%m-%dT%H:%M:%S")

        return {
            "data": {
                "type": "metric-aggregate",
                "attributes": {
                    "metric_id": self.metric_id,
                    "interval": "day",
                    # TO-DO: Figure out how to paginate so we don't need a huge page size
                    "page_size": 10000,
                    "timezone": "UTC",
                    "by": self.aggregate_by,
                    "measurements": [f"{self.aggregate}"],
                    "filter": [
                        f"greater-or-equal(datetime,{start_date})",
                        f"less-than(datetime,{end_date})",
                    ],
                },
            }
        }


class ReceivedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "receivedemailsbycampaign"
    metric_id = "H4DrTy"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "openedemailsbycampaign"
    metric_id = "P4W93C"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate = "unique"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ClickedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "clickedemailsbycampaign"
    metric_id = "MYayva"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate = "unique"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class BouncedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "bouncedemailsbycampaign"
    metric_id = "Ld4b2k"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class UnsubscribesByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "unsubscribesbycampaign"
    metric_id = "QqUYDV"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class PlacedOrdersByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "placedordersbycampaign"
    metric_id = "T7RgqW"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class CheckoutAmountsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "checkoutamountsbycampaign"
    metric_id = "T7RgqW"
    schema_filepath = SCHEMAS_DIR / "metricaggregatessumbycampaign.json"
    aggregate = "sum_value"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                sum_values = item["measurements"]["sum_value"]

                for date, sum_value in zip(
                    record["data"]["attributes"]["dates"], sum_values
                ):
                    result = {
                        "Date": date,
                        "CampaignName": dimensions,
                        "Sum": sum_value,
                    }
                    if sum_value == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ReceivedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "receivedemailsbyflowmessage"
    metric_id = "H4DrTy"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "openedemailsbyflowmessage"
    metric_id = "P4W93C"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ClickedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "clickedemailsbyflowmessage"
    metric_id = "MYayva"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class BouncedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "bouncedemailsbyflowmessage"
    metric_id = "Ld4b2k"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class UnsubscribesByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "unsubscribesbyflowmessage"
    metric_id = "QqUYDV"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class PlacedOrdersByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "placedordersbyflowmessage"
    metric_id = "T7RgqW"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class CheckoutAmountsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "checkoutamountsbyflowmessage"
    metric_id = "T7RgqW"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatessumbyflowmessage.json"
    aggregate = "sum_value"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                sum_values = item["measurements"]["sum_value"]

                for date, sum_value in zip(
                    record["data"]["attributes"]["dates"], sum_values
                ):
                    result = {
                        "Date": date,
                        "FlowMessage": flow_message,
                        "Sum": sum_value,
                    }
                    if sum_value == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenEventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "openevents"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "events.json"

    UTC = timezone.utc
    DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def _isodate_from_date_string(date_string: str) -> str:
        """Convert a date string to an ISO date string.

        Args:
            date_string: The date string to convert.

        Returns:
            An ISO date string.
        """
        return (
            datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()
        )

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            else:
                filter_timestamp = datetime.now() - relativedelta(days=5)

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["fields[profile]"] = "email"
            params["include"] = "profile"

            params[
                "filter"
            ] = f'equals(metric_id,"P4W93C"),greater-than({self.replication_key},{filter_timestamp})'
            self.logger.info(f"params are: {params}")
        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
            },
        ]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"
    max_page_size = 100

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "metrics.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    schema_filepath = SCHEMAS_DIR / "listperson.json"
    max_page_size = 1000

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["list_id"] = context["list_id"]
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "flows.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["flow_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class FlowActionsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flowactions"
    path = "/flows/{flow_id}/flow-actions"
    primary_keys = ["id"]
    replication_key = "updated"
    parent_stream_type = FlowsStream
    schema_filepath = SCHEMAS_DIR / "flowactions.json"
    max_page_size = 50

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["flow_action_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["flow_id"] = context["flow_id"]
        row["updated"] = row["attributes"]["updated"]
        time.sleep(1)
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class FlowMessagesStream(KlaviyoStream):
    """Define custom stream."""

    name = "flowmessages"
    path = "/flow-actions/{flow_action_id}/flow-messages"
    primary_keys = ["id"]
    replication_key = "updated"
    parent_stream_type = FlowActionsStream
    schema_filepath = SCHEMAS_DIR / "flowmessages.json"
    max_page_size = 50

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["flow_id"] = context["flow_id"]
        row["flow_action_id"] = context["flow_action_id"]
        row["updated"] = row["attributes"]["updated"]
        time.sleep(1)
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True
