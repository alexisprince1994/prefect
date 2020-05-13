import datetime

from prefect_server.database.orm import HasuraModel, UUIDString


class FlowConcurrencyLimit(HasuraModel):
    __hasura_type__ = "flow_concurrency_limit"

    id: UUIDString = None
    created: datetime.datetime = None
    updated: datetime.datetime = None
    name: str = None
    description: str = None
    limit: int = None
