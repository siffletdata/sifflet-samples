from typing import Literal, List, Union
from typing_extensions import NotRequired, TypedDict


from .parameters_field import ParametersFieldDict


class TagFieldDict(TypedDict):
    id: NotRequired[str]  # (NotRequired) ID of the tag
    name: NotRequired[str]  # (NotRequired) Name of the tag
    kind: NotRequired[Literal["Tag", "Classification"]]  # (NotRequired) Type of tag


class TermFieldDict(TypedDict):
    id: NotRequired[str]  # (NotRequired) ID of the term
    name: NotRequired[str]  # (NotRequired) Name of the term


class IncidentFieldDict(TypedDict):
    severity: Literal["Low", "Moderate", "High", "Critical"]  # (REQUIRED)
    message: NotRequired[
        str
    ]  # (NotRequired) message for the incident and notifications


class NotificationsFieldDict(TypedDict):
    kind: Literal["Slack", "Email", "MicrosoftTeams"]  # (REQUIRED)
    id: NotRequired[str]  # (NotRequired) ID of the notification
    name: NotRequired[str]  # (NotRequired) Name of the kind selected


class MonitorsSharedFieldsDict(TypedDict):
    """
    Monitor parameters that are shared between DQAC and Collection
    file structures.
    """

    version: Union[str, int]  # (REQUIRED) Version of the monitor
    name: str  # (NotRequired) Name of the monitor
    kind: Literal["Monitor"]  # (REQUIRED)
    description: NotRequired[str]  # (NotRequired) Description for the monitor
    tags: NotRequired[List[TagFieldDict]]
    terms: NotRequired[List[TermFieldDict]]
    schedule: NotRequired[str]  # (NotRequired) Schedule for the monitor
    incident: NotRequired[IncidentFieldDict]  # (REQUIRED)
    notifications: NotRequired[List[NotificationsFieldDict]]  # (NotRequired)
    parameters: ParametersFieldDict
