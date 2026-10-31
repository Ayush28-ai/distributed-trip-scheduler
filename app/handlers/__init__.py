from app.handlers.base import JobHandler
from app.handlers.pickup_alert import PickupAlertHandler
from app.handlers.simple import (BillingHandler, FlakyHandler, NoopHandler, NoShowHandler,
                                 RosterHandler)

JOB_TYPES = ("pickup_alert", "roster", "billing", "no_show", "noop", "flaky")


def build_handlers(notification_url: str) -> dict[str, JobHandler]:
    handlers: list[JobHandler] = [
        PickupAlertHandler(notification_url), RosterHandler(), BillingHandler(),
        NoShowHandler(), NoopHandler(), FlakyHandler(),
    ]
    return {h.job_type: h for h in handlers}
