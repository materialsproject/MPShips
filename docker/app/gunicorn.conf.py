"""gunicorn settings shared by MP frontend apps (used via `start.sh`: `gunicorn -c gunicorn.conf.py`).

Settings that vary per deployment stay as command-line flags in `start.sh`
(bind, workers, max requests, ...); command-line flags override this file.
"""

from mp_ui.core.logging import gunicorn_logconfig_dict

# gunicorn's own error and access logs go through the same structured logging
# config as the application (see `mp_ui.core.logging`). Applied in the master
# before forking, so it doesn't import the application.
logconfig_dict = gunicorn_logconfig_dict()

# Log each request through `gunicorn.access`; the formatter maps gunicorn's
# atoms to structured fields, so the format string itself is only a fallback.
accesslog = "-"
access_log_format = '%(h)s %(m)s %(U)s %(q)s %(s)s %(B)s %(D)s "%(f)s" "%(a)s"'
