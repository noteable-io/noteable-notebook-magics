from functools import wraps

import click
from IPython.core.error import UsageError as IPythonUsageError


def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s


class NtblError(Exception):
    def _render_traceback_(self):
        return ["Contact support."]


def catch_em_all(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except click.UsageError:
            raise IPythonUsageError("See above and correct your command.") from None
        except:  # noqa
            raise NtblError() from None

    return wrapped
