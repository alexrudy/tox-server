import logging
from typing import Any
from typing import Optional

import click

from .client import init_click_group as init_client_commands
from .server import serve

log = logging.getLogger(__name__)


class LogLevelParamType(click.ParamType):
    name = "log_level"

    def convert(self, value: str, param: Any, ctx: Any) -> int:
        try:
            return int(value)
        except TypeError:
            self.fail(
                "expected string for int() conversion, got " f"{value!r} of type {type(value).__name__}", param, ctx
            )
        except ValueError:
            try:
                return getattr(logging, value.upper())
            except AttributeError:
                pass

            self.fail(f"{value!r} is not a valid integer", param, ctx)


@click.group()
@click.option("-p", "--port", type=int, envvar="TOX_SERVER_PORT", help="Port to connect for tox-server.", required=True)
@click.option(
    "-h", "--host", type=str, envvar="TOX_SERVER_HOST", default="localhost", help="Host to connect for tox-server."
)
@click.option(
    "-b",
    "--bind-host",
    type=str,
    envvar="TOX_SERVER_BIND_HOST",
    default="127.0.0.1",
    help="Host to bind for tox-server serve.",
)
@click.option(
    "-t",
    "--timeout",
    type=float,
    default=None,
    help="Timeout for waiting on a response (s).",
    envvar="TOX_SERVER_TIMEOUT",
)
@click.option(
    "-l",
    "--log-level",
    envvar="TOX_SERVER_LOG_LEVEL",
    type=LogLevelParamType(),
    help="Set logging level",
    default=logging.WARNING,
)
@click.pass_context
def main(ctx: click.Context, host: str, port: int, bind_host: str, timeout: Optional[float], log_level: int) -> None:
    """Interact with a tox server."""
    cfg = ctx.ensure_object(dict)

    cfg["uri"] = f"tcp://{host}:{port:d}"
    cfg["bind"] = f"tcp://{bind_host}:{port:d}"
    cfg["timeout"] = timeout
    logging.basicConfig(format=f"[{ctx.invoked_subcommand}] %(message)s", level=log_level)


init_client_commands(main)
main.add_command(serve)
