import itertools
import shlex
from os.path import expandvars

from six.moves import configparser as CP
from sqlalchemy.engine.url import URL


def connection_from_dsn_section(section, config):
    parser = CP.ConfigParser()
    parser.read(config.dsn_filename)
    cfg_dict = dict(parser.items(section))
    return str(URL.create(**cfg_dict))


def _connection_string(s, config):

    s = expandvars(s)  # for environment variables
    if "@" in s or "://" in s:
        return s
    if s.startswith("[") and s.endswith("]"):
        section = s.lstrip("[").rstrip("]")
        parser = CP.ConfigParser()
        parser.read(config.dsn_filename)
        cfg_dict = dict(parser.items(section))
        return str(URL.create(**cfg_dict))
    return ""


def parse(cell, config):
    """Extract connection info and result variable from SQL

    Please don't add any more syntax requiring
    special parsing.
    Instead, add @arguments to SqlMagic.execute.

    We're grandfathering the
    connection string and `<<` operator in.
    """

    result = {"connection": "", "sql": "", "result_var": None}

    pieces = cell.split(None, 3)
    if not pieces:
        return result
    result["connection"] = _connection_string(pieces[0], config)
    if result["connection"]:
        pieces.pop(0)
    if len(pieces) > 1 and pieces[1] == "<<":
        result["result_var"] = pieces.pop(0)
        pieces.pop(0)  # discard << operator

    joined_pieces = (" ".join(pieces)).strip()

    # If cell was multilined, there will be embedded newlines.
    # Need to strip away any any SQL comments else a cell like "-- the following shows all schemas\n\schemas"
    # will not be treated like a bare "\schemas" would.

    result["sql"] = '\n'.join(
        without_sql_comment(line) for line in joined_pieces.split('\n')
    ).strip()

    return result


def _option_strings_from_parser(parser):
    """Extracts the expected option strings (-a, --append, etc) from argparse parser

    Thanks Martijn Pieters
    https://stackoverflow.com/questions/28881456/how-can-i-list-all-registered-arguments-from-an-argumentparser-instance

    :param parser: [description]
    :type parser: IPython.core.magic_arguments.MagicArgumentParser
    """
    opts = [a.option_strings for a in parser._actions]
    return list(itertools.chain.from_iterable(opts))


def without_sql_comment(line):
    """Strips -- comment from a line

    :param line: A line of SQL.
    :type line: str
    """
    result = itertools.takewhile(
        lambda word: not word.startswith("--"),
        shlex.split(line, posix=False),
    )
    return " ".join(result)
