import itertools
import shlex
from typing import Dict, Optional, Union


def parse(cell, config) -> Dict[str, Optional[Union[str, bool]]]:
    """Extract connection info, result variable, and other meta-bits from SQL"""

    result = {"connection": "", "sql": "", "result_var": None, 'skip_boxing_scalar_result': False}

    breakpoint()

    pieces = cell.split()

    if not pieces:
        return result

    result["connection"] = pieces[0]
    pieces.pop(0)

    if len(pieces) > 1 and pieces[1] == "<<":
        result["result_var"] = pieces.pop(0)
        pieces.pop(0)  # discard << operator

    # Parse directives, like desire for a 1x1 result to not be boxed into a dataframe.
    if len(pieces) > 1 and pieces[0] == '#scalar':
        result['skip_boxing_scalar_result'] = True
        pieces.pop(0)  # discard

    joined_pieces = (" ".join(pieces)).strip()

    # If cell was multilined, there will be embedded newlines.
    # Need to strip away any any SQL comments else a cell like "-- the following shows all schemas\n\schemas"
    # will not be treated like a bare "\schemas" would.

    result["sql"] = '\n'.join(
        without_sql_comment(line) for line in joined_pieces.split('\n')
    ).strip()

    return result


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
