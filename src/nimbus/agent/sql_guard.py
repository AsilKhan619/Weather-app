"""SELECT-only enforcement for the agent's `run_sql` (brief section 11, ADR 0009).

The agent writes SQL; this decides whether it may run. Checking that the top-level
statement is a SELECT is not enough - sqlglot parses all of these as plain SELECTs:

    WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d      -- a write in a CTE
    SELECT * INTO new_table FROM t                              -- creates a table
    SELECT pg_sleep(600), set_config(...), lo_import('/etc/x')  -- side effects

So the whole tree is walked: exactly one statement, a query at the top, no write or DDL
node anywhere, no SELECT ... INTO, no row locks, no function from a deny list, and every
table schema-qualified in silver, gold or ops (so system catalogs are out of reach). The
query that runs is regenerated from the validated tree, without comments, never the raw
text, so nothing the parser did not see can reach Postgres.

This is the first of three layers. The query also runs as a read-only Postgres role, in a
read-only transaction, under a statement timeout - so a gap here still cannot write."""

import logging
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

# sqlglot warns when it falls back to an opaque Command; those are rejected below anyway.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

ALLOWED_SCHEMAS = frozenset({"silver", "gold", "ops"})

# Nodes that change data or schema, wherever they appear in the tree.
_FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Copy,
    exp.Command,
    exp.Set,
    exp.Into,
    exp.Lock,
)

# Functions with side effects, file or network access, arbitrary-SQL execution, or waits.
_DENIED_FUNCTIONS = frozenset(
    {
        "set_config",
        "current_setting",
        "query_to_xml",
        "query_to_xml_and_xmlschema",
        "cursor_to_xml",
        "table_to_xml",
        "schema_to_xml",
        "database_to_xml",
        "nextval",
        "setval",
        "dblink",
    }
)
_DENIED_PREFIXES = ("pg_", "lo_", "dblink_", "txid_")


class UnsafeSQLError(ValueError):
    """The query was rejected before reaching the database; the message says why."""


@dataclass(frozen=True)
class SafeQuery:
    sql: str  # regenerated from the validated tree - this is what runs
    tables: tuple[str, ...]


def _function_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def validate_select(sql: str) -> SafeQuery:
    try:
        statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except ParseError as exc:
        raise UnsafeSQLError(f"could not parse the query: {str(exc).splitlines()[0]}") from exc
    if len(statements) != 1:
        raise UnsafeSQLError(f"exactly one statement is allowed, got {len(statements)}")
    tree = statements[0]
    if not isinstance(tree, exp.Query):
        raise UnsafeSQLError(f"only SELECT queries are allowed, got {type(tree).__name__}")

    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise UnsafeSQLError(f"{type(node).__name__} is not allowed in a read-only query")
        if isinstance(node, exp.Func):
            name = _function_name(node)
            if name in _DENIED_FUNCTIONS or name.startswith(_DENIED_PREFIXES):
                raise UnsafeSQLError(f"function {name}() is not allowed")

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    tables: list[str] = []
    for table in tree.find_all(exp.Table):
        name, schema = table.name.lower(), (table.db or "").lower()
        if not schema and name in cte_names:
            continue
        if schema not in ALLOWED_SCHEMAS:
            raise UnsafeSQLError(
                f"table {table.sql(dialect='postgres')} is not allowed: qualify tables with "
                "one of the schemas silver, gold or ops (see describe_data)"
            )
        tables.append(f"{schema}.{name}")
    if not tables:
        raise UnsafeSQLError("the query must read at least one silver, gold or ops table")
    # Comments are dropped, not re-emitted: they can carry nothing the query needs.
    return SafeQuery(tree.sql(dialect="postgres", comments=False), tuple(sorted(set(tables))))
