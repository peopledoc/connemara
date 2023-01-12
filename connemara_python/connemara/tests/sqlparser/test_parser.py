import pytest
from connemara.sqlparser import parser


def assert_parsed_node(node, original_string, node_tag, comments, language):
    assert node.original_string == original_string
    assert node.node.node_tag == node_tag
    assert node.comments == comments
    assert node.language == language


def test_parser_statement():
    p = parser.Parser()
    p.feed("SELECT 1;")
    p.feed("SELECT * FROM table_name --test comment\n;")
    p.feed("SELECT * FROM table_name /*test comment*/;")
    p.feed("CREATE FUNCTION dup(int) RETURNS TABLE(f1 int, f2 text) AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$ LANGUAGE SQL;")
    p.feed("INSERT INTO table_name(column1, column2) VALUES (value1, value2);")

    assert_parsed_node(
        next(p.statements),
        "SELECT 1;",
        "SelectStmt",
        [],
        None,
    )
    assert_parsed_node(
        next(p.statements),
        "SELECT * FROM table_name --test comment\n;",
        "SelectStmt",
        [parser.Comment("test comment", location=27, length=12).to_node_dict()],
        None,
    )
    assert_parsed_node(
        next(p.statements),
        "SELECT * FROM table_name /*test comment*/;",
        "SelectStmt",
        [parser.Comment("test comment", location=27, length=12).to_node_dict()],
        None,
    )
    assert_parsed_node(
        next(p.statements),
        "CREATE FUNCTION dup(int) RETURNS TABLE(f1 int, f2 text) AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$ LANGUAGE SQL;",
        "CreateFunctionStmt",
        [],
        "sql",
    )
    assert_parsed_node(
        next(p.statements),
        "INSERT INTO table_name(column1, column2) VALUES (value1, value2);",
        "InsertStmt",
        [],
        None,
    )

    with pytest.raises(StopIteration):
        next(p.statements)

