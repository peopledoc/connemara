"""
This module overrides some node printers from pglast.
Those overrides should be removed as soon as the fixes have been incorporated
into upstream pglast.
"""

from pglast.printer import node_printer
from pglast.node import Missing, Node, List
from pglast.printers import dml
from pglast import enums
from pglast.printers.ddl import OBJECT_NAMES
import re

@node_printer('CollateClause', override=True)
def collate_clause(node, output):
    if node.arg:
        output.print_node(node.arg)
    output.swrite('COLLATE ')
    output.print_name(node.collname, '.')

@node_printer('AlterEnumStmt', override=True)
def alter_enum_stmt(node, output):
    output.write("ALTER TYPE ")
    output.print_name(node.typeName)
    if node.newVal:
        if node.oldVal:
            output.write("RENAME VALUE ")
            output._write_quoted_string(node.oldVal.value)
            output.write("TO ")
        else:
            output.write("ADD VALUE ")
            if node.skipIfNewValExists:
                output.write("IF NOT EXISTS ")
        output._write_quoted_string(node.newVal.value)
    if node.newValNeighbor:
        if node.newValIsAfter:
            output.write(" AFTER ")
        else:
            output.write(" BEFORE ")
        output._write_quoted_string(node.newValNeighbor.value)

