"""
This module overrides some node printers from pglast.
Those overrides should be removed as soon as the fixes have been incorporated
into upstream pglast.
"""

from pglast.printer import node_printer
from pglast.node import Missing, Node, List
from pglast.printers import dml
from pglast import enums
from pglast.printers.ddl import OBJECT_NAMES, AlterTableTypePrinter
import re

# Monkey patch AlterTableTypePrinter

def AT_SetStatistics(self, node, output):
    output.write("ALTER COLUMN ")
    if node.name:
        output.print_name(node.name)
    elif node.num:
        output.write(str(node.num.value))
    output.write(" SET STATISTICS ")
    output.print_node(node['def'])


def AT_SetUnLogged(self, node, output):
    output.write("SET UNLOGGED")

def AT_SetLogged(self, node, output):
    output.write("SET LOGGED")


AlterTableTypePrinter.AT_SetStatistics = AT_SetStatistics
AlterTableTypePrinter.AT_SetUnLogged = AT_SetUnLogged
AlterTableTypePrinter.AT_SetLogged = AT_SetLogged
