"""
This module implements a simple parser for splitting a stream of text
into SQL statements.

It differs from the pglast.parse_sql function in the fact that it parses
and preserves comments, and ignores psql commands instead of erroring out.
"""
import re
import warnings
import logging
from pglast import parse_sql, Node, prettify


class ParserState():
    """
    Internal state of the parser.
    """

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def __repr__(self):
        return "%s(%s)" % (self.name, self.value or "")


class Comment():
    """
    A comment.

    Arguments:
        comment (str): the actual comment
        location (int): the position within a statement
        length (int): the comment length
    """

    def __init__(self, comment, location, length):
        self.comment = comment
        self.location = location
        self.length = length

    def to_node_dict(self):
        return {'InlineComment': {
                    'comment': self.comment,
                    'location': self.location,
                    'length': self.length}}



RE_DOLLARQUOTE = re.compile(r"\$(\w*)\$")


class Parser():
    """
    The parser itself.


    Usage:

    >>> myparser = Parser()
    >>> myparser.feed("SELECT col1 FROM")
    >>> print(list(myparser.statements))
    >>> myparser.feed("table1; SELECT col2")
    >>> print(list(myparser.statements))
    >>> myparser.feed("FROM table2;")
    >>> print(list(myparser.statements))


    The implementation is a simplified state machine, where fed text switches
    the current state, and the next chunk of text is dispatched according to
    the current text to one of the `handle_<state>` methods.
    """

    def __init__(self, validate=True):
        self.finished_statements = []
        self.current_statement = ""
        self.current_statements_comments = []
        self.stack = [ParserState("plain")]
        self.validate = validate
        self.comments = []
        self.current_comment = ""
        self.current_pos_in_statement = 0

    def feed(self, buf):
        """
        Feeds new text into the parser.

        Args:
            buf (str): new text to parse.
        """
        while buf:
            buf = getattr(self, "handle_%s" % self.stack[-1].name)(buf)

    @property
    def statements(self):
        """
        Returns:
            generator: Return value yielding fully parsed statements from the
            feed.
        """
        while self.finished_statements:
            stmt, comments = self.finished_statements.pop(0)
            if self.validate:
                with warnings.catch_warnings():
                    warnings.filterwarnings("error")
                    # The prettify stmt includes a sanity check raising
                    # warnings
                    try:
                        prettify(stmt, expression_level=1)
                    except Exception as e:
# We should raise Error, but pglast don't know about REPLICA
# And we are not able to fix and rebuild pglast package
                        logging.getLogger().debug("Error while parsing statement")
                        logging.getLogger().debug(stmt)
                        logging.getLogger().debug(e)
                        continue

            parse_stmt = Node(parse_sql(stmt)[0]).stmt
            if comments:
                parse_stmt.parse_tree['comments'] = comments
            parse_stmt.parse_tree['original_string'] = stmt
            if parse_stmt.node_tag == 'CreateFunctionStmt':
                # Find the language used
                for option in parse_stmt.options:
                    if option.defname == 'language':
                        parse_stmt.parse_tree['language'] = option.arg.string_value
            yield parse_stmt

    def handle_plain(self, buf):
        """
        Handle the default state, that is, probably a statement.
        """
        # If we are not currently in the middle of statement, trim
        # the content
        if self.current_statement == '':
            buf = buf.lstrip()

        for idx, ch in enumerate(buf):
            # Skip leading whitespaces
            if ch == "-" and buf[idx + 1] == "-":
                self.stack.append(ParserState("line_comment"))
                self.current_pos_in_statement += idx + 2

                self.current_statement += "--"
                return buf[idx + 2:]
            if ch == "/" and buf[idx + 1] == "*":
                self.stack.append(ParserState("block_comment"))
                self.current_pos_in_statement += idx + 2
                self.current_statement += "/*"
                return buf[idx + 2:]
            if ch == "$":
                # Need to check if we're the start of dollar-quote.
                match = RE_DOLLARQUOTE.match(buf[idx:])
                if match:
                    self.stack.append(ParserState("dollar_quote",
                                                  match.group(1)))
                    end_of_match = idx + match.span()[1]
                    self.current_statement += buf[idx:end_of_match]
                    self.current_pos_in_statement += end_of_match
                    return buf[end_of_match:]
            if ch == ";":
                self.current_statement += ";"
                full_stmt = self.current_statement.strip()
                if full_stmt:
                    self.finished_statements.append(
                            (full_stmt, self.current_statements_comments))
                self.current_statement = ""
                self.current_statements_comments = []
                self.current_pos_in_statement = 0
                return buf[idx + 1:]
            if ch == '\\':
                self.stack.append(ParserState('psql_command'))
                self.current_pos_in_statement += idx + 1
                return buf[idx + 1:]
            if ch == "'":
                self.current_statement += "'"
                self.stack.append(ParserState('literal'))
                self.current_pos_in_statement += idx + 1
                return buf[idx + 1:]

            self.current_statement += ch
        return None

    def handle_literal(self, buf):
        """
        Handle literal strings.
        """
        for idx, ch in enumerate(buf):
            self.current_statement += ch
            if ch == "'":
                self.stack.pop()
                self.current_pos_in_statement += idx + 1
                return buf[idx+1:]
        return None

    def handle_psql_command(self, buf):
        """
        Handle psql commands.
        """
        for idx, ch in enumerate(buf):
            if ch == "\n":
                self.stack.pop()
            self.current_pos_in_statement += idx + 1
            return buf[idx + 1:]

    def handle_line_comment(self, buf):
        """
        Handle line comment.
        """
        for idx, ch in enumerate(buf):
            self.current_statement += ch
            if ch == "\n":
                self.stack.pop()
                self.comments.append(self.current_comment)
                com = Comment(self.current_comment,
                              self.current_pos_in_statement,
                              idx)
                self.current_statements_comments.append(com.to_node_dict())
                self.current_comment = ""
                self.current_pos_in_statement += idx + 1
                return buf[idx + 1:]
            self.current_comment += ch
        self.current_pos_in_statement += idx + 1
        return None

    def handle_block_comment(self, buf):
        """
        Handle block comment
        """
        for idx, ch in enumerate(buf):
            if ch == "*" and buf[idx + 1] == "/":
                self.stack.pop()
                self.comments.append(self.current_comment)
                self.current_comment = ""
                self.current_pos_in_statement += idx + 2
                self.current_statement += "*/"
                return buf[idx + 2:]
            # Block comments can be nested
            if ch == "/" and buf[idx + 1] == "*":
                self.stack.append(ParserState("block_comment"))
                self.current_pos_in_statement += idx + 2
                self.current_statement += "/*"
                return buf[idx + 2:]
            self.current_comment += ch
            self.current_statement += ch
        return None

    def handle_dollar_quote(self, buf):
        """
        Handle dollar-quoted literal strings.
        """
        for idx, ch in enumerate(buf):
            self.current_statement += ch
            if ch == "$":
                # Try to find our own value
                matches = RE_DOLLARQUOTE.finditer(buf[idx:])
                for match in matches:
                    value = match.group(1)
                    if value == self.stack[-1].value:
                        self.stack.pop()
                        end_of_match = idx + match.span()[1]
                        self.current_statement += buf[idx + 1:end_of_match]
                        self.current_pos_in_statement += end_of_match
                        return buf[end_of_match:]
        return None


def is_on_same_line(node, comment, original_stmt):
    lines = original_stmt.split('\n')
    lineno = 0
    start_of_line = 0
    node_pos = node.location.value
    comment_pos = comment.location.value
    for idx, line in enumerate(lines):
        lineno += 1
        end_of_line = start_of_line + len(line)
        # This is the line for the start of this node.
        if node_pos > start_of_line and node_pos < end_of_line:
            return (comment_pos > start_of_line and
                    comment_pos < end_of_line)
        start_of_line = end_of_line + 1
    return False


def comments_on_same_line(node, stmt):
    # TODO: this is the dumbest algorithm ever, rewrite that as soon as needed.
    return  [x for x in stmt.comments
             if is_on_same_line(node, x, stmt.original_string.value)]
