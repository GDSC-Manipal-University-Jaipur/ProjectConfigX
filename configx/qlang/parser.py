"""
ConfigXQL - Parser (v0.1)

Uses Lark to parse ConfigXQL v0.1 into a clean, minimal AST
that can later be executed against ConfigTree.
Refer README.md for syntax & its rules

Developed & Maintained by Aditya Gaur, 2025
"""

from lark import Lark, Transformer, v_args
from dataclasses import dataclass
from typing import List, Any
import os

# -----------------------------------------------------------------------------
# AST Node Definitions
# -----------------------------------------------------------------------------

@dataclass
class ASTNode:
    pass


@dataclass
class GetNode(ASTNode):
    path: List[str]
    safe: bool = False


@dataclass
class SetNode(ASTNode):
    path: List[str]
    value: Any


@dataclass
class DeleteNode(ASTNode):
    path: List[str]


# -----------------------------------------------------------------------------
# Transformer: Parse Tree -> AST
# -----------------------------------------------------------------------------

@v_args(inline=True)
class ConfigXQLTransformer(Transformer):
    
    def start(self, *statements):
        return list(statements)


    def statement_list(self, *statements):
        return list(statements)
    
    def path(self, *parts):
        return [str(p) for p in parts]

        
    # --- Statements ---

    def get_stmt(self, path):
        return GetNode(path=path, safe=False)

    def safe_get_stmt(self, path):
        return GetNode(path=path, safe=True)

    def set_stmt(self, path, value):
        return SetNode(path=path, value=value)

    def delete_stmt(self, path):
        return DeleteNode(path=path)

    # --- Datatypes ---

    def string(self, token):
        # token is an escaped string, remove quotes
        return token[1:-1]

    def int(self, token):
        return int(token)

    def float(self, token):
        return float(token)

    def bool(self, token):
        return token == "true"


# -----------------------------------------------------------------------------
# Parser Wrapper
# -----------------------------------------------------------------------------

class ConfigXQLParser:
    """
    Thin toffee wrapper around Lark parser + transformer.
    """

    def __init__(self):
        grammar_path = os.path.join(
            os.path.dirname(__file__), "configxql.lark"
        )

        with open(grammar_path, "r", encoding="utf-8") as f:
            grammar = f.read()

        self._parser = Lark(
            grammar,
            parser="lalr",
            transformer=ConfigXQLTransformer(),
            propagate_positions=True,
            maybe_placeholders=False,
        )

    def parse(self, query: str) -> ASTNode:
        """
        Parse a single ConfigXQL statement into an AST node.
        Raises Lark exceptions on syntax errors.
        """
        return self._parser.parse(query)
