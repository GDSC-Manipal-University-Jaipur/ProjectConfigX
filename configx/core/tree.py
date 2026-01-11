"""
configx.core.tree

Tree definition : The ConfigTree is the Manager of All Nodes
It has one job:

> Provide a clean, predictable interface to get/set/delete/query hierarchical parameters.

Refer rules.md for rules of a ConfigX Node.

Developed & Maintained by Aditya Gaur, 2025

"""


from __future__ import annotations
from typing import Any, Dict, List, Optional
import struct
import io
import os

from .node import Node
from .errors import (
    ConfigPathNotFoundError,
    ConfigInvalidPathError,
    ConfigStrictModeError,
    ConfigNodeStructureError,
    ConfigInvalidFormatError,
)


class ConfigTree:
    def __init__(self, strict_mode: bool = False, runtime=None):
        """
        Create a ConfigTree.

        :param strict_mode: when True, setting a deep path that has missing
                            intermediate nodes will raise an error instead
                            of auto-creating them.
        """
        # the root node holds the top-level children
        self.root: Node = Node(name="root")

        # runtime flag to enforce strict path creation behavior
        self.strict_mode: bool = strict_mode

        self.runtime = runtime

    def _split(self, path: str) -> List[str]:
        """
        Normalize and split a dotted path into parts.
        Examples:
          "a.b.c" -> ["a","b","c"]
        """
        if path is None:
            raise ConfigInvalidPathError(str(path), "Path cannot be None.")

        
        parts = [p for p in path.strip().split(".") if p]

        if len(parts) == 0:
            raise ConfigInvalidPathError(path, "Path cannot be empty.")

        return parts

    def _walk(self, path: str, create_missing: bool = False):
        """
        Walk the tree and return the node at `path`.
        If create_missing is True, intermediate nodes are created as interior nodes.
        Returns None if a required node is missing and create_missing is False.
        """
        parts = self._split(path)
        node = self.root

        for idx, part in enumerate(parts):
            # Node exists → descend
            if part in node.children:
                node = node.children[part]
                continue

            # Node missing
            if create_missing:
                # Strict mode disallows creating missing nodes
                if self.strict_mode:
                    raise ConfigStrictModeError(path)

                # Create new interior node
                new_node = Node(name=part)
                node.children[part] = new_node
                node = new_node
                continue

            # Missing but not allowed to create
            return None

        return node

    
    def get(self, path: str) -> Any:
        """
        Return a primitive value for a leaf node or a dict for an interior node.
        Raises KeyError if path does not exist.
        """
        node = self._walk(path, create_missing=False)
        if node is None:
            raise ConfigPathNotFoundError(path)

        return node.to_primitive()
    

    def set(self, path: str, value: Any, _internal: bool = False) -> Any:
        """
        Set a leaf value at `path`. Creates intermediate nodes if permitted.
        Enforces strict rule: a node that currently has children cannot be converted
        into a leaf (error), and a leaf with a value cannot become interior with children.
        Returns the assigned value.
        
        CRUD Ruleset : Validate -> Log -> Mutate 

        _internal : If enabled, No WAL logged
        
        """

        #validate 
        parts = self._split(path)
        if not parts:
            raise ConfigInvalidPathError(path, "Empty path is not allowed.")

        # walk and create intermediates if allowed
        node = self._walk(path, create_missing=True)
        if node is None:
            raise ConfigPathNotFoundError(path)

        # strict rule: cannot assign to interior node
        if len(node.children) > 0:
            raise ConfigNodeStructureError(
                path,
                "Cannot assign value to an interior node; it has children."
            )
        
        #log
        if not _internal and self.runtime:
            self.runtime.before_set(path, value)

        #apply mutation, safe to set: assign value and infer type
        node.value = value
        node.type = Node.infer_type(value)
        
        # ensure children remain empty for strictness (defensive)
        node.children = {}

        return node.value

    def delete(self, path: str, _internal: bool = False) -> bool:
        """
        Delete the node at `path`. Returns True if deletion occurred, False if path not found.
        Deleting the root is forbidden.
        
        CRUD Ruleset : Validate -> Log -> Mutate 
        
        _internal : If enabled, No WAL logged
        """
        #validate
        parts = self._split(path)

        if len(parts) == 1 and parts[0] == "root":
            raise ConfigNodeStructureError(path, "Cannot delete root node.")

        parent_path = ".".join(parts[:-1])
        parent = self._walk(parent_path, create_missing=False) if parent_path else self.root

        if parent is None:
            return False

        key = parts[-1]

        if key not in parent.children:
            return False
        
        #log 
        if not _internal and self.runtime:
            self.runtime.before_delete(path)
    
        #mutate
        parent.children.pop(key)
        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the entire tree into a nested Python dict of primitives.
        """
        return self.root.to_primitive() if self.root.children else {}

    def load_dict(self, data: Dict[str, Any]):
        """
        Replace the tree with nodes built from a Python dict.
        This is a destructive operation: it resets the existing tree.
        """
        if not isinstance(data, dict):
            raise ConfigInvalidFormatError("Top-level configuration must be a dict.")

        self.root = Node(name="root")

        for key, value in data.items():
            if not isinstance(key, str):
                raise ConfigInvalidFormatError("All keys must be strings.")

            self.root.children[key] = Node.from_primitive(key, value)

    def set_strict_mode(self, enabled: bool):
        """Allow toggling strict mode at runtime."""
        self.strict_mode = bool(enabled)

