# configx/runtime/configx.py
"""
ConfigX Public Runtime API
-------------------------

This module exposes the *single public entry point* for using ConfigX as a
Python library. All other layers (core, storage, qlang) remain internal.

Design goals:
- One stable API surface: ConfigX
- Language-agnostic core (CLI / server / other bindings wrap this)
- Hide parser / interpreter / tree internals
- Expose only *actually implemented* lifecycle features


No disk

No files

Perfect for tests, scripts, AI agents
"""

from typing import Any, Optional

from configx.core.tree import ConfigTree
from configx.storage.runtime import StorageRuntime
from configx.qlang.interpreter import ConfigXQLInterpreter
import os, json
import atexit

from colorama import Fore, Style, init
init(autoreset=True)


from colorama import Fore, Style, init
init(autoreset=True)


class _TreeRenderer:
    ROOT = Style.BRIGHT + Fore.WHITE

    OBJ = Fore.WHITE
    VAL = Fore.GREEN              # ConfigX accent
    TYPE = Fore.LIGHTBLACK_EX     # subtle gray
    TREE = Fore.LIGHTBLACK_EX     # structure only


    @classmethod
    def render(cls, node, prefix="", is_last=True, show_values=False):
        lines = []

        connector = "└── " if is_last else "├── "
        line = prefix + cls.TREE + connector

        # Object node (has children)
        if node.children:
            line += cls.OBJ + node.name
            lines.append(line)

            new_prefix = prefix + cls.TREE + ("    " if is_last else "│   ")

            children = list(node.children.values())
            for i, child in enumerate(children):
                lines.extend(
                    cls.render(
                        child,
                        prefix=new_prefix,
                        is_last=(i == len(children) - 1),
                        show_values=show_values
                    )
                )

        # Value node (leaf)
        else:
            line += cls.VAL + node.name
            line += cls.TYPE + f" : {node.type or 'UNKNOWN'}"

            if show_values and node.value is not None:
                line += cls.TYPE + " = " + repr(node.value)

            lines.append(line)

        return lines


class ConfigX:
    """
    Public ConfigX runtime.

    Example usage:

        confx = ConfigX()
        confx.resolve('app.ui.theme="dark"')
        value = confx.resolve('app.ui.theme')
    """
    def __init__(
        self,
        *,
        persistent: bool = False,
        storage_dir: Optional[str] = None,
        load_json: Optional[str] = None,
        ):
        """
        Initialize a ConfigX runtime.
        Args:
        persistent: Enable WAL + snapshot persistence
        storage_dir: Custom storage directory (defaults to .configx/)
        load_json: Optional JSON file to bootstrap initial state
        
        """
        print(f"Welcome to {Style.BRIGHT}{Fore.GREEN}ConfigX Runtime {Fore.WHITE}(v0.1.0)")

        # Core in-memory structure
        self._tree = ConfigTree()
        self._intp = ConfigXQLInterpreter(self._tree)
        self._closed = False # Made close() idempotent


        # Persistence runtime (optional)
        self._storage = None
        if storage_dir:
            snapshot_path = f"{storage_dir}/snapshot.cx"
            wal_path = f"{storage_dir}/wal.cx"

            self._storage = StorageRuntime(snapshot_path, wal_path)
            self._tree.runtime = self._storage
            self._storage.start(self._tree)

            self._storage = None


        if persistent:
            base_dir = storage_dir or os.path.join(os.getcwd(), ".configx")
            os.makedirs(base_dir, exist_ok=True)


            snapshot_path = os.path.join(base_dir, "snapshot.cx")
            wal_path = os.path.join(base_dir, "wal.cx")


            self._storage = StorageRuntime(snapshot_path, wal_path)
            self._tree.runtime = self._storage
            self._storage.start(self._tree)
    
        if load_json:
            self.load_json(load_json)

        if persistent:
            atexit.register(self.close) #Auto-close on program exit as a safety feature

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, query: str) -> Any:
        """
        Resolve a ConfigXQL query against the current runtime.

        This is the primary API surface for ConfigX.
        """
        return self._intp.execute(query)
    
    def load_json(self, path: str):
        """
        Load a JSON file and ingest it as initial state.
        This mutates the current tree.
        """
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._ingest_dict(data)

    def close(self):
        """
        Flush state to disk (snapshot + WAL cleanup).
        Safe to call multiple times
        """
        if self._closed:
            return

        if self._storage:
            self._storage.shutdown(self._tree)

        self._closed = True

    def print_tree(self, hide_values=False) -> str:
        """
        Pretty-print the current ConfigX state as a tree.
        """
        from configx.core.node import Node

        root = self._tree.root

        lines = [
            _TreeRenderer.ROOT + "ConfigX"
        ]

        children = list(root.children.values())
        for i, child in enumerate(children):
            lines.extend(
                _TreeRenderer.render(
                    child,
                    prefix="",
                    is_last=(i == len(children) - 1),
                    show_values=not hide_values
                )
            )

        print("\n".join(lines))
    
    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------


    def _ingest_dict(self, data: dict, prefix: str = ""):
        """
        Recursively ingest a Python dict into the ConfigTree.
        """
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else key


            if isinstance(value, dict):
                # Ensure branch exists
                self._tree.ensure_branch(path)
                self._ingest_dict(value, path)
            else:
                self._tree.set(path, value)


    def dump(self) -> dict:
        """
        Dump the entire configuration tree as a Python dict.
        Intended for debugging, exporting, and inspection.
        """
        return self._tree.to_dict()

    # ------------------------------------------------------------------
    # Explicitly unsupported (future features)
    # ------------------------------------------------------------------

    def transaction(self):
        """
        Transactions are not implemented yet.

        WAL provides durability and crash recovery, but multi-operation
        atomic transactions will be added in a future version.
        """
        raise NotImplementedError(
            "Transactions are not implemented yet. WAL-only durability is enabled."
        )
