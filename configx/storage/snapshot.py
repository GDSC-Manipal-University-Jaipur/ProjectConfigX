# configx/storage/snapshot.py

from __future__ import annotations
import struct
import io
import os

from configx.core.node import Node
from configx.core.errors import (
    ConfigInvalidFormatError,
    ConfigPathNotFoundError,
)


class SnapshotStore:
    """
    Handles full-state persistence of a ConfigTree.

    Snapshots store the complete tree structure at a point in time.
    They are used for fast startup, recovery checkpoints, and WAL compaction.
    """

    MAGIC = b"CFGX"
    VERSION = 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def save(cls, tree, file_path: str):
        """
        Save the entire tree to a binary snapshot.
        """
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(file_path, "wb") as f:
            cls._write_header(f)
            cls._write_node(f, tree.root)

    @classmethod
    def load(cls, tree, file_path: str):
        """
        Load tree state from a binary snapshot.
        This REPLACES the tree contents.
        """
        if not os.path.exists(file_path):
            raise ConfigPathNotFoundError(file_path)

        with open(file_path, "rb") as f:
            cls._read_header(f)
            tree.root = cls._read_node(f)

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    @classmethod
    def _write_header(cls, f: io.BufferedWriter):
        f.write(cls.MAGIC)
        f.write(struct.pack("B", cls.VERSION))

    @classmethod
    def _read_header(cls, f: io.BufferedReader):
        magic = f.read(4)
        if magic != cls.MAGIC:
            raise ConfigInvalidFormatError(
                "Invalid snapshot file (bad magic header)."
            )

        version = struct.unpack("B", f.read(1))[0]
        if version != cls.VERSION:
            raise ConfigInvalidFormatError(
                f"Unsupported snapshot version: {version}"
            )

    # ------------------------------------------------------------------
    # Node Serialization
    # ------------------------------------------------------------------

    @classmethod
    def _write_node(cls, f: io.BufferedWriter, node: Node):
        """
        Binary node format:
        [name_len][name][type_tag][value_len][value][child_count][children...]
        """
        # --- NAME ---
        name_bytes = node.name.encode("utf-8")
        f.write(struct.pack(">I", len(name_bytes)))
        f.write(name_bytes)

        # --- VALUE ---
        tag = b"N"
        val_bytes = b""

        if node.value is not None:
            if isinstance(node.value, bool):
                tag = b"B"
                val_bytes = struct.pack("?", node.value)
            elif isinstance(node.value, int):
                tag = b"I"
                val_bytes = struct.pack(">q", node.value)
            elif isinstance(node.value, float):
                tag = b"F"
                val_bytes = struct.pack(">d", node.value)
            elif isinstance(node.value, str):
                tag = b"S"
                val_bytes = node.value.encode("utf-8")
            else:
                raise ConfigInvalidFormatError(
                    f"Unsupported value type: {type(node.value)}"
                )

        f.write(tag)
        f.write(struct.pack(">I", len(val_bytes)))
        f.write(val_bytes)

        # --- CHILDREN ---
        children = list(node.children.values())
        f.write(struct.pack(">I", len(children)))

        for child in children:
            cls._write_node(f, child)

    @classmethod
    def _read_node(cls, f: io.BufferedReader) -> Node:
        """
        Read a node recursively from the binary snapshot.
        """
        raw_len = f.read(4)
        if not raw_len:
            raise ConfigInvalidFormatError("Unexpected EOF while reading snapshot.")

        name_len = struct.unpack(">I", raw_len)[0]
        name = f.read(name_len).decode("utf-8")
        node = Node(name=name)

        # --- VALUE ---
        tag = f.read(1)
        val_len = struct.unpack(">I", f.read(4))[0]
        val_data = f.read(val_len)

        if tag == b"N":
            node.value = None
            node.type = None
        elif tag == b"B":
            node.value = struct.unpack("?", val_data)[0]
            node.type = "BOOL"
        elif tag == b"I":
            node.value = struct.unpack(">q", val_data)[0]
            node.type = "INT"
        elif tag == b"F":
            node.value = struct.unpack(">d", val_data)[0]
            node.type = "FLOAT"
        elif tag == b"S":
            node.value = val_data.decode("utf-8")
            node.type = "STR"
        else:
            raise ConfigInvalidFormatError(f"Unknown value tag: {tag}")

        # --- CHILDREN ---
        child_count = struct.unpack(">I", f.read(4))[0]
        for _ in range(child_count):
            child = cls._read_node(f)
            node.children[child.name] = child

        return node
