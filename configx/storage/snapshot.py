# configx/storage/snapshot.py

from __future__ import annotations
import struct
import io
import os
from typing import Any

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

        def _serialize_value_to_bytes(value: Any)->bytes:
            """This function converts values into serialized binary form
                Return the serialized bytes for a value in the form:
                [tag][value_len][value]

            """
            tag = b"N"
            val_bytes = b""
            if value is not None:
                if isinstance(value, bool):
                    tag = b"B"
                    val_bytes = struct.pack("?", value)
                elif isinstance(value, int):
                    tag = b"I"
                    val_bytes = struct.pack(">q", value)
                elif isinstance(value, float):
                    tag = b"F"
                    val_bytes = struct.pack(">d", value)
                elif isinstance(value, str):
                    tag = b"S"
                    val_bytes = value.encode("utf-8")
                elif isinstance(value,list):
                    tag = b"L"
                    inner = bytearray() # for containing byte elements of the list in a seralized (list like object for bytes)
                    for item in value:
                        inner.extend(_serialize_value_to_bytes(item)) # inserts the returned element
                    val_bytes = bytes(inner)

                else:
                    raise ConfigInvalidFormatError(
                        f"Unsupported value type: {type(value)}"
                    )
            
            out = bytearray()
            out.extend(tag)
            out.extend(struct.pack(">I", len(val_bytes)))
            out.extend(val_bytes)
            return bytes(out)
        
        # --- NAME ---
        name_bytes = node.name.encode("utf-8")
        f.write(struct.pack(">I", len(name_bytes)))
        f.write(name_bytes)
        
        # --- VALUE ---
        f.write(_serialize_value_to_bytes(node.value))

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
        def _type_value_reader(tag:bytes,val_data:bytes)->Any:
            if tag == b"N":
                value = None
                dtype = None
            elif tag == b"B":
                value = struct.unpack("?", val_data)[0]
                dtype = "BOOL"
            elif tag == b"I":
                value = struct.unpack(">q", val_data)[0]
                dtype = "INT"
            elif tag == b"F":
                value = struct.unpack(">d", val_data)[0]
                dtype = "FLOAT"
            elif tag == b"S":
                value = val_data.decode("utf-8")
                dtype = "STR"
            elif tag == b"L":
                value=[]
                dtype="LIST"
                byte_len = len(val_data)
                pointer=0 # It works as a pointer of bytes (of value data)
                while pointer<byte_len:
                    element_tag = val_data[pointer:pointer+1] # Tag of elements of list
                    pointer+=1 
                    if pointer+4>byte_len:
                        raise ConfigInvalidFormatError("Truncated element length in list.")
                    element_val_len = struct.unpack(">I", val_data[pointer:pointer+4])[0]
                    pointer+=4
                    if pointer + element_val_len > byte_len:
                        raise ConfigInvalidFormatError("Truncated element payload in list.")
                    element_val_data = val_data[pointer:pointer + element_val_len]
                    pointer += element_val_len
                    _, item_value = _type_value_reader(tag=element_tag,val_data=element_val_data)
                    value.append(item_value)
            else:
                raise ConfigInvalidFormatError(f"Unknown value tag: {tag}")
            return dtype,value
        
        # --- NAME ---
        raw_len = f.read(4)
        name_len = struct.unpack(">I", raw_len)[0]
        name_byte = f.read(name_len)
        name = name_byte.decode("utf-8")
        node = Node(name=name)
        
        # --- VALUE ---
        tag = f.read(1)
        val_len_bytes = f.read(4)
        val_len = struct.unpack(">I", val_len_bytes)[0]
        val_data = f.read(val_len) 
        dtype,value=_type_value_reader(tag=tag,val_data=val_data)
        node.type=dtype
        node.value=value

        # --- CHILDREN ---
        child_count = struct.unpack(">I", f.read(4))[0]
        for _ in range(child_count):
            child = cls._read_node(f)
            node.children[child.name] = child

        return node
