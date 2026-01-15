"""
ConfigX Testing Suite - test_list.py

Tests for LIST datatype support:
- Basic list parsing and retrieval
- Nested lists
- List persistence through snapshots

"""

import pytest
import os
import shutil
import tempfile

from configx.core.tree import ConfigTree
from configx.qlang.interpreter import ConfigXQLInterpreter
from configx.storage.runtime import StorageRuntime


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def exec_query(tree, query):
    """
    Helper to parse + execute a single ConfigXQL statement.
    """
    interpreter = ConfigXQLInterpreter(tree)
    return interpreter.execute(query)


@pytest.fixture()
def temp_storage():
    """
    Creates a temporary directory for snapshot + WAL files.
    Automatically cleaned up after test.
    """
    tmpdir = tempfile.mkdtemp()
    snapshot = os.path.join(tmpdir, "state.snapshot")
    wal = os.path.join(tmpdir, "state.wal")

    yield snapshot, wal

    shutil.rmtree(tmpdir)


# -----------------------------------------------------------------------------
# test_list_basic - create/retrieve/safe-get
# -----------------------------------------------------------------------------

def test_list_basic():
    tree = ConfigTree()

    # Set integer list
    exec_query(tree, 'items=[1,2,3]')
    result = exec_query(tree, 'items')
    assert result == [1, 2, 3]

    # Set string list
    exec_query(tree, 'names=["alice","bob"]')
    result = exec_query(tree, 'names')
    assert result == ["alice", "bob"]

    # Empty list
    exec_query(tree, 'empty=[]')
    result = exec_query(tree, 'empty')
    assert result == []

    # Safe get on existing list
    result = exec_query(tree, 'items!')
    assert result == [1, 2, 3]

    # Safe get on non-existing returns None
    result = exec_query(tree, 'missing!')
    assert result is None


# -----------------------------------------------------------------------------
# test_list_nested - nested lists like [[1,2],[3,4]]
# -----------------------------------------------------------------------------

def test_list_nested():
    tree = ConfigTree()

    exec_query(tree, 'nested=[[1,2],[3,4]]')
    result = exec_query(tree, 'nested')
    assert result == [[1, 2], [3, 4]]

    # Mixed types in nested
    exec_query(tree, 'mixed=[[1,"a"],[true,3.14]]')
    result = exec_query(tree, 'mixed')
    assert result == [[1, "a"], [True, 3.14]]


# -----------------------------------------------------------------------------
# test_list_persistence - save, close, reload, assert list
# -----------------------------------------------------------------------------

def test_list_persistence(temp_storage):
    snapshot, wal = temp_storage

    # Create and populate tree
    runtime = StorageRuntime(snapshot, wal)
    tree = ConfigTree(runtime=runtime)
    runtime.start(tree)

    tree.set("items", [1, 2, 3])
    tree.set("names", ["alice", "bob"])
    tree.set("nested", [[1, 2], [3, 4]])
    tree.set("empty", [])

    # Checkpoint and shutdown
    runtime.shutdown(tree)

    # Reload from snapshot
    runtime2 = StorageRuntime(snapshot, wal)
    tree2 = ConfigTree(runtime=runtime2)
    runtime2.start(tree2)

    assert tree2.get("items") == [1, 2, 3]
    assert tree2.get("names") == ["alice", "bob"]
    assert tree2.get("nested") == [[1, 2], [3, 4]]
    assert tree2.get("empty") == []
