"""
ConfigX Testing Suite - test_configxql.py
Tests for ConfigXQL language layer:
- parser -> AST
- interpreter -> engine execution
- safe vs unsafe semantics

"""

import pytest
import os

import shutil

from configx.core.tree import ConfigTree
from configx.core.errors import ConfigPathNotFoundError
from configx.qlang.parser import ConfigXQLParser
from configx.qlang.interpreter import ConfigXQLInterpreter
from configx.runtime.configx import ConfigX


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def exec_query(tree, query):
    """
    Helper to parse + execute a single ConfigXQL statement.
    """
    interpreter = ConfigXQLInterpreter(tree)
    return interpreter.execute(query)


# -----------------------------------------------------------------------------
# Basic SET / GET
# -----------------------------------------------------------------------------

def test_set_and_get():
    tree = ConfigTree()

    exec_query(tree, 'app.ui.theme="dark"')
    result = exec_query(tree, 'app.ui.theme')

    assert result == "dark"


# -----------------------------------------------------------------------------
# DELETE
# -----------------------------------------------------------------------------

def test_delete():
    tree = ConfigTree()

    exec_query(tree, 'app.ui.theme="dark"')
    exec_query(tree, 'app.ui.theme-')

    with pytest.raises(ConfigPathNotFoundError):
        exec_query(tree, 'app.ui.theme')


# -----------------------------------------------------------------------------
# Safe vs Unsafe GET
# -----------------------------------------------------------------------------

def test_safe_get_returns_none():
    tree = ConfigTree()

    result = exec_query(tree, 'app.ui.missing!')
    assert result is None


def test_unsafe_get_raises():
    tree = ConfigTree()

    with pytest.raises(ConfigPathNotFoundError):
        exec_query(tree, 'app.ui.missing')


# -----------------------------------------------------------------------------
# Interior node returns {}
# -----------------------------------------------------------------------------

def test_interior_node_returns_dict():
    tree = ConfigTree()

    exec_query(tree, 'app.ui.theme="dark"')

    result = exec_query(tree, 'app.ui') 
    assert result == {"theme": "dark"}



# -----------------------------------------------------------------------------
# Strict string handling
# -----------------------------------------------------------------------------

def test_single_quotes_not_allowed():
    tree = ConfigTree()
    parser = ConfigXQLParser()

    with pytest.raises(Exception):
        parser.parse("app.ui.theme='dark'")


# -----------------------------------------------------------------------------
# Additional semantic boundary tests
# -----------------------------------------------------------------------------


def test_deep_interior_retrieval():
    tree = ConfigTree()
    exec_query(tree, 'a.b.c="x"')
            
    result = exec_query(tree, 'a')
    assert result == {"b": {"c": "x"}}


def test_delete_subtree():
    tree = ConfigTree()


    exec_query(tree, 'a.b.c="x"')
    exec_query(tree, 'a.b-')


    result = exec_query(tree, 'a')
    assert result == {}


def test_overwrite_leaf_value():
    tree = ConfigTree()


    exec_query(tree, 'a.b="x"')
    exec_query(tree, 'a.b="y"')


    result = exec_query(tree, 'a.b')
    assert result == "y"


def test_invalid_overwrite_interior_to_leaf():
    tree = ConfigTree()
    exec_query(tree, 'a.b.c="x"')

    with pytest.raises(Exception):
        exec_query(tree, 'a.b="y"')


def test_parser_rejects_unquoted_string():
    parser = ConfigXQLParser()

    with pytest.raises(Exception):
        parser.parse('a.b=dark')


# -----------------------------------------------------------------------------
# ConfigX initialization test
# -----------------------------------------------------------------------------

def test_configx_initialization_with_storage_dir():
    try:
        path = os.path.join(os.getcwd(),'memory')
        configx = ConfigX(storage_dir=path,persistent=True)
        configx.close()
    except AttributeError:
        raise
    finally:
        if os.path.exists(path):
            shutil.rmtree(path)

def test_list_basic():
    c = ConfigX()
    assert c.resolve('items=[1, 2, 3]') == [1, 2, 3]
    assert c.resolve('items') == [1, 2, 3]
    assert c.resolve('items!') == [1, 2, 3]
    assert c.resolve('missing!') is None

def test_list_nested():
    c = ConfigX()
    c.resolve('matrix1=[[1, 2], [3, 4]]')
    assert c.resolve('matrix1') == [[1, 2], [3, 4]]

def test_list_persistence():
    path = os.path.join(os.getcwd(),'test_db')
    c = ConfigX(storage_dir=path)
    c.resolve('data=[1, 2, 3]')
    c.close()
    
    c2 = ConfigX(storage_dir=path)
    c2.close()
    shutil.rmtree(path)
    assert c2.resolve('data') == [1, 2, 3]

