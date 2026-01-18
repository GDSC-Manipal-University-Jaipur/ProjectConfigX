"""
ConfigX Testing Suite - test_node.py

End-to-end tests for ConfigX Node 

Developed & Maintained by Aditya Gaur, 2025
"""
from configx.core.node import Node

def test_leaf_detection():
    """
    Test if leaf detection is working correctly or not.
    """
    leaf = Node(name="value_node", value=10, type="INT")
    assert leaf.is_leaf() is True

def test_interior_node_detection():
    """
    Test if child is node or not
    """
    interior = Node(name="root")
    interior.children["child"] = Node(name="child", value="dark", type="STR")
    assert interior.is_leaf() is False

def test_to_primitive_leaf():
    """
    Test to_primitive conversion
    """
    node = Node(name="theme", value="dark", type="STR")
    assert node.to_primitive() == "dark"

def test_to_primitive_interior():
    """
    Test for to_interior conversion (tree-node)
    """
    root = Node(name="root")
    root.children["theme"] = Node(name="theme", value="dark", type="STR")
    assert root.to_primitive() == {"theme": "dark"}

def test_from_primitive_dict():
    """
    Test for loading from dict to interior (tree-node)
    """
    tree = Node.from_primitive("root", {"a": {"b": 10}})
    assert tree.children["a"].children["b"].value == 10

def test_type_inference():
    """
    Test for type inference check
    """
    assert Node.infer_type(True) == "BOOL"
    assert Node.infer_type(10) == "INT"
    assert Node.infer_type(1.2) == "FLOAT"
    assert Node.infer_type("hi") == "STR"
