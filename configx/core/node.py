"""
configx.core.node

Node definition : an atomic unit of meaning
A Node can represent:
```
theme → "dark"
ui → {theme: "dark"}
limits → {maxRetries: 5}
```

Refer rules.md for rules of a ConfigX Node.

Developed & Maintained by Aditya Gaur, 2025

"""


from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class Node:
    name: str
    value: Any = None
    type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    children: Dict[str, "Node"] = field(default_factory=dict)

    def is_leaf(self) -> bool:
        """
        is_leaf :
        Leaf node refers to a node that does not have children but a defined value instead.

        :return: Returns if the current node is a leaf node or not.
        :rtype: bool
        """
        return len(self.children) == 0
    
    def to_primitive(self):
        """
        to_primitive :
        Converts current node to a primitive python datatype 
        """

        if self.children:
            return {k: v.to_primitive() for k, v in self.children.items()}

        # leaf with value -> return that
        if self.value is not None:
            return self.value

        # empty node -> treat as empty map
        return {}


    @staticmethod
    def from_primitive(name: str, data) -> "Node":
        """
        from_primitive :
        Bridge between external data formats and ConfigX's internal tree-node structure

        :param name: Name of the node to be loaded from primitive-type
        :type name: str
        :param data: Dict data
        :return: Description
        :rtype: Node
        """
        node = Node(name=name)

        if isinstance(data, dict):
            for key, value in data.items():
                node.children[key] = Node.from_primitive(key, value)

        else:
            node.value = data
            node.type = Node.infer_type(data)

        return node
    

    @staticmethod
    def infer_type(value) -> str:
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int) and not isinstance(value, bool):
            return "INT"
        if isinstance(value, float):
            return "FLOAT"
        if isinstance(value, str):
            return "STR"
        return "JSON"
