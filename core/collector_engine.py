#!/usr/bin/env python3
"""CollectorEngine v2 compatibility layer.

The project is migrating from BaseCollector naming to CollectorEngine naming.
Keep this alias so existing runtime behavior stays unchanged while code follows
v2 guide imports.
"""

from .base_collector import BaseCollector


class CollectorEngine(BaseCollector):
    """v2 base class name alias for BaseCollector."""

    pass
