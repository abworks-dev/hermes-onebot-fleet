"""Profile-scoped compatibility identity for the single-source pure module."""

import sys

from ._compat import legacy_module

sys.modules[__name__] = legacy_module(__name__)
