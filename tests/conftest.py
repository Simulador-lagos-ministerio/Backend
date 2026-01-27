"""Test configuration helpers for import paths."""
import sys
from pathlib import Path

# Ensure the app package (Backend/src) is importable in tests.
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
