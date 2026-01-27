# tests/conftest.py
import sys
from pathlib import Path

# ensure src is importable
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))
