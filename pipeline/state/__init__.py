# pipeline/state/__init__.py
"""
Re-exports the three incremental state managers so existing call sites
(`from pipeline.state import SFTPState, SilverState, GoldState`) keep working
now that state.py has been split into one file per layer:

  pipeline/state/sftp_state.py   : SFTPState
  pipeline/state/silver_state.py : SilverState
  pipeline/state/gold_state.py   : GoldState

"""
from __future__ import annotations

from pipeline.state.sftp_state import SFTPState
from pipeline.state.silver_state import SilverState
from pipeline.state.gold_state import GoldState

__all__ = ["SFTPState", "SilverState", "GoldState"]
