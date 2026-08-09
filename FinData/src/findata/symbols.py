"""S&P 500 symbol universe loading."""

from __future__ import annotations

import io
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import wikipedia as wp

from findata.config import FIN_DATA_DIR

wp.set_user_agent(
    "sp500-updater (https://github.com/fja05680/sp500; contact: fja0568@gmail.com)"
)

SP500_CSV = FIN_DATA_DIR / "sp500.csv"

TableLoader = Callable[[str, Path, str, bool], pd.DataFrame]


def get_table(
    title: str,
    filename: Path,
    match: str,
    use_cache: bool = False,
) -> pd.DataFrame:
    if not (use_cache and filename.is_file()):
        html = wp.page(title).html()
        df = pd.read_html(io.StringIO(html), header=0, match=match)[0]
        df.to_csv(filename, header=True, index=False, encoding="utf-8")

    return pd.read_csv(filename)


def get_sp500_symbols(
    gics_sector: str | None = None,
    *,
    csv_path: Path = SP500_CSV,
    table_loader: TableLoader = get_table,
) -> list[str]:
    sp500 = table_loader(
        "List of S&P 500 companies", csv_path, "Symbol", True
    )
    if gics_sector:
        sp500 = sp500[sp500["GICS Sector"] == gics_sector]
    return sp500["Symbol"].tolist()
