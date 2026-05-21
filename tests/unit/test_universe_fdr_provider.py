from __future__ import annotations

from datetime import date

import pandas as pd

from krx_collector.adapters.universe_fdr.provider import FdrUniverseProvider
from krx_collector.domain.enums import Market, Source
from krx_collector.domain.models import StockUniverseSnapshot, UniverseResult
from krx_collector.util.time import now_kst


def test_fdr_provider_maps_code_column(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    def fake_stock_listing(market: str) -> pd.DataFrame:
        assert market == "KOSPI"
        return pd.DataFrame(
            [
                {"Code": "005930", "Name": "Samsung Electronics"},
                {"Code": "000660", "Name": "SK hynix"},
            ]
        )

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.fdr.StockListing",
        fake_stock_listing,
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.source == Source.FDR
    assert [stock.ticker for stock in result.snapshot.records] == ["005930", "000660"]


def test_fdr_provider_maps_symbol_column(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: pd.DataFrame([{"Symbol": "035420", "Name": "NAVER"}]),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert [stock.ticker for stock in result.snapshot.records] == ["035420"]


def test_fdr_provider_falls_back_to_pykrx(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: (_ for _ in ()).throw(ValueError("Failed to load data from KRX")),
    )

    fallback_snapshot = StockUniverseSnapshot(
        snapshot_id="fallback",
        as_of_date=date(2026, 5, 21),
        source=Source.PYKRX,
        fetched_at=now_kst(),
        records=[],
    )

    def fake_fallback(self, markets: list[Market], as_of: date | None = None) -> UniverseResult:
        assert markets == [Market.KOSPI]
        assert as_of == date(2026, 5, 21)
        return UniverseResult(snapshot=fallback_snapshot)

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.PykrxUniverseProvider.fetch_universe",
        fake_fallback,
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is fallback_snapshot


def test_fdr_provider_reports_both_errors_when_fallback_fails(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: (_ for _ in ()).throw(ValueError("FDR down")),
    )

    monkeypatch.setattr(
        "krx_collector.adapters.universe_fdr.provider.PykrxUniverseProvider.fetch_universe",
        lambda self, markets, as_of=None: UniverseResult(error="pykrx down"),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.snapshot is None
    assert result.error == "FDR failed: FDR down; pykrx fallback failed: pykrx down"
