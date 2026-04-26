from krx_collector.cli import app


def test_dart_main_prefixes_dart_subcommand(monkeypatch) -> None:
    captured: dict[str, list[str] | None] = {}

    def fake_main(argv: list[str] | None = None) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(app, "main", fake_main)

    app.dart_main(["sync-corp"])

    assert captured["argv"] == ["dart", "sync-corp"]


def test_dart_main_uses_sys_argv_when_not_given(monkeypatch) -> None:
    captured: dict[str, list[str] | None] = {}

    def fake_main(argv: list[str] | None = None) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(app, "main", fake_main)
    monkeypatch.setattr("sys.argv", ["dart", "sync-corp", "--force"])

    app.dart_main()

    assert captured["argv"] == ["dart", "sync-corp", "--force"]
