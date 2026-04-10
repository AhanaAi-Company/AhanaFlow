from __future__ import annotations

import json

import pytest

from ahanaflow import _cli


def test_parse_value_decodes_json() -> None:
    assert _cli._parse_value('{"role": "admin"}') == {"role": "admin"}
    assert _cli._parse_value("123") == 123


def test_parse_value_falls_back_to_raw_string() -> None:
    assert _cli._parse_value("not-json") == "not-json"


def test_build_parser_accepts_mode_command() -> None:
    parser = _cli.build_parser()
    args = parser.parse_args(["--host", "localhost", "mode", "fast"])
    assert args.host == "localhost"
    assert args.command == "mode"
    assert args.mode == "fast"


def test_main_ping_uses_client_and_prints(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    class StubClient:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            assert host == "127.0.0.1"
            assert port == 9633
            assert timeout == 5.0

        def __enter__(self) -> "StubClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def ping(self) -> str:
            return "PONG"

    monkeypatch.setattr(_cli, "AhanaFlowClient", StubClient)
    assert _cli.main(["ping"]) == 0
    assert capsys.readouterr().out.strip() == "PONG"


def test_main_stats_prints_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    class StubClient:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            pass

        def __enter__(self) -> "StubClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def stats(self) -> dict[str, int]:
            return {"keys": 3, "queues": 1}

    monkeypatch.setattr(_cli, "AhanaFlowClient", StubClient)
    assert _cli.main(["stats"]) == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == {"keys": 3, "queues": 1}
