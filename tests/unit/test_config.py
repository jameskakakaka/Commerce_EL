from datetime import date

from include import config


def test_paths_follow_env_overrides(workspace):
    d = date(2020, 3, 2)
    assert config.landing_dir() == workspace["landing"]
    assert config.duckdb_path() == workspace["db"]
    assert config.raw_path(d) == workspace["landing"] / "raw/2020/03/raw_data_20200302.csv"
    assert (
        config.processed_path(d)
        == workspace["landing"] / "processed/2020/03/clean_data_20200302.csv"
    )
    assert config.quarantine_path(d).parts[-4:] == (
        "quarantine",
        "2020",
        "03",
        "clean_data_20200302.csv",
    )


def test_defaults_live_inside_project(monkeypatch):
    monkeypatch.delenv("LANDING_DIR", raising=False)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    assert config.landing_dir() == config.PROJECT_ROOT / "landing"
    assert config.duckdb_path().is_relative_to(config.PROJECT_ROOT)


def test_parse_date_str():
    assert config.parse_date_str("20200101") == date(2020, 1, 1)


def test_default_args_have_retries():
    assert config.DEFAULT_ARGS["retries"] >= 2
