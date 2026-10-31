import pytest


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("TAXIFLOW_DATA", str(tmp_path))
    return tmp_path
