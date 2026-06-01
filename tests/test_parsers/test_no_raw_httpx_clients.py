import pathlib


def test_no_raw_httpx_async_client_in_url_pkg():
    """All URL httpx clients must go through parsers.url._http.make_async_client
    so they inherit the redirect + body-size caps. _http.py is the only file
    allowed to construct httpx.AsyncClient directly."""
    pkg = pathlib.Path("parsers/url")
    offenders = []
    for p in pkg.glob("*.py"):
        if p.name == "_http.py":
            continue
        if "httpx.AsyncClient(" in p.read_text(encoding="utf-8"):
            offenders.append(p.name)
    assert offenders == [], f"raw httpx.AsyncClient in: {offenders}"
