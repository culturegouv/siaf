#!/usr/bin/env python3
"""
Moissonnage FranceArchives (multi-fichiers).

- gère les URLs avec redirection JavaScript ;
- gère les URLs qui renvoient directement le CSV ;
- enregistre les fichiers dans le dépôt.

Aucune dépendance externe.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, build_opener, HTTPCookieProcessor
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError


BASE_URL = "https://data-dump.francearchives.gouv.fr"

DATASETS = {
    "circulaires": "https://data-dump.francearchives.gouv.fr/ape-ead-eac/editorial/circulaires.csv",
    "annuaire": "https://data-dump.francearchives.gouv.fr/ape-ead-eac/editorial/annuaire.csv",
}

OUTPUT_DIR = Path("data")
TIMEOUT_SECONDS = 60
USER_AGENT = "siaf-moissonnage/1.0 (+https://github.com/culturegouv/siaf)"


cookie_jar = CookieJar()
opener = build_opener(HTTPCookieProcessor(cookie_jar))
opener.addheaders = [
    ("User-Agent", USER_AGENT),
]


def fetch_bytes(url: str, referer: str | None = None) -> bytes:
    headers = {"Accept": "*/*"}
    if referer:
        headers["Referer"] = referer

    request = Request(url, headers=headers)
    with opener.open(request, timeout=TIMEOUT_SECONDS) as response:
        return response.read()


def extract_redirect_url(text: str) -> str | None:
    match = re.search(
        r'window\.location\.href\s*=\s*[\'"]([^\'"]+)[\'"]',
        text,
    )
    if not match:
        return None
    return urljoin(BASE_URL, match.group(1))


def looks_like_html(content: bytes) -> bool:
    sample = content[:1000].lstrip().lower()
    return (
        sample.startswith(b"<html")
        or b"<script" in sample
        or b"<body" in sample
        or b"<noscript" in sample
    )


def save_file(content: bytes, filename: str) -> None:
    path = OUTPUT_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def process_dataset(name: str, source_url: str) -> None:
    print(f"\n--- Traitement : {name} ---")
    print(f"URL source : {source_url}")

    print("Téléchargement source")
    initial_content = fetch_bytes(source_url)

    if looks_like_html(initial_content):
        print("Contenu HTML détecté")
        html = initial_content.decode("utf-8", errors="replace")

        download_url = extract_redirect_url(html)
        if not download_url:
            raise ValueError(
                f"{name} : contenu HTML détecté mais aucune redirection JavaScript trouvée."
            )

        print(f"URL détectée : {download_url}")
        print("Téléchargement fichier réel")
        final_content = fetch_bytes(download_url, referer=source_url)
    else:
        print("Pas de HTML détecté : le fichier semble accessible directement")
        final_content = initial_content

    if looks_like_html(final_content):
        raise ValueError(
            f"{name} : le contenu final ressemble encore à du HTML et non à un CSV."
        )

    filename = f"{name}.csv"
    print(f"Enregistrement : {filename}")
    save_file(final_content, filename)

    print("OK")


def main() -> None:
    has_error = False

    for name, url in DATASETS.items():
        try:
            process_dataset(name, url)
        except HTTPError as e:
            has_error = True
            print(f"Erreur HTTP pour {name} : {e.code} - {e.reason}")
        except URLError as e:
            has_error = True
            print(f"Erreur réseau pour {name} : {e.reason}")
        except Exception as e:
            has_error = True
            print(f"Erreur pour {name} : {e}")

    if has_error:
        print("\nTerminé avec au moins une erreur.")
    else:
        print("\nTerminé avec succès.")


if __name__ == "__main__":
    main()
