"""Write and push the root STAC catalog linking the four sibling trees."""

import json
from pathlib import Path
from subprocess import run as _run

from ._service import CATALOG_TITLE

_TREE_TITLES = {
    "original": "Original",
    "extended": "Extended",
    "matched": "Matched",
    "global": "Global",
}


def write_top_catalog(work_dir: Path, tree_names: list[str]) -> None:
    """Write work_dir/catalog.json, linking each sibling tree's own catalog.json."""
    data = {
        "type": "Catalog",
        "id": "cod-ab",
        "stac_version": "1.1.0",
        "description": CATALOG_TITLE,
        "links": [
            {
                "rel": "root",
                "href": "./catalog.json",
                "type": "application/json",
                "title": CATALOG_TITLE,
            },
            *(
                {
                    "rel": "child",
                    "href": f"./{name}/catalog.json",
                    "type": "application/json",
                    "title": _TREE_TITLES.get(name, name.title()),
                }
                for name in tree_names
            ),
        ],
    }
    (work_dir / "catalog.json").write_text(json.dumps(data, indent=2))


def push_top_catalog(work_dir: Path, remote: str) -> None:
    """Upload work_dir/catalog.json to the root of remote."""
    _run(
        [
            "aws",
            "s3",
            "cp",
            str(work_dir / "catalog.json"),
            f"{remote.rstrip('/')}/catalog.json",
        ],
        check=True,
    )


def _push_catalog_files(work_dir: Path, remote: str) -> None:
    """Sync intermediate catalog.json/README.md; portolan push skips those."""
    _run(
        [
            "aws",
            "s3",
            "sync",
            str(work_dir),
            remote.rstrip("/"),
            "--exclude",
            "*",
            "--include",
            "catalog.json",
            "--include",
            "*/catalog.json",
            "--include",
            "*/*/catalog.json",
            "--include",
            "*/README.md",
            "--include",
            "*/*/README.md",
            "--exclude",
            "*/*/*/*",
        ],
        check=True,
    )
