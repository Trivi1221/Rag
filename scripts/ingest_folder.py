#!/usr/bin/env python3

import argparse
import base64
import logging
import re
import threading
from pathlib import Path
from typing import Any

import requests

from private_gpt.server.ingest.ingest_watcher import IngestWatcher

logger = logging.getLogger(__name__)


class ApiIngestWorker:
    def __init__(
        self,
        root_path: Path,
        collection: str,
        debounce_ms: int,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        ignored: list[str] | None = None,
    ) -> None:
        self.root_path = root_path
        self.collection = collection
        self.debounce_ms = debounce_ms
        self.base_url = base_url.rstrip("/")
        self.ignored = set(ignored or [])
        self._headers: dict[str, str] = {}
        if username is not None and password is not None:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
                "ascii"
            )
            self._headers["Authorization"] = f"Basic {token}"

        self.total_documents = 0
        self.current_document_count = 0

        self._files_under_root_folder: list[Path] = []
        self._timers: dict[Path, threading.Timer] = {}
        self._timers_lock = threading.Lock()
        self._supported_extensions: set[str] | None = None
        self._supported_extensions_loaded = False

    def _artifact_from_path(self, file_path: Path) -> str:
        try:
            rel_path = file_path.resolve().relative_to(self.root_path.resolve())
            artifact_name = rel_path.as_posix()
        except Exception:
            artifact_name = file_path.name

        artifact_name = artifact_name.replace("/", "__")
        artifact_name = re.sub(r"[^A-Za-z0-9._-]", "_", artifact_name)
        return artifact_name

    def _is_ignored(self, file_path: Path) -> bool:
        if file_path.name in self.ignored:
            return True

        return any(part in self.ignored for part in file_path.parts)

    def _find_all_files_in_folder(self, root_path: Path, ignored: list[str]) -> None:
        """Search all files under the root folder recursively.

        Count them at the same time
        """
        for file_path in root_path.iterdir():
            if file_path.is_file() and file_path.name not in ignored:
                self.total_documents += 1
                self._files_under_root_folder.append(file_path)
            elif file_path.is_dir() and file_path.name not in ignored:
                self._find_all_files_in_folder(file_path, ignored)

    def ingest_folder(self, folder_path: Path, ignored: list[str]) -> None:
        # Count total documents before ingestion
        self._files_under_root_folder = []
        self.total_documents = 0
        self._find_all_files_in_folder(folder_path, ignored)
        self._ingest_all(self._files_under_root_folder)

    def _ingest_all(self, files_to_ingest: list[Path]) -> None:
        logger.info("Ingesting files=%s", [f.name for f in files_to_ingest])
        for path in files_to_ingest:
            self._do_ingest_one(path)

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        url = f"{self.base_url}{path}"
        headers = dict(self._headers)
        if payload is not None:
            headers["Content-Type"] = "application/json"
        response = requests.request(
            method,
            url,
            headers=headers,
            json=payload,
            timeout=120,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = response.text.strip()
            msg = f"{exc} | response_body={body[:1000]}"
            raise requests.HTTPError(msg, response=response) from exc
        if not response.text:
            return None
        return response.json()

    def _load_supported_extensions(self) -> set[str] | None:
        if self._supported_extensions_loaded:
            return self._supported_extensions

        self._supported_extensions_loaded = True
        try:
            response = self._request("GET", "/v1/artifacts/readers")
            if not isinstance(response, dict):
                return None

            data = response.get("data", {})
            extensions: set[str] = set()
            if isinstance(data, dict):
                for reader_info in data.values():
                    reader_extensions = (
                        reader_info.get("extensions", [])
                        if isinstance(reader_info, dict)
                        else []
                    )
                    for ext in reader_extensions:
                        if isinstance(ext, str) and ext.startswith("."):
                            extensions.add(ext.lower())

            if extensions:
                self._supported_extensions = extensions
                logger.info(
                    "Loaded %d supported extensions from /v1/artifacts/readers",
                    len(extensions),
                )
        except Exception:
            logger.warning(
                "Unable to fetch supported extensions from server; continuing without extension pre-check"
            )

        return self._supported_extensions

    def _is_supported_file_for_ingest(self, changed_path: Path) -> bool:
        ext = changed_path.suffix.lower()
        if not ext:
            logger.info("Skipping file with no extension: %s", changed_path)
            return False

        supported_extensions = self._load_supported_extensions()
        if supported_extensions is None:
            return True

        if ext not in supported_extensions:
            logger.info(
                "Skipping unsupported file extension '%s' for path=%s",
                ext,
                changed_path,
            )
            return False

        return True

    def ingest_on_watch_event(self, event_type: str, changed_path: Path) -> None:
        if self._is_ignored(changed_path):
            logger.debug("Skipping ignored path=%s", changed_path)
            return

        if changed_path.is_dir():
            logger.debug("Skipping directory event=%s for path=%s", event_type, changed_path)
            return

        logger.info("Detected %s at path=%s", event_type, changed_path)

        if event_type == "deleted":
            self._cancel_debounce_timer(changed_path)
            self._delete_one(changed_path)
            return

        self._schedule_debounced_ingest(changed_path)

    def _cancel_debounce_timer(self, changed_path: Path) -> None:
        with self._timers_lock:
            timer = self._timers.pop(changed_path, None)
            if timer:
                timer.cancel()

    def _schedule_debounced_ingest(self, changed_path: Path) -> None:
        self._cancel_debounce_timer(changed_path)

        timer = threading.Timer(
            self.debounce_ms / 1000,
            self._do_ingest_one,
            args=(changed_path,),
        )
        timer.daemon = True

        with self._timers_lock:
            self._timers[changed_path] = timer
        timer.start()

    def _do_ingest_one(self, changed_path: Path) -> None:
        self._cancel_debounce_timer(changed_path)
        try:
            if changed_path.exists():
                if not self._is_supported_file_for_ingest(changed_path):
                    return
                logger.info(f"Started ingesting file={changed_path}")
                file_b64 = base64.b64encode(changed_path.read_bytes()).decode("ascii")
                payload = {
                    "artifact": self._artifact_from_path(changed_path),
                    "collection": self.collection,
                    "input": {"type": "file", "value": file_b64},
                    "metadata": {"file_name": str(changed_path.name)},
                }
                self._request(
                    method="POST",
                    path="/v1/artifacts/ingest",
                    payload=payload,
                )
                logger.info(f"Completed ingesting file={changed_path}")
        except Exception:
            logger.exception(
                f"Failed to ingest document: {changed_path}, find the exception attached"
            )

    def _delete_one(self, deleted_path: Path) -> None:
        artifact = self._artifact_from_path(deleted_path)
        try:
            logger.info(
                "Deleting artifact for removed file path=%s artifact=%s",
                deleted_path,
                artifact,
            )
            self._request(
                method="POST",
                path="/v1/artifacts/delete",
                payload={"collection": self.collection, "artifact": artifact},
            )
            logger.info("Completed delete sync for artifact=%s", artifact)
        except Exception:
            logger.exception(
                "Failed to delete artifact during sync for path=%s artifact=%s",
                deleted_path,
                artifact,
            )


parser = argparse.ArgumentParser(prog="ingest_folder.py")
parser.add_argument("folder", help="Folder to ingest")
parser.add_argument(
    "--collection",
    help="Collection to ingest into (must match UI collection to appear in Documents).",
    type=str,
    default="pgpt_collection",
)
parser.add_argument(
    "--debounce-ms",
    help="Wait time before ingesting a changed file (helps avoid partial-file ingestion while copying).",
    type=int,
    default=1200,
)
parser.add_argument(
    "--base-url",
    help="PrivateGPT API base URL.",
    type=str,
    default="http://127.0.0.1:8080",
)
parser.add_argument(
    "--username",
    help="Optional HTTP Basic username for PrivateGPT API.",
    type=str,
    default=None,
)
parser.add_argument(
    "--password",
    help="Optional HTTP Basic password for PrivateGPT API.",
    type=str,
    default=None,
)
parser.add_argument(
    "--watch",
    help="Watch for changes",
    action=argparse.BooleanOptionalAction,
    default=False,
)
parser.add_argument(
    "--ignored",
    nargs="*",
    help="List of files/directories to ignore",
    default=[],
)
parser.add_argument(
    "--log-file",
    help="Optional path to a log file. If provided, logs will be written to this file.",
    type=str,
    default=None,
)

args = parser.parse_args()

# Set up logging to a file if a path is provided
if args.log_file:
    file_handler = logging.FileHandler(args.log_file, mode="a")
    file_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(file_handler)

if __name__ == "__main__":
    root_path = Path(args.folder)
    if not root_path.exists():
        raise ValueError(f"Path {args.folder} does not exist")

    worker = ApiIngestWorker(
        root_path=root_path,
        collection=args.collection,
        debounce_ms=args.debounce_ms,
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        ignored=args.ignored,
    )
    worker.ingest_folder(root_path, args.ignored)

    if args.ignored:
        logger.info(f"Skipping following files and directories: {args.ignored}")

    if args.watch:
        logger.info(f"Watching {args.folder} for changes, press Ctrl+C to stop...")
        watcher = IngestWatcher(root_path, worker.ingest_on_watch_event)
        watcher.start()
