import contextlib
import asyncio
import logging
import os
from typing import Any

from qdrant_client import (  # type: ignore[import-not-found]
    AsyncQdrantClient,
    QdrantClient,
)

from private_gpt.settings.settings import Settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class QdrantClients:
    client: QdrantClient
    aclient: Any

    def __init__(self, client: QdrantClient, aclient: Any) -> None:
        self.client = client
        self.aclient = aclient

    def close(self) -> None:
        """Close the sync and async clients."""
        if not self:
            return

        if self.client and hasattr(self.client, "close") and self.client:
            self.client.close()

        logger.debug("Closing Qdrant clients")
        if self.client:
            del self.client
        if self.aclient:
            del self.aclient

    async def aclose(self) -> None:
        """Close the async client."""
        if not self:
            return

        if hasattr(self.aclient, "close"):
            await self.aclient.close()

        logger.debug("Closing async Qdrant client")
        if self.aclient:
            del self.aclient

    def __del__(self) -> None:
        """Ensure the clients are closed when the instance is deleted."""
        with contextlib.suppress(Exception):
            self.close()
            del self


class QdrantClientBuilder:
    @staticmethod
    def build_clients(settings: Settings) -> QdrantClients:
        if settings.qdrant is None:
            client = QdrantClient()
            aclient = AsyncQdrantClient()

            return QdrantClients(
                client=client,
                aclient=aclient,
            )

        config = settings.qdrant.get_parameters(QdrantClient, exclude_none=True)

        if QdrantClientBuilder.is_local_path(config):
            from private_gpt.paths import resolve_data_path

            config = dict(config)
            config["path"] = str(resolve_data_path(config["path"]))
            client = QdrantClient(**config)
            # Qdrant local mode uses a filesystem lock; opening an additional
            # AsyncQdrantClient on the same path can fail on Windows.
            # Reuse the sync client via an async adapter in local mode.
            aclient = _AsyncQdrantLocalAdapter(client)

        else:
            client = QdrantClient(**config)
            aclient = AsyncQdrantClient(**config)

        return QdrantClients(
            client=client,
            aclient=aclient,
        )

    @staticmethod
    def is_local_path(config: dict[str, Any]) -> bool:
        return config.get("path") is not None

    @staticmethod
    def clean_lock(settings: Settings) -> None:
        db_dir = settings.qdrant.path
        if db_dir is None:
            return

        lock_file = os.path.join(db_dir, ".lock")
        if os.path.exists(lock_file):
            os.remove(lock_file)


class _AsyncQdrantLocalAdapter:
    """Minimal async adapter that delegates QdrantLocal calls to a sync client."""

    def __init__(self, client: QdrantClient) -> None:
        self._client = client

    async def scroll(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._client.scroll, *args, **kwargs)

    async def query_batch_points(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._client.query_batch_points, *args, **kwargs)

    async def query_points(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._client.query_points, *args, **kwargs)

    async def close(self) -> None:
        if hasattr(self._client, "close"):
            await asyncio.to_thread(self._client.close)
