from uuid import UUID
from typing import Optional

from cognee.infrastructure.databases.graph import get_graph_config
from cognee.infrastructure.databases.graph.get_graph_engine import (
    create_graph_engine,
    graph_engine_cache,
)
from cognee.infrastructure.databases.vector import get_vectordb_config
from cognee.infrastructure.databases.vector.create_vector_engine import (
    create_vector_engine,
    vector_engine_cache,
)
from cognee.modules.users.models import DatasetDatabase, User


FALKOR_DATASET_DATABASE_HANDLER = "falkor"
FALKOR_PROVIDER_ALIASES = {"falkor", "falkordb"}


class FalkorDatasetDatabaseHandler:
    """Dataset handler for community Falkor hybrid adapter."""

    @classmethod
    async def create_dataset(cls, dataset_id: Optional[UUID], user: Optional[User]) -> dict:
        graph_config = get_graph_config()
        vector_config = get_vectordb_config()
        graph_provider = graph_config.graph_database_provider.lower()
        vector_provider = vector_config.vector_db_provider.lower()

        if graph_provider not in FALKOR_PROVIDER_ALIASES:
            raise ValueError(
                "FalkorDatasetDatabaseHandler can only be used with Falkor graph provider."
            )
        if vector_provider not in FALKOR_PROVIDER_ALIASES:
            raise ValueError(
                "FalkorDatasetDatabaseHandler can only be used with Falkor vector provider."
            )
        if dataset_id is None:
            raise ValueError(
                "dataset_id is required to create a Falkor dataset database mapping."
            )

        # Dataset scope is represented by Falkor logical database name.
        dataset_db_name = str(dataset_id)
        
        await cls._initialize_graph_dataset(
            dataset_db_name=dataset_db_name,
            graph_url=graph_config.graph_database_url,
            graph_port=graph_config.graph_database_port,
            graph_key=graph_config.graph_database_key,
            graph_username=graph_config.graph_database_username,
            graph_password=graph_config.graph_database_password,
        )

        return {
            "graph_database_provider": "falkor",
            "graph_database_url": graph_config.graph_database_url,
            "graph_database_name": dataset_db_name,
            "graph_database_key": graph_config.graph_database_key,
            "graph_dataset_database_handler": FALKOR_DATASET_DATABASE_HANDLER,
            "graph_database_connection_info": {
                "graph_database_port": graph_config.graph_database_port,
            },
            "vector_database_provider": "falkor",
            "vector_database_url": vector_config.vector_db_url,
            "vector_database_name": dataset_db_name,
            "vector_database_key": vector_config.vector_db_key,
            "vector_dataset_database_handler": FALKOR_DATASET_DATABASE_HANDLER,
            "vector_database_connection_info": {
                "port": vector_config.vector_db_port,
            },
        }

    @classmethod
    async def _initialize_graph_dataset(
        cls,
        dataset_db_name: str,
        graph_url: str,
        graph_port: int,
        graph_key: str,
        graph_username: str,
        graph_password: str,
    ) -> None:
        """Ensure Falkor graph key exists for the dataset database name.

        The community Falkor adapter may call index discovery commands through
        GRAPH.RO_QUERY. On a non-existent graph key Falkor raises:
        "Invalid graph operation on empty key". We create a tiny marker node
        once at dataset provisioning time so first pipeline writes are stable.
        """
        graph_engine = create_graph_engine(
            graph_database_provider="falkor",
            graph_file_path="",
            graph_database_url=graph_url,
            graph_database_name=dataset_db_name,
            graph_database_username=graph_username,
            graph_database_password=graph_password,
            graph_database_port=graph_port,
            graph_database_key=graph_key,
            graph_dataset_database_handler=FALKOR_DATASET_DATABASE_HANDLER,
        )
        await graph_engine.query(
            "MERGE (n:__CogneeDatasetMarker {id:'dataset_root'}) RETURN n",
            {},
        )

    @classmethod
    async def resolve_dataset_connection_info(
        cls, dataset_database: DatasetDatabase
    ) -> DatasetDatabase:
        # Runtime credentials are resolved from live config to avoid persisting secrets.
        graph_config = get_graph_config()
        vector_config = get_vectordb_config()

        dataset_database.graph_database_connection_info["graph_database_username"] = (
            graph_config.graph_database_username
        )
        dataset_database.graph_database_connection_info["graph_database_password"] = (
            graph_config.graph_database_password
        )
        dataset_database.graph_database_connection_info["graph_database_host"] = (
            graph_config.graph_database_host
        )
        dataset_database.graph_database_connection_info["graph_database_allow_anonymous"] = (
            graph_config.graph_database_allow_anonymous
        )
        dataset_database.graph_database_connection_info["graph_database_port"] = (
            graph_config.graph_database_port
        )

        dataset_database.vector_database_connection_info["username"] = vector_config.vector_db_username
        dataset_database.vector_database_connection_info["password"] = vector_config.vector_db_password
        dataset_database.vector_database_connection_info["host"] = vector_config.vector_db_host
        dataset_database.vector_database_connection_info["port"] = vector_config.vector_db_port

        return dataset_database

    @classmethod
    async def delete_dataset(cls, dataset_database: DatasetDatabase) -> None:
        dataset_database = await cls.resolve_dataset_connection_info(dataset_database)

        graph_info = dataset_database.graph_database_connection_info or {}
        vector_info = dataset_database.vector_database_connection_info or {}

        await graph_engine_cache.aevict_for_database(dataset_database.graph_database_name)
        await vector_engine_cache.aevict_for_database(dataset_database.vector_database_name)

        # Best-effort cleanup for adapters that support pruning the scoped dataset DB.
        graph_engine = create_graph_engine(
            graph_database_provider=dataset_database.graph_database_provider,
            graph_file_path="",
            graph_database_url=dataset_database.graph_database_url,
            graph_database_name=dataset_database.graph_database_name,
            graph_database_username=graph_info.get("graph_database_username", ""),
            graph_database_password=graph_info.get("graph_database_password", ""),
            graph_database_host=graph_info.get("graph_database_host", ""),
            graph_database_allow_anonymous=graph_info.get("graph_database_allow_anonymous", False),
            graph_database_port=graph_info.get("graph_database_port", ""),
            graph_database_key=dataset_database.graph_database_key,
            graph_dataset_database_handler=FALKOR_DATASET_DATABASE_HANDLER,
        )
        if hasattr(graph_engine, "prune"):
            await graph_engine.prune()

        vector_engine = create_vector_engine(
            vector_db_provider=dataset_database.vector_database_provider,
            vector_db_url=dataset_database.vector_database_url,
            vector_db_name=dataset_database.vector_database_name,
            vector_db_port=vector_info.get("port", ""),
            vector_db_key=dataset_database.vector_database_key,
            vector_dataset_database_handler=FALKOR_DATASET_DATABASE_HANDLER,
            vector_db_username=vector_info.get("username", ""),
            vector_db_password=vector_info.get("password", ""),
            vector_db_host=vector_info.get("host", ""),
        )
        if hasattr(vector_engine, "prune"):
            await vector_engine.prune()
