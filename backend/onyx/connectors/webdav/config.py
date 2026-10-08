from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.connectors.connector_config import BaseUrlCredentialBinding, ConnectorConfig

DEFAULT_WEBDAV_MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024


class WebDAVConnectorConfig(BaseUrlCredentialBinding, ConnectorConfig):
    folder_paths: list[str]
    recursive: bool = True
    max_depth: int | None = None
    max_file_size_bytes: int = DEFAULT_WEBDAV_MAX_FILE_SIZE_BYTES
    batch_size: int = INDEX_BATCH_SIZE
