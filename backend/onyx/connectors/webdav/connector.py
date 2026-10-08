# WebDAV connector (RFC 4918). Tested against Nextcloud.
#
# Config (one set per connector instance, from the admin UI):
#     base_url      WebDAV root of one user, e.g.
#                   https://cloud.example.com/remote.php/dav/files/<user>
#     folder_paths  folders under base_url to index, e.g. ["/Projects", "/HR/Policy"]
#     recursive     also index subfolders (default True)
#     max_depth     subfolder levels to index below each folder (default: no limit)
#
# Credentials:
#     webdav_username, webdav_password. For Nextcloud, use an app password.
#
# Interfaces:
#     LoadConnector   full index of all files in the folders
#     PollConnector   only the files changed in [start, end] (see WebDAVEntry.changed_at),
#                     plus the files of the folders changed in [start, end] (renames, moves)
#     SlimConnector   only the document ids, for pruning of deleted files

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any
from urllib.parse import quote, unquote, urlparse
from xml.etree.ElementTree import Element

import requests
from defusedxml import ElementTree as DefusedET

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import (
    ConnectorValidationError,
    CredentialInvalidError,
    InsufficientPermissionsError,
)
from onyx.connectors.interfaces import (
    GenerateDocumentsOutput,
    GenerateSlimDocumentOutput,
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
    SlimConnector,
)
from onyx.connectors.models import (
    ConnectorMissingCredentialError,
    Document,
    HierarchyNode,
    SlimDocument,
    TextSection,
)
from onyx.connectors.webdav.config import DEFAULT_WEBDAV_MAX_FILE_SIZE_BYTES
from onyx.file_processing.extract_file_text import (
    extract_file_text,
    get_file_ext,
    is_macos_resource_fork_file,
)
from onyx.indexing.indexing_heartbeat import IndexingHeartbeatInterface
from onyx.utils.logger import setup_logger

logger = setup_logger()

# XML namespaces of the PROPFIND response. "oc" and "nc" hold the
# ownCloud/Nextcloud properties. Other servers ignore them and return them with
# a 404 status.
DAV_NS = "DAV:"
OC_NS = "http://owncloud.org/ns"
NC_NS = "http://nextcloud.org/ns"
NS = {"d": DAV_NS, "oc": OC_NS, "nc": NC_NS}

# Request only the properties that the connector uses.
PROPFIND_BODY = f"""<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="{DAV_NS}" xmlns:oc="{OC_NS}" xmlns:nc="{NC_NS}">
  <d:prop>
    <d:resourcetype/>
    <d:getlastmodified/>
    <d:getcontentlength/>
    <d:getcontenttype/>
    <d:getetag/>
    <oc:fileid/>
    <nc:upload_time/>
  </d:prop>
</d:propfind>"""

REQUEST_TIMEOUT = 60
LOCAL_HOSTNAMES = {"localhost", "127.0.0.1", "::1"}
NEXTCLOUD_DAV_ROOT = "/remote.php/dav"
NEXTCLOUD_URL_EXAMPLE = "for Nextcloud https://<host>/remote.php/dav/files/<username>"

# Binary formats without useful text. The connector skips them before the
# download, so it does not transfer large media files.
SKIPPED_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".svg", ".heic",
    ".mp3", ".wav", ".m4a", ".mp4", ".mov", ".avi", ".mkv",
    ".zip", ".tar", ".gz", ".7z", ".rar", ".exe", ".dmg", ".iso",
}  # fmt: skip


@dataclass
class WebDAVEntry:  # One file or folder from a PROPFIND response.
    href: str  # server-relative, url-encoded path, as returned by the server
    is_dir: bool
    last_modified: datetime | None
    size: int | None
    file_id: str | None  # Nextcloud "oc:fileid", None on other servers
    upload_time: datetime | None  # Nextcloud "nc:upload_time", None elsewhere

    @property
    def name(self) -> str:
        return unquote(self.href.rstrip("/").rsplit("/", 1)[-1])

    @property
    def changed_at(self) -> (datetime | None):  # Last time the file changed on the server. Nextcloud keeps the original modification time of an uploaded file in getlastmodified, so a file uploaded today can show a date from months ago. upload_time is the time the server received it. The later of the two dates catches both edits and new uploads.
        dates = [d for d in (self.last_modified, self.upload_time) if d is not None]
        return max(dates) if dates else None


def normalize_folder(path: str,) -> str:  # Return one canonical form, e.g. 'Projects/Clients/' -> '/Projects/Clients'.
    parts = [p for p in path.strip().split("/") if p]
    return "/" + "/".join(parts)


def describe_connection_error(url: str, error: requests.RequestException) -> str:  # Turn a transport error into a message that tells the admin what to check. The order matters: SSLError and ConnectTimeout are also ConnectionErrors.
    parsed = urlparse(url)
    server = parsed.netloc or url

    if isinstance(error, requests.exceptions.SSLError):
        return (
            f"WebDAV: TLS/SSL error with {server}. Check http:// or https:// in "
            "the URL: the server may not use HTTPS on this port, or its "
            "certificate is not valid."
        )
    if isinstance(error, requests.exceptions.Timeout):
        return (
            f"WebDAV: {server} did not answer within {REQUEST_TIMEOUT} seconds. "
            "Check that the server is running and reachable from Onyx."
        )
    if isinstance(error, requests.exceptions.ConnectionError):
        message = (
            f"WebDAV: cannot connect to {server}. Check the host name and the "
            "port in the URL, and that the server is running."
        )
        # Onyx runs in a container, where localhost is the container itself.
        if parsed.hostname in LOCAL_HOSTNAMES:
            message += (
                " Onyx runs in a container, so localhost does not point to "
                "your machine: use the server host name (with Docker Desktop, "
                "host.docker.internal for a server on the same machine)."
            )
        return message
    return f"WebDAV: the request to {server} failed ({type(error).__name__})."


class WebDAVConnector(LoadConnector, PollConnector, SlimConnector):  # Indexes the files of one or more folders of a WebDAV server.
    def __init__(
        self,
        base_url: str,
        folder_paths: list[str],
        recursive: bool = True,
        max_depth: int | None = None,
        max_file_size_bytes: int = DEFAULT_WEBDAV_MAX_FILE_SIZE_BYTES,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.base_url = base_url.rstrip("/")

        # A set removes duplicate folders. Sorting gives a stable order.
        # An empty list means the full base_url.
        self.folder_paths = sorted({normalize_folder(p) for p in folder_paths}) or ["/"]
        self.recursive = recursive

        # None: no limit. 1: only the direct subfolders of each folder.
        self.max_depth = max_depth
        self.max_file_size_bytes = max_file_size_bytes
        self.batch_size = batch_size

        # The server returns hrefs as absolute paths without host, e.g.
        # /remote.php/dav/files/luca/a.pdf. These two parts rebuild full URLs
        # and paths relative to base_url.
        parsed = urlparse(self.base_url)
        self._origin = f"{parsed.scheme}://{parsed.netloc}"
        self._base_path = parsed.path.rstrip("/")
        self._session: requests.Session | None = None

    # ----------------------------------------------------------------- auth

    def load_credentials(self, credentials: dict[str, Any]) -> (dict[str, Any] | None):  # Create an HTTP session with Basic Auth. Onyx calls this before indexing.
        username = credentials.get("webdav_username")
        password = credentials.get("webdav_password")

        if not username or not password:
            raise ConnectorMissingCredentialError("WebDAV: the credential must contain webdav_username and webdav_password")

        # One session for all requests: it keeps the TCP connection open.
        session = requests.Session()
        session.auth = (username, password)
        session.headers["User-Agent"] = "Onyx-WebDAV-Connector"
        self._session = session

        return None

    @property
    def session(self) -> (requests.Session):  # The authenticated session. Fails if Onyx did not call load_credentials.
        if self._session is None:
            raise ConnectorMissingCredentialError("WebDAV")
        return self._session

    # ----------------------------------------------------------------- http

    def folder_url(self, folder: str) -> str:  # Full URL of a folder. quote() encodes spaces and accents.
        return self.base_url + quote(folder) + "/"

    def href_to_url(self, href: str) -> (str):  # Absolute URL from a server href (servers often return a path without host).
        return href if href.startswith("http") else self._origin + href

    def href_to_folder_path(self, href: str) -> str:  # Server href -> path relative to base_url, e.g. '/Projects/a.pdf'.
        path = unquote(urlparse(href).path)
        base = unquote(self._base_path)
        rel = path.removeprefix(base)

        return "/" + rel.strip("/")

    def raise_for_status(self, resp: requests.Response, what: str) -> None:  # Map HTTP errors to the Onyx connector exceptions. Onyx shows these messages to the admin, and uses the exception type to mark the credential as invalid (401) or without access (403).
        if resp.status_code == 401:
            raise CredentialInvalidError(
                "WebDAV: the server rejected the username or the password "
                "(HTTP 401). If the account uses two-factor authentication, "
                "use an app password."
            )
        if resp.status_code == 403:
            raise InsufficientPermissionsError(
                f"WebDAV: access to {what} is denied (HTTP 403). The user has "
                "no permission to read it."
            )
        if resp.status_code == 404:
            raise ConnectorValidationError(f"WebDAV: {what} not found (HTTP 404).")

        if resp.status_code == 405:
            raise ConnectorValidationError(
                f"WebDAV: {what} does not accept WebDAV requests (HTTP 405). "
                f"The URL must point to a WebDAV folder, {NEXTCLOUD_URL_EXAMPLE}."
            )

        if resp.status_code >= 400:
            raise ConnectorValidationError(f"WebDAV: unexpected HTTP {resp.status_code} on {what}.")

    def send(self, method: str, url: str, headers: dict[str, str] | None = None) -> (requests.Response):  # Send one request. Transport errors become clear validation errors.
        try:
            return self.session.request(
                method,
                url,
                data=PROPFIND_BODY.encode() if method == "PROPFIND" else None,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            raise ConnectorValidationError(describe_connection_error(url, e)) from e

    def propfind(self, url: str, depth: str, what: str) -> list[WebDAVEntry]:  # Send a PROPFIND request. depth "0" returns only the resource at url. depth "1" also returns its direct children. The connector never uses "infinity": many servers block it, and on large trees it returns a very large response.
        resp = self.send(
            "PROPFIND",
            url,
            headers={"Depth": depth, "Content-Type": "application/xml; charset=utf-8"},
        )
        self.raise_for_status(resp, what)
        return self.parse_multistatus(resp.content)

    @staticmethod
    def parse_multistatus(xml_bytes: bytes,) -> list[WebDAVEntry]:  # Parse a 207 Multi-Status body into entries. The body comes from an external server, so defusedxml parses it. This blocks XML attacks such as entity expansion ("billion laughs").
        entries: list[WebDAVEntry] = []
        root = DefusedET.fromstring(xml_bytes)

        for response in root.findall("d:response", NS):
            href = response.findtext("d:href", default="", namespaces=NS)

            # A response can have more than one propstat block: one with
            # status 200 for the found properties, one with status 404 for the
            # properties that the server does not support. Keep only the 200 ones.
            props: dict[str, Element] = {}

            for propstat in response.findall("d:propstat", NS):
                status = propstat.findtext("d:status", default="", namespaces=NS)

                if " 200 " not in f"{status} ":
                    continue

                prop = propstat.find("d:prop", NS)

                if prop is not None:
                    for child in prop:
                        props[child.tag] = child

            # A folder has <d:resourcetype><d:collection/></d:resourcetype>.
            restype = props.get(f"{{{DAV_NS}}}resourcetype")
            is_dir = (restype is not None and restype.find("d:collection", NS) is not None)

            # getlastmodified uses the HTTP date format (RFC 1123).
            last_modified = None
            lm = props.get(f"{{{DAV_NS}}}getlastmodified")

            if lm is not None and lm.text:
                try:
                    last_modified = parsedate_to_datetime(lm.text).astimezone(timezone.utc)
                except (TypeError, ValueError):
                    logger.warning("WebDAV: getlastmodified is not valid: %s", lm.text)

            size = None
            cl = props.get(f"{{{DAV_NS}}}getcontentlength")

            if cl is not None and cl.text and cl.text.isdigit():
                size = int(cl.text)

            fid = props.get(f"{{{OC_NS}}}fileid")
            file_id = fid.text if fid is not None and fid.text else None

            # Unix seconds. Nextcloud sends 0 when it does not know the time.
            upload_time = None
            ut = props.get(f"{{{NC_NS}}}upload_time")
            if ut is not None and ut.text and ut.text.isdigit() and int(ut.text) > 0:
                upload_time = datetime.fromtimestamp(int(ut.text), tz=timezone.utc)

            entries.append(WebDAVEntry(href, is_dir, last_modified, size, file_id, upload_time))
        return entries

    # ------------------------------------------------------------ traversal

    def iter_files(self) -> Iterator[tuple[WebDAVEntry, WebDAVEntry | None]]:  # Yield (file, parent folder) for every file in the configured folders. The walk uses a stack and PROPFIND depth 1 on each folder, so it reads one folder level for each request. The parent comes from the same response: depth 1 also returns the folder itself.

        # File paths already yielded. Overlapping folders (e.g. /A and /A/B)
        # list the same files more than one time.
        seen: set[str] = set()

        for folder in self.folder_paths: # Each item is (folder URL, level below the configured folder).
            stack = [(self.folder_url(folder), 0)]
            visited_dirs: set[str] = set()

            while stack:
                url, level = stack.pop()
                entries = self.propfind(url, "1", f"folder {folder}")
                self_path = urlparse(url).path.rstrip("/")
                parent = next((e for e in entries if unquote(urlparse(e.href).path.rstrip("/")) == unquote(self_path)), None)

                for entry in entries:
                    entry_path = urlparse(entry.href).path.rstrip("/")

                    # Depth 1 also returns the folder itself: skip it.
                    if entry is parent:
                        continue

                    if entry.is_dir:# visited_dirs prevents loops if a server lists a folder twice.
                        if (self.should_descend(level + 1)and entry_path not in visited_dirs):
                            visited_dirs.add(entry_path)
                            stack.append((self.href_to_url(entry.href.rstrip("/") + "/"), level + 1,))
                        continue

                    if entry_path in seen:
                        continue
                    seen.add(entry_path)
                    yield entry, parent

    def should_descend(self, level: int) -> (bool):  # True if the walk can enter a subfolder at this level (1 = direct child).
        if not self.recursive:
            return False
        return self.max_depth is None or level <= self.max_depth

    def doc_id(self, entry: WebDAVEntry) -> str:  # Return a stable document id. The id must not change between runs, or Onyx indexes the same file two times. The Nextcloud fileid stays the same after a rename or a move. Other servers do not have it, so the id is the full URL.
        if entry.file_id:
            return f"webdav:{self._origin}:fileid:{entry.file_id}"
        return f"webdav:{self.href_to_url(entry.href)}"

    def should_index(self, entry: WebDAVEntry) -> bool:  # Filter on metadata only, before the download. The slim pass uses the same filter, so pruning keeps exactly the documents that the main pass indexes.
        name = entry.name

        # Hidden files (".DS_Store", ".nextcloudsync.log") and macOS "._" files.
        if name.startswith(".") or is_macos_resource_fork_file(name):
            return False
        if get_file_ext(name) in SKIPPED_EXTENSIONS:
            return False

        if entry.size is not None and entry.size > self.max_file_size_bytes:
            logger.info("WebDAV: skip %s, its size (%s bytes) is larger than the limit", name, entry.size,)
            return False
        return True

    def to_document(self, entry: WebDAVEntry) -> (Document | None):  # Download one file and convert it to an Onyx Document. Returns None if the file is gone or has no text.
        url = self.href_to_url(entry.href)
        resp = self.send("GET", url)

        # The file was deleted after the listing. Pruning removes it later.
        if resp.status_code == 404:
            return None
        self.raise_for_status(resp, f"file {entry.name}")

        # Onyx's shared extractor: PDF, Office, HTML, Markdown, plain text, etc.
        text = extract_file_text(BytesIO(resp.content), file_name=entry.name, break_on_unprocessable=False)

        if not text.strip():
            logger.info("WebDAV: skip %s, no text extracted", entry.name)
            return None

        folder_path = self.href_to_folder_path(entry.href)

        return Document(
            id=self.doc_id(entry),
            source=DocumentSource.WEBDAV,
            semantic_identifier=entry.name,
            sections=[TextSection(link=url, text=text)],
            metadata={
                "path": folder_path,
                "folder": folder_path.rsplit("/", 1)[0] or "/",
            },
            # Onyx skips a document whose content hash did not change. The hash
            # covers doc_metadata but not semantic_identifier, link or metadata,
            # so the path goes here: a renamed or moved file is then updated.
            doc_metadata={"path": folder_path},
            # Polling compares this date with the [start, end] window.
            doc_updated_at=entry.changed_at,
        )

    @staticmethod
    def changed_in_window(entry: WebDAVEntry, start: SecondsSinceUnixEpoch | None, end: SecondsSinceUnixEpoch | None) -> bool:  # True if the entry changed in [start, end]. An entry without any change date counts as changed.
        if entry.changed_at is None:
            return True
        ts = entry.changed_at.timestamp()
        return (start is None or ts >= start) and (end is None or ts <= end)

    def yield_documents(self, start: SecondsSinceUnixEpoch | None, end: SecondsSinceUnixEpoch | None) -> GenerateDocumentsOutput:  # Shared body of the full index and of polling. With start and end set, only the files changed in that window are downloaded (see WebDAVEntry.changed_at), plus all the files of a folder that changed in the window. Documents go out in batches of batch_size.
        batch: list[Document | HierarchyNode] = []

        for entry, parent in self.iter_files():
            if not self.should_index(entry):
                continue
            # A rename or a move keeps the dates of the file, but Nextcloud
            # updates the date of the folders that contain it. So a changed
            # folder sends all its files: Onyx skips the unchanged ones with
            # the content hash, and updates the renamed or moved one. A folder
            # without a date is ignored, else every poll sends all its files.
            folder_changed = (parent is not None and parent.changed_at is not None and self.changed_in_window(parent, start, end))
            if not (self.changed_in_window(entry, start, end) or folder_changed):
                continue
            try:
                doc = self.to_document(entry)
            # Auth errors stop the run: all next files would fail the same way.
            except (CredentialInvalidError, InsufficientPermissionsError):
                raise
            except Exception as e:
                logger.exception("WebDAV: failed to index %s: %s", entry.href, e)
                continue

            if doc is None:
                continue
            batch.append(doc)

            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    # ----------------------------------------------------------- interfaces

    def load_from_state(self) -> GenerateDocumentsOutput:  # Full index: no time window.
        return self.yield_documents(None, None)

    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> (GenerateDocumentsOutput):  # Incremental index: Onyx sends the window since the last good run.
        return self.yield_documents(start, end)

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:  # Return the ids of all indexable files, without download. Onyx deletes from the index the documents that are not in this list. So the time window is ignored: pruning needs the full list. A missing folder raises (404) and does not return an empty list, because an empty list would delete all the documents of this connector.
        batch: list[SlimDocument | HierarchyNode] = []

        for entry, _ in self.iter_files():
            if not self.should_index(entry):
                continue
            batch.append(SlimDocument(id=self.doc_id(entry)))

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

                # Stop early if Onyx cancels the pruning task.
                if callback and callback.should_stop():
                    return
        if batch:
            yield batch

    def validate_connector_settings(self) -> None:  # Check the URL, the credential and each folder before the first run. Onyx calls this when the admin links the credential to the connector, and shows the error message in the UI. The base URL is checked first, so a wrong URL is not reported as a missing folder. Each check is one PROPFIND depth 0, so validation is fast.
        if not self.base_url.startswith(("http://", "https://")):
            raise ConnectorValidationError(
                f"WebDAV: the URL must start with http:// or https:// "
                f"({NEXTCLOUD_URL_EXAMPLE})."
            )

        if self.max_depth is not None and self.max_depth < 1:
            raise ConnectorValidationError(
                "WebDAV: the maximum folder depth must be 1 or more. Leave it "
                "empty for no limit, or clear Include Subfolders to index only "
                "the files of each folder."
            )

        # Nextcloud/ownCloud: /remote.php/dav is the DAV root of all users, not
        # a folder of files. The files live under /remote.php/dav/files/<user>.
        if self._base_path.endswith(NEXTCLOUD_DAV_ROOT):
            raise ConnectorValidationError(
                f"WebDAV: the URL ends at {NEXTCLOUD_DAV_ROOT}. Add /files/<username> "
                f"at the end ({NEXTCLOUD_URL_EXAMPLE})."
            )

        base_url_response = self.send("PROPFIND", self.base_url + "/", headers={"Depth": "0"})

        # A 404 on the base URL usually means a wrong user name at its end:
        # the folders can exist and still be unreachable from this URL.
        if base_url_response.status_code == 404:
            raise ConnectorValidationError(
                f"WebDAV: the URL {self.base_url} was not found (HTTP 404). "
                "Check the path and the user name at the end of the URL "
                f"({NEXTCLOUD_URL_EXAMPLE}, with the login name of the credential)."
            )
        self.raise_for_status(base_url_response, f"the URL {self.base_url}")

        for folder in self.folder_paths:
            folder_response = self.send("PROPFIND", self.folder_url(folder), headers={"Depth": "0"})

            if folder_response.status_code == 404:
                raise ConnectorValidationError(
                    f"WebDAV: folder {folder} not found in {self.base_url} "
                    "(HTTP 404). Check the spelling: folder names are "
                    "case-sensitive."
                )
            self.raise_for_status(folder_response, f"folder {folder}")
            entries = self.parse_multistatus(folder_response.content)

            if not entries:
                raise ConnectorValidationError(f"WebDAV: the server returned no data for folder {folder}.")

            if not entries[0].is_dir:
                raise ConnectorValidationError(f"WebDAV: {folder} is a file, not a folder. Enter the path of a folder.")


if __name__ == "__main__":
    import os
    import time

    from onyx.db.engine.sql_engine import SqlEngine

    # extract_file_text reads settings from Postgres/Redis: run this inside the
    # Onyx network (POSTGRES_HOST/REDIS_HOST pointing at the stack).
    SqlEngine.init_engine(pool_size=2, max_overflow=0)

    connector = WebDAVConnector(
        base_url=os.environ["WEBDAV_BASE_URL"],
        folder_paths=os.environ.get("WEBDAV_FOLDERS", "/").split(","),
    )

    connector.load_credentials(
        {
            "webdav_username": os.environ["WEBDAV_USERNAME"],
            "webdav_password": os.environ["WEBDAV_PASSWORD"],
        }
    )
    connector.validate_connector_settings()
    print("validate_connector_settings: OK")

    print("== load_from_state")
    for docs in connector.load_from_state():
        for doc in docs:
            if isinstance(doc, Document):
                text_length = sum(len(section.text or "") for section in doc.sections)
                print(f"  {doc.semantic_identifier} ({text_length} chars) id={doc.id}")

    print("== poll_source (last hour)")
    now = time.time()

    for docs in connector.poll_source(now - 3600, now):
        for doc in docs:
            if isinstance(doc, Document):
                print(f"  {doc.semantic_identifier}")

    print("== retrieve_all_slim_docs")
    print("  ", sum(len(b) for b in connector.retrieve_all_slim_docs()), "ids")
