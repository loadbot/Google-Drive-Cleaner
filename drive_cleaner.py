"""
drive_cleaner.py
====================
Google Drive duplicate file cleaner.

Usage:
  python drive_cleaner.py               # Dry run (safe, no changes)
  python drive_cleaner.py --reindex     # Re-fetch all metadata from Drive
  python drive_cleaner.py --delete      # Trash duplicates after confirmation
"""

import os
import json
import logging
import sqlite3
import sys
import time
import argparse
from collections import deque
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Set ROOT_FOLDER_ID to one of:
#   'root'                → scan all of My Drive (personal drive)
#   '0AGdvD8JVt...'       → scan a full Shared Drive
#   '1FAYh9uNqbllIcjG...' → scan a specific folder (any depth)
ROOT_FOLDER_ID   = 'YOUR_FOLDER_ID'
SCOPES           = ['https://www.googleapis.com/auth/drive']
DB_NAME          = 'database.db'
LOG_FILE         = 'logs.log'
TOKEN_FILE       = 'token.json'
CREDENTIALS_FILE = 'credentials.json'
PAGE_SIZE        = 1000
LOG_INTERVAL     = 5_000
RATE_LIMIT_CODES = {403, 429}

# How many folder IDs to combine in one files().list() query (OR chain).
# 50 is safe — keeps the query string well under Drive API URL limits.
FOLDER_BATCH_SIZE = 50

# ---------------------------------------------------------------------------
# LOGGING — UTF-8 forced on both handlers (fixes Vietnamese filenames on Windows)
# ---------------------------------------------------------------------------
_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

_file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
_file_handler.setFormatter(_formatter)

_console_handler = logging.StreamHandler(
    open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
)
_console_handler.setFormatter(_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger(__name__)

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------
def load_credentials() -> Optional[Credentials]:
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, 'r') as f:
        data = json.load(f)
    return Credentials.from_authorized_user_info(data, SCOPES)


def save_credentials(creds: Credentials) -> None:
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except AttributeError:
        pass  # Windows — skip chmod


def authenticate() -> Credentials:
    creds = load_credentials()
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        log.info("Refreshing expired credentials...")
        creds.refresh(Request())
        save_credentials(creds)
        return creds
    log.info("No valid credentials found — starting OAuth flow...")
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)
    save_credentials(creds)
    return creds


def get_service():
    return build('drive', 'v3', credentials=authenticate())


# ---------------------------------------------------------------------------
# AUTO-DETECT: Shared Drive or regular folder?
# ---------------------------------------------------------------------------
def detect_target(service, folder_id: str) -> dict:
    """
    Determine what ROOT_FOLDER_ID points to and return a config dict:
      - 'root'          → My Drive (full personal drive)
      - Shared Drive ID → entire Shared Drive
      - Folder ID       → that folder + all subfolders recursively
    """
    # Handle 'root' — the special alias for My Drive
    if folder_id == 'root':
        log.info("Target: My Drive (root)")
        return {
            'type':      'folder',
            'label':     'My Drive',
            'folder_id': 'root',
            'list_kwargs': dict(
                corpora='user',
                includeItemsFromAllDrives=False,
                supportsAllDrives=False,
            ),
        }

    # Try as a Shared Drive first
    try:
        drive = service.drives().get(driveId=folder_id).execute()
        log.info(f"Detected Shared Drive: '{drive['name']}' [{folder_id}]")
        return {
            'type':      'shared_drive',
            'label':     drive['name'],
            'folder_id': None,  # not needed — driveId scopes everything
            'list_kwargs': dict(
                driveId=folder_id,
                corpora='drive',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ),
        }
    except HttpError as e:
        if e.resp.status not in (404, 403):
            raise

    # Try as a regular folder
    try:
        folder = service.files().get(
            fileId=folder_id,
            fields='id, name, mimeType',
            supportsAllDrives=True,
        ).execute()

        if folder.get('mimeType') != 'application/vnd.google-apps.folder':
            raise ValueError(
                f"ID '{folder_id}' is not a folder (mimeType: {folder.get('mimeType')})"
            )

        log.info(f"Detected regular folder: '{folder['name']}' [{folder_id}]")
        return {
            'type':      'folder',
            'label':     folder['name'],
            'folder_id': folder_id,
            'list_kwargs': dict(
                corpora='allDrives',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ),
        }
    except HttpError as e:
        raise RuntimeError(
            f"Could not resolve ROOT_FOLDER_ID '{folder_id}'.\n"
            f"  Valid values:\n"
            f"    'root'                      → all of My Drive\n"
            f"    '0AGdvD8JVt...'      → a Shared Drive ID\n"
            f"    '1FAYh9uNqbllIcjG...' → a specific folder ID\n"
            f"  Ensure your account has at least Viewer access to it.\n"
            f"  API error: {e}"
        ) from e


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------
def open_db(reindex: bool) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if reindex:
        log.info("--reindex flag set: dropping existing file index.")
        cur.execute('DROP TABLE IF EXISTS files')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id           TEXT PRIMARY KEY,
            name         TEXT,
            md5          TEXT,
            parent       TEXT,
            created_time TEXT,
            size         INTEGER DEFAULT 0
        )
    ''')

    # Critical: without this index, duplicate detection is a full table scan
    cur.execute('''
        CREATE INDEX IF NOT EXISTS idx_parent_md5
        ON files (parent, md5)
    ''')

    conn.commit()
    return conn


def is_already_indexed(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM files')
    return cur.fetchone()[0] > 0


# ---------------------------------------------------------------------------
# FOLDER BFS — collect every subfolder ID recursively
# ---------------------------------------------------------------------------
def collect_all_folder_ids(service, root_id: str) -> list[str]:
    """
    BFS walk starting from root_id.
    Returns a list of ALL folder IDs in the tree (including root_id itself).

    This is necessary because the Drive API has no native recursive query —
    'X in parents' only matches direct children, not deeper descendants.
    """
    all_ids: list[str] = []
    queue: deque[str] = deque([root_id])
    visited: set[str] = {root_id}

    log.info("Walking folder tree (BFS)... this may take a moment for large trees.")

    while queue:
        parent_id = queue.popleft()
        all_ids.append(parent_id)
        page_token: Optional[str] = None

        while True:
            q = (
                f"'{parent_id}' in parents "
                f"and mimeType = 'application/vnd.google-apps.folder' "
                f"and trashed = false"
            )
            response = _api_call_with_retry(
                service.files().list(
                    q=q,
                    spaces='drive',
                    pageSize=PAGE_SIZE,
                    fields='nextPageToken, files(id)',
                    pageToken=page_token,
                    corpora='allDrives',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                )
            )
            for f in response.get('files', []):
                fid = f['id']
                if fid not in visited:
                    visited.add(fid)
                    queue.append(fid)

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        if len(all_ids) % 500 == 0:
            log.info(f"  Folders discovered so far: {len(all_ids):,}")

    log.info(f"Folder walk complete. Total folders found: {len(all_ids):,}")
    return all_ids


# ---------------------------------------------------------------------------
# INDEXING
# ---------------------------------------------------------------------------
def fetch_and_store_files(service, conn: sqlite3.Connection, target: dict) -> None:
    """
    Index all files into SQLite.

    - Shared Drive: single paginated query scoped by driveId (fast, one pass)
    - Regular folder: BFS walk to collect all folder IDs, then query files
                      in batches of FOLDER_BATCH_SIZE folder IDs at a time
    """
    if target['type'] == 'shared_drive':
        _index_shared_drive(service, conn, target)
    else:
        _index_folder_recursive(service, conn, target)


def _index_shared_drive(service, conn: sqlite3.Connection, target: dict) -> None:
    """Single paginated query — driveId handles all scoping."""
    cur = conn.cursor()
    page_token: Optional[str] = None
    total = 0
    last_logged = 0

    log.info(f"Indexing Shared Drive '{target['label']}'...")
    q = "trashed = false and mimeType != 'application/vnd.google-apps.folder'"

    while True:
        response = _api_call_with_retry(
            service.files().list(
                q=q,
                spaces='drive',
                pageSize=PAGE_SIZE,
                fields='nextPageToken, files(id, name, md5Checksum, parents, createdTime, size)',
                pageToken=page_token,
                **target['list_kwargs'],
            )
        )
        total, last_logged = _store_page(conn, cur, response.get('files', []), total, last_logged)
        page_token = response.get('nextPageToken')
        if not page_token:
            break

    log.info(f"Indexing complete. Total files scanned: {total:,}")


def _index_folder_recursive(service, conn: sqlite3.Connection, target: dict) -> None:
    """
    BFS-walk the folder tree to get all folder IDs, then query files
    for each batch of folder IDs.
    """
    # Step A: collect all folder IDs via BFS
    all_folder_ids = collect_all_folder_ids(service, target['folder_id'])

    # Step B: query files for each batch of folder IDs
    cur = conn.cursor()
    total = 0
    last_logged = 0
    num_batches = (len(all_folder_ids) + FOLDER_BATCH_SIZE - 1) // FOLDER_BATCH_SIZE

    log.info(f"Indexing files across {len(all_folder_ids):,} folders in {num_batches} batch(es)...")

    for batch_num, i in enumerate(range(0, len(all_folder_ids), FOLDER_BATCH_SIZE), start=1):
        batch_ids = all_folder_ids[i : i + FOLDER_BATCH_SIZE]

        # Build: ('id1' in parents or 'id2' in parents or ...)
        parents_clause = ' or '.join(f"'{fid}' in parents" for fid in batch_ids)
        q = (
            f"({parents_clause}) "
            f"and trashed = false "
            f"and mimeType != 'application/vnd.google-apps.folder'"
        )

        page_token: Optional[str] = None
        while True:
            response = _api_call_with_retry(
                service.files().list(
                    q=q,
                    spaces='drive',
                    pageSize=PAGE_SIZE,
                    fields='nextPageToken, files(id, name, md5Checksum, parents, createdTime, size)',
                    pageToken=page_token,
                    **target['list_kwargs'],
                )
            )
            total, last_logged = _store_page(conn, cur, response.get('files', []), total, last_logged)
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        if batch_num % 10 == 0 or batch_num == num_batches:
            log.info(f"  Folder batches processed: {batch_num}/{num_batches}")

    log.info(f"Indexing complete. Total files scanned: {total:,}")


def _store_page(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    files: list,
    total: int,
    last_logged: int,
) -> tuple[int, int]:
    """Insert a page of file metadata into SQLite. Returns updated (total, last_logged)."""
    batch = []
    for f in files:
        if f.get('md5Checksum') and f.get('parents'):
            batch.append((
                f['id'],
                f['name'],
                f['md5Checksum'],
                f['parents'][0],
                f.get('createdTime', ''),
                int(f.get('size', 0)),
            ))
    if batch:
        cur.executemany('INSERT OR IGNORE INTO files VALUES (?,?,?,?,?,?)', batch)
        conn.commit()

    total += len(files)
    if total - last_logged >= LOG_INTERVAL:
        log.info(f"Indexed {total:,} files so far...")
        last_logged = total

    return total, last_logged


# ---------------------------------------------------------------------------
# API HELPER — retry with exponential backoff
# ---------------------------------------------------------------------------
def _api_call_with_retry(request, max_attempts: int = 5) -> dict:
    """Execute a Drive API request with exponential backoff on rate-limit errors."""
    for attempt in range(max_attempts):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status in RATE_LIMIT_CODES:
                wait = 10 * (2 ** attempt)
                log.warning(f"Rate limit (attempt {attempt+1}/{max_attempts}). Retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Max retries exceeded.")


# ---------------------------------------------------------------------------
# DUPLICATE DETECTION
# ---------------------------------------------------------------------------
def find_duplicates(conn: sqlite3.Connection) -> list[tuple]:
    """
    Find duplicates, keeping the earliest-created file per (parent, md5) group.
    Returns list of (id, name, parent, size) tuples to be trashed.
    """
    cur = conn.cursor()
    query = '''
        SELECT id, name, parent, size
        FROM files
        WHERE (parent, md5) IN (
            SELECT parent, md5
            FROM files
            GROUP BY parent, md5
            HAVING COUNT(*) > 1
        )
        AND id NOT IN (
            SELECT id FROM files f1
            WHERE created_time = (
                SELECT MIN(created_time)
                FROM files f2
                WHERE f2.parent = f1.parent AND f2.md5 = f1.md5
            )
        )
        ORDER BY parent, name
    '''
    cur.execute(query)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# DELETE (batch + retry)
# ---------------------------------------------------------------------------
def trash_files(service, duplicates: list[tuple]) -> None:
    """
    Trash duplicates using batch HTTP requests (100 per batch).
    Falls back to sequential with per-file retry on rate-limit errors.
    """
    total   = len(duplicates)
    trashed = 0
    failed: list[str] = []
    chunks  = [duplicates[i:i+100] for i in range(0, total, 100)]

    log.info(f"Trashing {total:,} files in {len(chunks)} batch(es)...")

    for chunk_index, chunk in enumerate(chunks):
        batch = service.new_batch_http_request()

        for file_id, name, parent, size in chunk:
            def make_callback(fid: str, fname: str):
                def callback(request_id, response, exception):
                    nonlocal trashed
                    if exception:
                        log.error(f"Error trashing '{fname}': {exception}")
                        failed.append(fid)
                    else:
                        log.info(f"TRASHED: {fname}")
                        trashed += 1
                return callback

            batch.add(
                service.files().update(
                    fileId=file_id,
                    body={'trashed': True},
                    supportsAllDrives=True,
                ),
                callback=make_callback(file_id, name),
            )

        try:
            batch.execute()
        except HttpError as e:
            if e.resp.status in RATE_LIMIT_CODES:
                wait = 30
                log.warning(f"Rate limit on batch {chunk_index+1}. Waiting {wait}s, retrying sequentially...")
                time.sleep(wait)
                for file_id, name, parent, size in chunk:
                    _trash_single_with_retry(service, file_id, name, failed)
            else:
                raise

        log.info(f"Progress: {trashed:,}/{total:,} trashed | {len(failed)} failed")

    log.info(f"Done. Trashed: {trashed:,} | Failed: {len(failed)}")
    if failed:
        log.warning(f"Failed file IDs: {failed}")


def _trash_single_with_retry(service, file_id: str, name: str, failed: list) -> None:
    for attempt in range(4):
        try:
            service.files().update(
                fileId=file_id,
                body={'trashed': True},
                supportsAllDrives=True,
            ).execute()
            log.info(f"TRASHED: {name}")
            return
        except HttpError as e:
            if e.resp.status in RATE_LIMIT_CODES:
                wait = 10 * (2 ** attempt)
                log.warning(f"Rate limit on '{name}' (attempt {attempt+1}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"Permanent error trashing '{name}': {e}")
                failed.append(file_id)
                return
    log.error(f"Max retries exceeded for '{name}'. Skipping.")
    failed.append(file_id)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def format_size(total_bytes: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if total_bytes < 1024:
            return f"{total_bytes:.1f} {unit}"
        total_bytes /= 1024
    return f"{total_bytes:.1f} PB"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description='Google Drive duplicate cleaner — works with Shared Drives and regular folders',
    )
    parser.add_argument('--reindex', action='store_true', help='Drop and re-fetch all metadata from Drive')
    parser.add_argument('--delete',  action='store_true', help='Trash duplicate files (default: dry run)')
    args = parser.parse_args()

    service = get_service()

    # Auto-detect: Shared Drive or regular folder?
    target = detect_target(service, ROOT_FOLDER_ID)

    conn = open_db(reindex=args.reindex)

    # ── Step 1: Indexing ─────────────────────────────────────────────────────
    if is_already_indexed(conn) and not args.reindex:
        log.info("Existing index found. Skipping re-fetch. Use --reindex to refresh.")
    else:
        fetch_and_store_files(service, conn, target)

    # ── Step 2: Duplicate detection ──────────────────────────────────────────
    log.info("Scanning for duplicates...")
    duplicates = find_duplicates(conn)

    if not duplicates:
        log.info("✅ No duplicates found.")
        conn.close()
        return

    total_bytes = sum(size for _, _, _, size in duplicates)
    log.info(f"Found {len(duplicates):,} duplicate files ({format_size(total_bytes)} recoverable).")

    # ── Step 3: Report / Delete ──────────────────────────────────────────────
    if not args.delete:
        log.info("Dry run — no files will be trashed. Use --delete to proceed.")
        print("\nSample duplicates (first 10):")
        for file_id, name, parent, size in duplicates[:10]:
            print(f"  [{format_size(size):>10}]  {name}  (folder: {parent})")
        if len(duplicates) > 10:
            print(f"  ... and {len(duplicates)-10:,} more. See {LOG_FILE} for full list.")
        return

    print(f"\n⚠️  About to TRASH {len(duplicates):,} files, freeing {format_size(total_bytes)}.")
    print("Files will move to Google Drive Trash (recoverable for 30 days).")
    confirm = input(f"Type the number of files to confirm ({len(duplicates)}): ").strip()

    if confirm != str(len(duplicates)):
        log.info("Confirmation mismatch. Aborting — no files were trashed.")
        return

    trash_files(service, duplicates)
    conn.close()


if __name__ == '__main__':
    main()
