# Google Drive Cleaner

A Python script that finds and removes duplicate files in Google Drive. Works with all three target types — My Drive, Shared Drives, and any specific subfolder — with full recursive scanning at every depth.

---

## How It Works

1. **Detect** — reads `ROOT_FOLDER_ID` and auto-detects whether it is My Drive (`root`), a Shared Drive, or a regular folder.
2. **Index** — fetches metadata for every file in the target and stores it in a local SQLite database. For My Drive and regular folders, it first does a full recursive BFS walk of the folder tree so no subfolder at any depth is missed.
3. **Detect duplicates** — SQL query finds files that share the same MD5 checksum within the same parent folder. The earliest-created file in each group is kept; all later copies are flagged.
4. **Delete** — moves flagged files to Google Drive Trash using batch API requests. Files stay in Trash for 30 days and can be recovered.

> **Default mode is always a dry run.** Nothing is deleted unless you explicitly pass `--delete`.

---

## Supported Targets

Set `ROOT_FOLDER_ID` in the script to one of:

| Value | What it scans |
|---|---|
| `'root'` | All of My Drive (personal drive) |
| `'0AGdvD8JVt...'` | An entire Shared Drive |
| `'1FAYh9uNqbllIcjG...'` | A specific folder and all its subfolders |

The script detects which type it is automatically — no other config changes needed.

---

## Requirements

### Python

Python 3.10 or higher.

### Dependencies

```bash
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
or
pip install -r requirements.txt
```

### Google Cloud Setup

You need a Google Cloud project with OAuth credentials:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select a project
3. Go to *APIs & Services → Library* and enable the **Google Drive API**
4. Go to *APIs & Services → Credentials → Create Credentials → OAuth client ID*
5. Choose **Desktop app** as the application type
6. Download the file and save it as `credentials.json` in the same folder as the script

On first run a browser window will open for authorization. After that, a `token.json` file is saved so you won't need to log in again.

---

## Configuration

Open the script and edit the constants at the top:

| Constant | Default | Description |
|---|---|---|
| `ROOT_FOLDER_ID` | *(your folder ID)* | What to scan — see Supported Targets above |
| `DB_NAME` | `database.db` | SQLite database filename |
| `LOG_FILE` | `logs.log` | Log filename |
| `TOKEN_FILE` | `token.json` | OAuth token (auto-created on first login) |
| `CREDENTIALS_FILE` | `credentials.json` | OAuth credentials downloaded from Google Cloud |
| `PAGE_SIZE` | `1000` | Files fetched per API page (max 1000) |
| `LOG_INTERVAL` | `5000` | Log a progress line every N files during indexing |
| `FOLDER_BATCH_SIZE` | `50` | Folder IDs combined per file query (folder mode only) |

**How to find your ID:**

- **My Drive:** use the literal string `'root'`
- **Shared Drive:** open it in a browser — the ID is the long string at the end of the URL, e.g. `https://drive.google.com/drive/u/0/folders/0AGdvD8JVtDj9Uk9PVA` → ID is `0AGdvD8JVtDj9Uk9PVA`
- **Specific folder:** same — open the folder in a browser and copy the ID from the URL

---

## Usage

### Dry run — safe, no changes made

```bash
python drive_cleaner.py
```

Indexes all files, detects duplicates, and prints a summary. Nothing is deleted.

### Re-fetch all metadata from Drive

```bash
python drive_cleaner.py --reindex
```

Drops the existing local index and re-fetches everything. Use this after files have been added, moved, or deleted since the last run. Without this flag, the script reuses the cached database and skips re-indexing.

### Delete duplicates

```bash
python drive_cleaner.py --delete
```

After detecting duplicates, shows a summary and asks you to type the exact number of files to confirm. Files are moved to Trash — not permanently deleted.

### Re-index and delete in one go

```bash
python drive_cleaner.py --reindex --delete
```

---

## Example Output

**Dry run:**
```
2026-04-06 10:12:01 - INFO - Detected regular folder: 'My Books' [1FAYh9uNq...]
2026-04-06 10:12:01 - INFO - Walking folder tree (BFS)...
2026-04-06 10:12:04 - INFO - Folder walk complete. Total folders found: 142
2026-04-06 10:12:04 - INFO - Indexing files across 142 folders in 3 batch(es)...
2026-04-06 10:12:09 - INFO - Indexing complete. Total files scanned: 8,204
2026-04-06 10:12:09 - INFO - Scanning for duplicates...
2026-04-06 10:12:09 - INFO - Found 1,847 duplicate files (5.2 GB recoverable).
2026-04-06 10:12:09 - INFO - Dry run — no files will be trashed. Use --delete to proceed.

Sample duplicates (first 10):
  [   3.1 MB]  Dune - Frank Herbert.epub  (folder: 1A2B3C...)
  [   1.8 MB]  Sapiens.pdf                (folder: 5E6F7G...)
  ...and 1,837 more. See logs.log for full list.
```

**Delete confirmation:**
```
⚠️  About to TRASH 1,847 files, freeing 5.2 GB.
Files will move to Google Drive Trash (recoverable for 30 days).
Type the number of files to confirm (1847): 1847

2026-04-06 10:13:02 - INFO - Trashing 1,847 files in 19 batch(es)...
2026-04-06 10:13:05 - INFO - Progress: 100/1,847 trashed | 0 failed
...
2026-04-06 10:13:44 - INFO - Done. Trashed: 1,847 | Failed: 0
```

---

## Files Created at Runtime

| File | Description |
|---|---|
| `token.json` | OAuth access token — auto-created on first login, permissions set to 600 |
| `database.db` | SQLite database of indexed file metadata — safe to delete and rebuild with `--reindex` |
| `logs.log` | Full UTF-8 log of all actions and errors |

---

## How Scanning Works per Target Type

**My Drive (`root`) and regular folders**

The Drive API only matches direct children with `'<id>' in parents` — it has no native recursive query. To get around this, the script does a BFS walk first:

```
1. Start with ROOT_FOLDER_ID
2. Find all subfolders directly inside it
3. Find all subfolders inside those
4. Repeat until no more subfolders exist
5. Query files across all collected folder IDs in batches of 50
```

This guarantees every file at every depth is found, identical to what a Shared Drive scan produces.

**Shared Drives**

The `driveId` parameter in the Drive API naturally scopes results to the entire drive at all depths — no BFS walk needed. The script uses a single paginated query.

---

## Safety & Design Decisions

**Duplicates are trashed, not permanently deleted.**
Google Drive keeps trashed files for 30 days, giving you a full recovery window.

**The original file is always kept.**
Within each group (same parent folder + same MD5), the file with the earliest `createdTime` is preserved. All later copies are trashed.

**Confirmation requires typing the exact file count.**
The `--delete` prompt rejects anything other than the precise number, preventing accidental confirmation.

**Resumable indexing.**
If indexing crashes halfway, re-running without `--reindex` continues from where it left off — existing rows are skipped via `INSERT OR IGNORE`.

**No RAM pressure.**
All metadata is stored in SQLite. Millions of files can be indexed on a machine with minimal memory.

**UTF-8 logging.**
Both the log file and console output are forced to UTF-8, so filenames with Vietnamese or other non-Latin characters display correctly on Windows.

**No pickle.**
OAuth tokens are stored as plain JSON, not Python pickle files.

---

## Limitations

- Duplicates are detected **within the same parent folder only**. Identical files in different folders are intentionally not flagged — they may be there for a reason.
- Google Workspace files (Docs, Sheets, Slides) have no MD5 checksum and are automatically skipped.
- In the rare case two files in the same group share the exact same `createdTime`, the keeper is chosen by whichever row the database stored first.
- Trashing files from a Shared Drive requires **Content Manager** role or higher. Viewer and Commenter roles will get permission errors.

---

## Troubleshooting

**`credentials.json` not found**
Download the OAuth credentials file from Google Cloud Console and place it in the same directory as the script.

**`403 accessNotConfigured`**
The Google Drive API is not enabled. Go to *APIs & Services → Library* in Google Cloud Console and enable it.

**`Shared drive not found`**
Your `ROOT_FOLDER_ID` looks like a Shared Drive ID but the API can't find it. Double-check the ID from the browser URL, and confirm your account has access to that drive.

**`ID is not a folder`**
Your `ROOT_FOLDER_ID` points to a file, not a folder. Copy the ID from a folder URL in Google Drive, not a file URL.

**Rate limit warnings during indexing or delete**
Normal for large operations. The script retries automatically with exponential backoff — no action needed.

**Vietnamese or special characters showing as errors in the console**
Set the environment variable `PYTHONUTF8=1` before running, or set it permanently in Windows environment variables. The log file will always be correct regardless.
