
## Running

``` python
clear; python -u sync.py 2>&1 | Tee-Object -FilePath "out.log"
```

--- 

## Problem (what went wrong with iCloud + Obsidian on Windows)

When using an Obsidian vault stored in iCloud Drive on Windows (via the iCloud for Windows client), several practical sync/FS problems appear:

1. **Transient cloud placeholders & hydration**
   iCloud shows placeholder files in Explorer that are not fully downloaded (0 bytes or unreadable) until the provider hydrates them. A sync tool that reads these stubs can treat them as real files, causing incorrect operations.

2. **Transient locks & PermissionError**
   iCloud/Explorer and Obsidian sometimes hold short-lived exclusive handles while hydrating, uploading, or saving. A sync script attempting an atomic replace/rename during that exact moment gets `PermissionError` / access denied.

3. **Duplicate files created by cloud client**
   When edits race or metadata changes confuse the client, iCloud can produce duplicate names like:

   * `Scratch (1).md`, `Scratch (2).md`
     These appear when the provider or client tries to avoid overwriting or when it detects conflicts. They clutter the vault and indicate data churn/loss risk.

4. **Editor reports “file externally modified” during rapid edits**
   If Obsidian is saving rapidly (autosave or quick edits) and the sync process reads or writes at the same time, Obsidian can detect a change it did not make and warn “the file was externally modified” (or fail to save). This is a race: both the editor and the sync process contend for the same file.

5. **Deletes & renames get lost if you only scan one side**
   If the sync only inspects the local vault, deletes or creates on iCloud (or the history snapshot) can be ignored, producing inconsistent state.

All of the above are symptoms of naive two-way syncing and OS/cloud-provider behaviors on Windows. They lead to confusing duplicates, lost edits, and intermittent errors.

---

## High-level solution (what we aim to do)

**Maintain a single authoritative local vault and a self-hosted local history, and synchronize that vault with the iCloud folder using a robust three-way algorithm.**

Key principles:

* Keep a **History** snapshot folder with the last-known-good content for each file. This gives a third reference point to detect which side changed (local vs iCloud) rather than reacting to metadata-only changes.
* Always consider the **union** of files across Local, iCloud, and History (so deletions and creations anywhere are visible).
* Use **stabilization windows** and **per-file cooldowns** to avoid acting on mid-write states or ephemeral untitled files.
* Use **atomic copy + retry** for writes, with safe fallbacks to cope with transient Windows locks.
* Be conservative with deletes and always log destructive actions; create conflict duplicates instead of silently overwriting.

This design prevents duplicates like `Scratch (1).md`, avoids Obsidian “externally modified” races, and preserves user data.

---

## Core algorithm — three-way sync (Local / iCloud / History)

Terminology:

* `L` = local file (your Windows Obsidian vault)
* `C` = iCloud file (iCloud Drive copy on Windows)
* `H` = history snapshot (local History folder copy from last sync)

The per-file decision procedure runs over the union of relative paths present in `LOCAL_VAULT`, `ICLOUD_VAULT`, and `HISTORY_DIR`.

### Preparations (per sync pass)

1. Build the union set of relative file paths found under Local, iCloud, and History.
2. For each file path:

   * Skip if file is on a short **cooldown** (recently synced).
   * Use cheap checks (exists, size, mtime) first and only compute full content SHA-256 hashes if needed.
   * If hashing hits a locked/unreadable file, retry a few times with backoff.

### Stabilization & cooldown rules

* **STABILITY_WINDOW** (e.g., 1–2s): when a file is newly created, deleted, or appears to have changed, wait this short interval before making destructive decisions. This handles `Untitled.md` → rename → typing workflows and avoids mid-save races.
* **STABILIZE_WAIT** (longer, e.g., 8s): used only for “both changed” (conflict) cases to detect ongoing active editing.
* **COOLDOWN_SECONDS** (short, e.g., 3s): after a successful push/restore, skip that file for a short period to avoid thrash from autosave.

### Decision rules (explicit cases)

#### 1. Deletion rules (explicit)

* **If `L` missing, but `C` and `H` exist** → (user deleted locally)
  Wait `STABILITY_WINDOW`, recheck hashes:

  * If `C == H` → **delete `C` and `H`** (confirm local deletion).
  * Else (`C != H`) → remote changed since last sync → **restore local from `C`** (do not delete remote).

* **If `C` missing, but `L` and `H` exist** → (user deleted on iCloud)
  Wait `STABILITY_WINDOW`, recheck hashes:

  * If `L == H` → **delete `L` and `H`** (confirm remote deletion).
  * Else (`L != H`) → local changed → **push `L` → `C`** (do not delete local).

* **If `L` and `C` both missing, but `H` exists** → **delete `H`** (file removed everywhere).

These rules ensure deletes are intentional and avoid accidental removal due to renames or transient states.

#### 2. Creation rules

* **If `L` exists and `C` and `H` are missing** → new local file (e.g., a new note):
  Wait `STABILITY_WINDOW`. If still present and not a tiny placeholder (`Untitled`), **seed `H` from `L` and push `L` → `C`**.

* **If `C` exists and `L` and `H` are missing** → new remote file (created on phone/Mac):
  Wait `STABILITY_WINDOW`. If still present and not tiny, **restore local from `C` and seed `H`**.

This prevents creating history for ephemeral placeholder files and respects renames.

#### 3. Normal two-way sync (both exist)

Compute stable content hashes for `L`, `C`, `H`:

* **If `L == C == H`** → nothing to do.
* **If `L != H` and `C == H`** → Local changed → **push `L` → `C` and update `H`**.
* **If `C != H` and `L == H`** → Remote changed → **restore `L` from `C` and update `H`**.

#### 4. Conflict: both changed (`L != H` and `C != H` and `L != C`)

* Enter **STABILIZE_WAIT**, re-check hashes:

  * If one side is still actively changing (hash differs from previous), prefer the **active** side (push/restore accordingly).
  * If both stable, pick **newest by mtime**, but **create a conflict duplicate** of the losing side (`filename_CONFLICT_TIMESTAMP`) before overwriting. This preserves both versions and avoids data loss.

### Implementation building blocks

* **Atomic writes**: write to `dst.tmp` then `os.replace(tmp, dst)`. On Windows, retry with exponential backoff, and use Win32 `MoveFileEx` as a fallback when necessary.
* **Hashing with retries**: compute SHA-256; if file locked, retry a few times.
* **History folder**: a simple directory mirroring the vault structure containing the last-known-good file content per path.
* **Union scan**: walk Local, iCloud, and History to collect all relative paths each pass (so deletions anywhere are visible).
* **Extensive logging**: every destructive/merge action is logged so the operator can inspect and recover if needed.

---

## Why this avoids the original symptoms

* **No more duplicate `Scratch (1).md`**: duplicates were created by racey overwrites or cloud-client conflict logic. The algorithm avoids overwriting remote copies unless the local change is authoritative, and when ambiguity exists it creates conflict duplicates rather than letting the cloud auto-rename. That prevents iCloud from inventing numbered copies as often.
* **No more “externally modified” warnings during rapid editing**: stabilization and hashing-with-retries avoid reading/writing mid-save. The sync avoids contending with Obsidian’s write handle by not acting until the file is stable for STABILITY_WINDOW seconds, and by using retry/backoff rather than immediate replace.
* **Deletes and creates are handled predictably**: because we inspect the union of files and use `H` as last-known-good, deletes are confirmed before being propagated and creations are only seeded once stable and non-ephemeral.
* **Data safety**: conflict duplicates + history snapshots + logging mean nothing is silently lost.

---

## Example flows

1. **Simple edit on Windows**

   * You edit `note.md` locally. `L` changes, `C == H`. The script sees `L != H && C == H` → pushes local to `C` and updates `H`.

2. **Edit on iPhone (remote)**

   * `C` changes while `L == H`. Script sees `C != H && L == H` → restores local from `C` and updates `H`.

3. **Simultaneous edits (rare)**

   * Both `L` and `C` differ from `H`. Script waits `STABILIZE_WAIT`. If one side is actively changing, prefer it. Otherwise pick latest by mtime but create `*_CONFLICT_TIMESTAMP.md` for the other side before overwriting.

4. **Rename/Untitled workflow**

   * `Untitled.md` created → renamed → typed: stabilization prevents seeding history for the ephemeral tiny `Untitled.md`. After rename + typing, file is stable and then the script seeds and pushes, so rename behaves normally without causing deletes or duplicates.

5. **Delete on local**

   * You delete `old.md` locally while `C` and `H` exist. Script waits `STABILITY_WINDOW`. If `C == H`, it deletes `C` and `H` (confirmed). If `C != H`, remote changed — it restores local.

---

## Operational notes & tuning

* Run the script natively on Windows (not WSL). Windows file semantics and iCloud placeholders behave poorly under WSL.
* Tune `STABILITY_WINDOW` (1–2s), `STABILIZE_WAIT` (e.g., 8s), and `COOLDOWN_SECONDS` (3–10s) for your editing speed and vault size.
* Exclude ephemeral or large binary folders (e.g., `.obsidian/cache`) to reduce churn.
* Keep backups (or auto-commit `HISTORY_DIR` to Git) before wide deployment.

---

## Summary

The problem was racey, metadata-driven behavior when using an iCloud-backed Obsidian vault on Windows: duplicate files (`Scratch (1).md` etc.), `PermissionError` on atomic replace, and Obsidian “externally modified” warnings.

The solution is a conservative, history-backed three-way sync between Local, iCloud, and a local History snapshot. It uses stabilization windows, per-file cooldowns, content hashes, atomic writes with retries, and explicit creation/deletion rules to avoid duplicates, data loss, and editor-synchronization races while making sync predictable and auditable.
#   o b s i d i a n - i c l o u d - l o c a l - s y n c  
 