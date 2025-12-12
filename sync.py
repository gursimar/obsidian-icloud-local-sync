from colorama import Fore, Style, init as colorama_init
import asyncio
import aiofiles
import hashlib
import json
import os
import shutil
from datetime import datetime
import ctypes
from ctypes import wintypes
import time
import traceback

# initialize colorama for Windows console
colorama_init()

# Windows constants for MoveFileEx
MOVEFILE_REPLACE_EXISTING = 0x1
MOVEFILE_WRITE_THROUGH = 0x8

# SetFileAttributes flags
FILE_ATTRIBUTE_NORMAL = 0x80

# ctypes wrappers
kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
MoveFileExW = kernel32.MoveFileExW
MoveFileExW.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
MoveFileExW.restype = wintypes.BOOL

SetFileAttributesW = kernel32.SetFileAttributesW
SetFileAttributesW.argtypes = (wintypes.LPCWSTR, wintypes.DWORD)
SetFileAttributesW.restype = wintypes.BOOL

# ----------------- CONFIG -----------------
LOCAL_VAULT = r"C:\Users\simar\Projects\obsidian-sync\Personal"
ICLOUD_VAULT = r"C:\Users\simar\iCloudDrive\iCloud~md~obsidian\Personal"
HISTORY_DIR = r"C:\Users\simar\Projects\obsidian-sync\History"

STATE_FILE = "sync_state.json"
COOLDOWN_SECONDS = 3
POLL_INTERVAL = 2
STABILITY_WINDOW = 3   # for create/delete stability
STABILIZE_WAIT = 8       # for Case D (both changed)
TINY_THRESHOLD = 8       # bytes — treat tiny files as ephemeral (untitled)
# ------------------------------------------

cooldowns = {}  # rel_path -> timestamp until which file is on cooldown

# ---------- Logging helpers ----------
def log_info(msg):    print(Fore.CYAN + "[INFO] " + Style.RESET_ALL + msg)
def log_warn(msg):    print(Fore.YELLOW + "[WARN] " + Style.RESET_ALL + msg)
def log_error(msg):   print(Fore.RED + "[ERROR] " + Style.RESET_ALL + msg)
def log_success(msg): print(Fore.GREEN + "[OK] " + Style.RESET_ALL + msg)
def log_action(msg):  print(Fore.MAGENTA + "[ACTION] " + Style.RESET_ALL + msg)

# ----------- Utility helpers ------------
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def safe_exists(path):
    try:
        return os.path.exists(path)
    except Exception:
        return False

def size_or_zero(path):
    try:
        return os.path.getsize(path)
    except Exception:
        return 0

def safe_mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log_warn(f"Failed loading state file: {e}")
        return {}

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log_warn(f"Failed saving state file: {e}")

# ----------- Hashing (async) ------------
async def hash_file(path, max_retries=6):
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    attempt = 0
    backoff = 0.05
    while True:
        try:
            async with aiofiles.open(path, "rb") as f:
                while True:
                    chunk = await f.read(8192)
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
        except (PermissionError, OSError) as e:
            attempt += 1
            if attempt > max_retries:
                log_error(f"hash_file giving up on {path}: {e}")
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 1.0)

# ----------- IO helpers (atomic copy with retries) ------------
def set_normal_attributes(path):
    try:
        if not os.path.exists(path):
            return True
        res = SetFileAttributesW(path, FILE_ATTRIBUTE_NORMAL)
        return bool(res)
    except Exception:
        return False

async def async_copy(src, dst, max_retries=12, initial_backoff=0.25):
    """Copy src -> dst atomically with retries and Windows fallbacks."""
    log_action(f"Copying {src} -> {dst}")
    ensure_dir(os.path.dirname(dst))

    tmp = dst + ".tmp"
    try:
        # run blocking copy in thread so loop remains responsive
        await asyncio.to_thread(shutil.copy2, src, tmp)
    except Exception as e:
        log_error(f"Failed to write tmp file {tmp}: {e}")
        raise

    backoff = initial_backoff
    attempt = 0
    while True:
        try:
            if os.path.exists(dst):
                set_normal_attributes(dst)

            # first try os.replace (atomic same-drive)
            os.replace(tmp, dst)
            log_success(f"Updated: {dst}")
            return

        except PermissionError as e:
            attempt += 1
            log_warn(f"PermissionError replacing file (attempt {attempt}): {dst} - {e}")
            set_normal_attributes(dst)

            if attempt >= max_retries:
                log_warn("Max retries reached - trying Win32 MoveFileEx fallback")
                try:
                    ok = MoveFileExW(tmp, dst, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
                    if ok:
                        log_success(f"MoveFileEx succeeded: {dst}")
                        return
                    else:
                        err = ctypes.get_last_error()
                        log_error(f"MoveFileEx failed (err {err}).")
                except Exception as exc:
                    log_error(f"MoveFileEx exception: {exc}")

                # final brute-force attempt
                try:
                    if os.path.exists(dst):
                        os.remove(dst)
                    os.replace(tmp, dst)
                    log_success(f"Forced replace succeeded after removing destination: {dst}")
                    return
                except Exception as exc:
                    log_error(f"Final forced replace failed: {exc}")
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)
                    except Exception:
                        pass
                    raise PermissionError(f"Unable to replace {dst}") from exc

            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8, 5.0)

        except Exception as unexpected:
            log_error(f"Unexpected error during replace: {unexpected}")
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            raise

async def create_conflict_duplicate(path):
    base, ext = os.path.splitext(path)
    conflict = f"{base}_CONFLICT_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
    log_warn(f"Creating conflict duplicate: {conflict}")
    try:
        await asyncio.to_thread(shutil.copy2, path, conflict)
    except Exception as e:
        log_error(f"Failed to create conflict duplicate {conflict}: {e}")

# ---------- Core file operations ----------
async def push_local_to_icloud(rel):
    local = os.path.join(LOCAL_VAULT, rel)
    icloud = os.path.join(ICLOUD_VAULT, rel)
    history = os.path.join(HISTORY_DIR, rel)
    await async_copy(local, icloud)
    await async_copy(local, history)
    cooldowns[rel] = time.time() + COOLDOWN_SECONDS

async def restore_local_from_icloud(rel):
    local = os.path.join(LOCAL_VAULT, rel)
    icloud = os.path.join(ICLOUD_VAULT, rel)
    history = os.path.join(HISTORY_DIR, rel)
    await async_copy(icloud, local)
    await async_copy(icloud, history)
    cooldowns[rel] = time.time() + COOLDOWN_SECONDS

def remove_file_safe(path, description):
    try:
        if os.path.exists(path):
            os.remove(path)
            log_success(f"Removed {description}: {path}")
    except Exception as e:
        log_error(f"Failed to remove {description} {path}: {e}")

# ---------- Build union of all relative file paths ----------
def gather_all_rel_paths():
    rels = set()
    def collect(root):
        for dirpath, dirs, files in os.walk(root):
            for f in files:
                full = os.path.join(dirpath, f)
                # compute rel relative to its base root
                rel = os.path.relpath(full, root)
                # but we need unified relative path — transform to normalized path using forward slashes
                # For uniqueness, use os.path.normpath(rel)
                rels.add(os.path.normpath(os.path.join(os.path.relpath(dirpath, root), f)))
    # collect from each base, but ensure same relative structure assumptions
    collect(LOCAL_VAULT)
    collect(ICLOUD_VAULT)
    collect(HISTORY_DIR)
    # normalize rels to be relative path inside vault (strip leading . or ..)
    normalized = set()
    for r in rels:
        nr = os.path.normpath(r).lstrip(os.sep)
        normalized.add(nr)
    return normalized

# ---------- Main per-file sync logic (uses union of paths) ----------
async def sync_file(rel_path):
    now = time.time()
    if rel_path in cooldowns and cooldowns[rel_path] > now:
        # skip recently synced files
        return

    local = os.path.join(LOCAL_VAULT, rel_path)
    icloud = os.path.join(ICLOUD_VAULT, rel_path)
    history = os.path.join(HISTORY_DIR, rel_path)

    L_exists = safe_exists(local)
    C_exists = safe_exists(icloud)
    H_exists = safe_exists(history)

    # ------------- CASE: nothing exists anywhere -------------
    if not L_exists and not C_exists:
        if H_exists:
            # rule 3: no local, no icloud, but history exists -> delete history
            log_warn(f"\n[DELETE] No local & no iCloud for {rel_path} -> removing history")
            remove_file_safe(history, "history")
        return

    # helper to re-evaluate hashes after stability window
    async def recheck_hashes():
        await asyncio.sleep(STABILITY_WINDOW)
        Lh = await hash_file(local) if safe_exists(local) else None
        Ch = await hash_file(icloud) if safe_exists(icloud) else None
        Hh = await hash_file(history) if safe_exists(history) else None
        return Lh, Ch, Hh

    # ------------- CASE: local missing, history+icloud exist -> delete both -------------
    if (not L_exists) and C_exists and H_exists:
        # rule 1: user deleted local -> delete history and icloud (but wait stabilize)
        log_warn(f"\n[POTENTIAL DELETE] Local missing; history+icloud present for {rel_path} -> stabilizing")
        Lh, Ch, Hh = await recheck_hashes()
        # if icloud still equals history, treat as deletion
        if Ch is not None and Hh is not None and Ch == Hh:
            log_warn(f"[DELETE CONFIRMED] Removing iCloud & history for {rel_path}")
            remove_file_safe(icloud, "iCloud")
            remove_file_safe(history, "history")
        else:
            # remote changed -> restore local from icloud
            log_warn(f"[RESTORE] iCloud changed vs history for {rel_path} -> restoring local")
            await restore_local_from_icloud(rel_path)
        return

    # ------------- CASE: icloud missing, history+local exist -> delete local+history -------------
    if (not C_exists) and L_exists and H_exists:
        # rule 2: user deleted from icloud -> remove local & history (after stabilizing)
        log_warn(f"\n[POTENTIAL DELETE] iCloud missing; local+history present for {rel_path} -> stabilizing")
        Lh, Ch, Hh = await recheck_hashes()
        # if local equals history -> remote deletion intended
        if Lh is not None and Hh is not None and Lh == Hh:
            log_warn(f"[DELETE CONFIRMED] Removing local & history for {rel_path}")
            remove_file_safe(local, "local")
            remove_file_safe(history, "history")
        else:
            # local changed -> push to icloud
            log_warn(f"[PUSH] Local changed vs history for {rel_path} -> pushing local to iCloud")
            await push_local_to_icloud(rel_path)
        return

    # ------------- CASE: new creation (local exists, no history nor icloud) -------------
    if L_exists and (not C_exists) and (not H_exists):
        log_warn(f"\n[CREATE] Local-only new file detected {rel_path} -> stabilizing")
        Lh, Ch, Hh = await recheck_hashes()
        local_size = size_or_zero(local)
        if Lh is None:
            log_info(f"[CREATE] After stabilize local missing or unreadable for {rel_path} -> skip")
            return
        if local_size < TINY_THRESHOLD:
            log_info(f"[CREATE] Local file appears tiny ({local_size} bytes); deferring {rel_path}")
            return
        log_info(f"[CREATE] Seeding history & pushing to iCloud for {rel_path}")
        await push_local_to_icloud(rel_path)
        return

    # ------------- CASE: new creation (icloud exists, no local nor history) -------------
    if C_exists and (not L_exists) and (not H_exists):
        log_warn(f"\n[CREATE] iCloud-only new file detected {rel_path} -> stabilizing")
        Lh, Ch, Hh = await recheck_hashes()
        icloud_size = size_or_zero(icloud)
        if Ch is None:
            log_info(f"[CREATE] After stabilize iCloud unreadable/missing for {rel_path} -> skip")
            return
        if icloud_size < TINY_THRESHOLD:
            log_info(f"[CREATE] iCloud file appears tiny ({icloud_size} bytes); deferring {rel_path}")
            return
        log_info(f"[CREATE] Restoring local & seeding history from iCloud for {rel_path}")
        await restore_local_from_icloud(rel_path)
        return

    # ------------- At this point both sides exist OR we have mixed states -------------
    # Ensure history directory exists
    ensure_dir(os.path.dirname(history))

    L = await hash_file(local) if safe_exists(local) else None
    C = await hash_file(icloud) if safe_exists(icloud) else None
    H = await hash_file(history) if safe_exists(history) else None

    # If history missing but both sides exist, seed history conservatively after stabilization
    if H is None and (L is not None or C is not None):
        log_info(f"[HISTORY MISSING] Waiting to seed history for {rel_path}")
        Lh, Ch, Hh = await recheck_hashes()
        # prefer local if exists and not tiny
        if Lh is not None and size_or_zero(local) >= TINY_THRESHOLD:
            await async_copy(local, history)
            H = Lh
            log_info(f"Initialized history from local for {rel_path}")
        elif Ch is not None and size_or_zero(icloud) >= TINY_THRESHOLD:
            await async_copy(icloud, history)
            H = Ch
            log_info(f"Initialized history from iCloud for {rel_path}")
        else:
            log_info(f"History seeding skipped for {rel_path}; will retry next pass")
            return

    # CASE A: identical
    if L == C == H:
        return

    # CASE B: local changed
    if L is not None and H is not None and (L != H) and (C == H):
        log_info(f"\n[SYNC] Local changed -> pushing local for {rel_path}")
        await push_local_to_icloud(rel_path)
        return

    # CASE C: icloud changed
    if C is not None and H is not None and (C != H) and (L == H):
        log_info(f"\n[SYNC] iCloud changed -> restoring local for {rel_path}")
        await restore_local_from_icloud(rel_path)
        return

    # CASE D: both changed (rare)
    log_warn(f"\nCase D (both changed) for {rel_path} -> stabilizing {STABILIZE_WAIT}s")
    await asyncio.sleep(STABILIZE_WAIT)
    L2 = await hash_file(local) if safe_exists(local) else None
    C2 = await hash_file(icloud) if safe_exists(icloud) else None
    H2 = await hash_file(history) if safe_exists(history) else None

    if L2 is not None and L2 != L:
        log_warn("Local still changing — choose local")
        await create_conflict_duplicate(icloud)
        await push_local_to_icloud(rel_path)
        return

    if C2 is not None and C2 != C:
        log_warn("iCloud still changing — choose iCloud")
        await create_conflict_duplicate(local)
        await restore_local_from_icloud(rel_path)
        return

    # fallback to timestamps
    local_m = safe_mtime(local)
    icloud_m = safe_mtime(icloud)
    if local_m >= icloud_m:
        log_warn("Resolving conflict: local newer")
        await create_conflict_duplicate(icloud)
        await push_local_to_icloud(rel_path)
    else:
        log_warn("Resolving conflict: iCloud newer")
        await create_conflict_duplicate(local)
        await restore_local_from_icloud(rel_path)

# ---------- Main loop ----------
async def main():
    log_info("Starting Obsidian Sync Engine (async mode)")
    ensure_dir(HISTORY_DIR)
    state = load_state()
    while True:
        try:
            rel_paths = gather_all_rel_paths()
            # iterate sorted for stable logs/order
            for rel in sorted(rel_paths):
                if '.obsidian' in rel and 'workspace' in rel and '.json' in rel: continue  # skip workspace.json files
                try:
                    await sync_file(rel)
                except Exception as e:
                    log_error(f"Error syncing {rel}: {e}")
                    traceback.print_exc()
            save_state(state)
        except Exception as outer:
            log_error(f"Unexpected error during scan: {outer}")
            traceback.print_exc()
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
