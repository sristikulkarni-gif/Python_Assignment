#!/usr/bin/env python3
"""
Supabase Storage File Manager (Flask)
- Buckets listed one per line
- Click a bucket to expand its contents below (multiple open at once)
- Create/Delete buckets
- Browse folders & files inline (no full page reload)
- Create/Delete folders (recursive), Upload/Download/Delete files
- Copy/Move files & folders within or across buckets
"""

from __future__ import annotations
import io
import mimetypes
import os
import re
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

from flask import (
    Flask, request, redirect, url_for, render_template_string,
    flash, send_file, jsonify
)
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ---------------------- Config ----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("SUPABASE_URL and SUPABASE_KEY must be set as environment variables.")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 512 * 1024 * 1024  # 512 MB uploads
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------- Helpers ----------------------
VALID_SEGMENT = re.compile(r"^[A-Za-z0-9._#@+-][A-Za-z0-9._#@+\-\s]*$")

def split_path(path: str) -> List[str]:
    path = (path or "").strip("/")
    return [p for p in path.split("/") if p]

def join_path(*parts: str) -> str:
    items: List[str] = []
    for p in parts:
        if not p:
            continue
        for seg in split_path(p):
            items.append(seg)
    return "/".join(items)

def validate_segment(seg: str) -> Optional[str]:
    if not seg:
        return "Name is required."
    if seg in {".", ".."}:
        return "Name cannot be '.' or '..'."
    if "/" in seg:
        return "Name cannot contain '/'."
    if not VALID_SEGMENT.match(seg):
        return "Invalid characters. Allowed: letters, numbers, spaces, . _ - + # @"
    return None

def get_bucket_names() -> List[str]:
    """Return bucket names for different supabase-py versions (dict or objects)."""
    try:
        buckets = sb.storage.list_buckets()
        names: List[str] = []
        for b in buckets or []:
            if isinstance(b, dict):
                nm = b.get("name")
            else:
                nm = getattr(b, "name", None)
            if nm:
                names.append(nm)
        return sorted(names)
    except Exception as ex:
        flash(f"Failed to list buckets: {ex}", "error")
        return []

@dataclass
class Item:
    name: str
    is_folder: bool
    size: Optional[int]
    updated_at: Optional[str]

def list_items(bucket: str, prefix: str) -> Tuple[List[Item], List[Item]]:
    prefix = (prefix or "").strip("/")
    resp = sb.storage.from_(bucket).list(
        prefix or "",
        {"limit": 1000, "offset": 0, "sortBy": {"column": "name", "order": "asc"}}
    )
    entries: List[Dict[str, Any]] = resp or []
    folders: List[Item] = []
    files: List[Item] = []
    for e in entries:
        name = e.get("name") or ""
        meta = e.get("metadata") or {}
        size = meta.get("size") if isinstance(meta, dict) else None
        updated = e.get("updated_at")
        is_folder = size in (None, 0) and not name.endswith(".keep") and e.get("id") is None
        # some backends return trailing slash to signal folder
        if name and name.endswith("/"):
            is_folder = True
            name = name.rstrip("/")
        if is_folder:
            folders.append(Item(name, True, None, updated))
        else:
            files.append(Item(name, False, size, updated))
    return folders, files

def ensure_placeholder_for_folder(bucket: str, prefix: str) -> Tuple[bool, str]:
    placeholder_key = join_path(prefix, ".keep")
    try:
        sb.storage.from_(bucket).upload(placeholder_key, b"")
        return True, placeholder_key
    except Exception as ex:
        return False, f"Failed to create folder placeholder: {ex}"

def delete_prefix_recursive(bucket: str, prefix: str) -> Tuple[int, List[str]]:
    """Delete everything under prefix (folder)."""
    deleted, errors = 0, []

    def _walk_delete(pfx: str):
        nonlocal deleted
        resp = sb.storage.from_(bucket).list(pfx or "", {"limit": 1000, "offset": 0})
        for e in resp or []:
            name = e.get("name") or ""
            meta = e.get("metadata") or {}
            size = meta.get("size") if isinstance(meta, dict) else None
            full = join_path(pfx, name)
            # recurse folders (heuristic)
            if size in (None, 0) and e.get("id") is None and not name.endswith(".keep"):
                _walk_delete(full)
                continue
            try:
                sb.storage.from_(bucket).remove([full])
                deleted += 1
            except Exception as dex:
                errors.append(f"{full}: {dex}")
        # remove placeholder if exists
        try:
            sb.storage.from_(bucket).remove([join_path(pfx, ".keep")])
        except Exception:
            pass

    _walk_delete(prefix.strip("/"))
    return deleted, errors

def copy_file(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
    """Copy single file via download+upload; compatible across SDK versions."""
    data = sb.storage.from_(src_bucket).download(src_key)
    sb.storage.from_(dst_bucket).upload(dst_key, data)

def copy_folder_recursive(src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str) -> Tuple[int, List[str]]:
    """Copy all contents of a folder recursively."""
    copied, errors = 0, []
    src_prefix = (src_prefix or "").strip("/")
    dst_prefix = (dst_prefix or "").strip("/")

    def _walk_copy(pfx_src: str, pfx_dst: str):
        nonlocal copied
        # create placeholder for folder visibility
        try:
            ensure_placeholder_for_folder(dst_bucket, pfx_dst)
        except Exception:
            pass
        resp = sb.storage.from_(src_bucket).list(pfx_src or "", {"limit": 1000, "offset": 0})
        for e in resp or []:
            name = e.get("name") or ""
            meta = e.get("metadata") or {}
            size = meta.get("size") if isinstance(meta, dict) else None
            src_full = join_path(pfx_src, name)
            dst_full = join_path(pfx_dst, name.rstrip("/"))
            if size in (None, 0) and e.get("id") is None and not name.endswith(".keep"):
                # subfolder
                _walk_copy(src_full, dst_full)
                continue
            try:
                copy_file(src_bucket, src_full, dst_bucket, dst_full)
                copied += 1
            except Exception as ex:
                errors.append(f"{src_full} -> {dst_full}: {ex}")

    _walk_copy(src_prefix, dst_prefix)
    return copied, errors

# tries to empty bucket using SDK if available; otherwise manual recursive delete
def empty_bucket(bucket: str):
    try:
        if hasattr(sb.storage, "empty_bucket"):
            sb.storage.empty_bucket(bucket)
            return
    except Exception:
        pass
    # Fallback: list at root and delete recursively
    # supabase storage isn't hierarchical at API level, but we simulate folders (prefixes)
    # We'll recursively delete every top-level entry
    try:
        items = sb.storage.from_(bucket).list("", {"limit": 1000, "offset": 0})
        for e in items or []:
            name = e.get("name") or ""
            meta = e.get("metadata") or {}
            size = meta.get("size") if isinstance(meta, dict) else None
            if size in (None, 0) and e.get("id") is None and not name.endswith(".keep"):
                delete_prefix_recursive(bucket, name)
            else:
                try:
                    sb.storage.from_(bucket).remove([name])
                except Exception:
                    pass
    except Exception:
        pass

# ---------------------- Templates ----------------------
PAGE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Supabase File Manager</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen" style="background:#FAFAFA;">
  <div class="max-w-6xl mx-auto p-2 sm:p-6">
    <header class="mb-6">
      <h1 class="text-2xl font-bold text-gray-900">Supabase File Manager</h1>
      <p class="text-sm text-gray-600">Buckets listed below. Click a bucket to expand its contents.</p>
    </header>

    {% with messages = get_flashed_messages(with_categories=true) %}
      {% if messages %}
        <div class="space-y-2 mb-4">
          {% for category, msg in messages %}
            <div class="p-3 rounded-lg text-sm border
                        {% if category=='error' %} bg-red-50 text-red-700 border-red-200
                        {% else %} bg-green-50 text-green-700 border-green-200 {% endif %}">
              {{ msg }}
            </div>
          {% endfor %}
        </div>
      {% endif %}
    {% endwith %}

    <!-- Create bucket -->
    <section class="mb-6">
      <form method="post" action="{{ url_for('create_bucket') }}" class="flex flex-col gap-2 sm:flex-row sm:items-center">
        <input class="w-full sm:flex-1 rounded-lg border border-gray-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
               type="text" name="bucket_name" placeholder="New bucket name" required />
        <button class="rounded-lg px-4 py-2 font-medium border bg-indigo-600 text-white border-indigo-600 hover:bg-indigo-700"
                type="submit">Create Bucket</button>
      </form>
    </section>

    <!-- Buckets list (one per line) -->
    <section class="space-y-3">
      <h2 class="font-semibold text-gray-800 mb-2">Your Buckets</h2>
      {% if buckets|length == 0 %}
        <div class="rounded-lg border border-gray-200 bg-white p-4 text-sm text-gray-500">No buckets yet. Create one above.</div>
      {% endif %}
      {% for b in buckets %}
        <div class="rounded-xl border border-gray-200 bg-white">
          <div class="flex items-center justify-between p-4">
            <button class="text-left font-medium text-indigo-700 hover:underline"
                    onclick="togglePanel('{{ b }}')"
                    title="Click to expand">{{ b }}</button>
            <form method="post" action="{{ url_for('delete_bucket', bucket=b) }}"
                  onsubmit="return confirm('Delete bucket {{ b }} and all its contents?');">
              <button class="rounded-lg px-3 py-1.5 border bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200"
                      type="submit">Delete</button>
            </form>
          </div>
          <div id="panel-{{ b }}" class="border-t border-gray-200 hidden" data-bucket="{{ b }}"></div>
        </div>
      {% endfor %}
    </section>
  </div>

  <script>
    function togglePanel(bucket) {
      const el = document.getElementById('panel-' + bucket);
      const isHidden = el.classList.contains('hidden');
      if (isHidden && !el.dataset.loaded) {
        loadPanel(bucket, "");
      }
      el.classList.toggle('hidden');
    }

    function loadPanel(bucket, path) {
      const el = document.getElementById('panel-' + bucket);
      const params = new URLSearchParams();
      params.set('partial', '1');
      if (path) params.set('path', path);
      fetch(`/b/${encodeURIComponent(bucket)}?` + params.toString(), { credentials: 'same-origin' })
        .then(r => r.text())
        .then(html => {
          el.innerHTML = html;
          el.dataset.loaded = '1';
          initPanelScripts(bucket);
        })
        .catch(err => {
          el.innerHTML = `<div class="p-4 text-sm text-red-700 bg-red-50 border border-red-200">Failed to load: ${err}</div>`;
        });
    }

    function initPanelScripts(bucket) {
      const container = document.getElementById('panel-' + bucket);
      if (!container) return;

      // Intercept all panel forms marked with data-ajax-panel
      container.querySelectorAll('form[data-ajax-panel]').forEach(form => {
        form.addEventListener('submit', function(e) {
          e.preventDefault();
          const fd = new FormData(form);
          fetch(form.action, { method: 'POST', body: fd, credentials: 'same-origin' })
            .then(r => r.text())
            .then(_ => {
              // Reload current path for this panel
              const cur = container.querySelector('input[name="__panel_path"]');
              const path = cur ? cur.value : "";
              loadPanel(bucket, path);
            })
            .catch(err => alert('Action failed: ' + err));
        });
      });

      // Attach click handlers for breadcrumb buttons that carry data-path
      container.querySelectorAll('[data-path-btn]').forEach(btn => {
        btn.addEventListener('click', () => {
          const p = btn.getAttribute('data-path-btn') || "";
          loadPanel(bucket, p);
        });
      });
    }

    // Allow folder link buttons to navigate inside the panel
    function browsePanel(bucket, path) {
      loadPanel(bucket, path);
    }
  </script>
</body>
</html>
"""

# This is the HTML fragment returned for a single bucket panel (contents)
PANEL = r"""
{% set _buckets = buckets %}
{% set _bucket = bucket %}
{% set _path = path or '' %}
<div class="p-4">
  <!-- Keep track of current path inside this panel for AJAX refresh -->
  <input type="hidden" name="__panel_path" value="{{ _path }}"/>

  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      <div class="space-y-2 mb-3">
        {% for category, msg in messages %}
          <div class="p-3 rounded-lg text-sm border
                      {% if category=='error' %} bg-red-50 text-red-700 border-red-200
                      {% else %} bg-green-50 text-green-700 border-green-200 {% endif %}">
            {{ msg }}
          </div>
        {% endfor %}
      </div>
    {% endif %}
  {% endwith %}

  <!-- Breadcrumbs -->
  <div class="text-sm text-gray-600 mb-3 flex items-center gap-1 flex-wrap">
    <button class="text-indigo-700 hover:underline" data-path-btn="">Home</button>
    {% set accum = [] %}
    {% for seg in segments %}
      {% set _ = accum.append(seg) %}
      <span>/</span>
      <button class="text-indigo-700 hover:underline" data-path-btn="{{ '/'.join(accum) }}">{{ seg }}</button>
    {% endfor %}
  </div>

  <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
    <!-- Left: table -->
    <section class="lg:col-span-2 rounded-xl border border-gray-200 overflow-hidden bg-white">
      <div class="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
        <div class="font-semibold text-gray-800">Contents: <span class="text-gray-500">{{ _path or '/' }}</span></div>
        {% if _path %}
          <form method="post" action="{{ url_for('delete_prefix', bucket=_bucket) }}" data-ajax-panel
                onsubmit="return confirm('Delete this folder and everything inside?');">
            <input type="hidden" name="current_path" value="{{ _path }}" />
            <button class="rounded-lg px-3 py-1.5 border bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200"
                    type="submit">Delete this folder</button>
          </form>
        {% endif %}
      </div>
      <table class="w-full text-sm">
        <thead class="bg-gray-50 text-gray-600">
          <tr>
            <th class="text-left px-3 py-2">Name</th>
            <th class="text-left px-3 py-2">Type</th>
            <th class="text-left px-3 py-2">Size</th>
            <th class="text-left px-3 py-2">Updated</th>
            <th class="text-right px-3 py-2">Actions</th>
          </tr>
        </thead>
        <tbody>
          {% if folders|length == 0 and files|length == 0 %}
            <tr><td colspan="5" class="px-3 py-6 text-center text-gray-500">Empty</td></tr>
          {% endif %}

          {% for f in folders %}
            {% set full = (_path + '/' if _path else '') + f.name %}
            <tr class="border-t">
              <td class="px-3 py-2 font-medium">
                <button class="text-indigo-700 hover:underline" onclick="browsePanel('{{ _bucket }}', '{{ full }}')">{{ f.name }}</button>
              </td>
              <td class="px-3 py-2">Folder</td>
              <td class="px-3 py-2">—</td>
              <td class="px-3 py-2 text-gray-500">{{ f.updated_at or '—' }}</td>
              <td class="px-3 py-2">
                <div class="flex flex-col items-end gap-2">
                  <!-- Delete -->
                  <form method="post" action="{{ url_for('delete_prefix', bucket=_bucket) }}" data-ajax-panel
                        onsubmit="return confirm('Delete folder {{ f.name }} and everything inside?');">
                    <input type="hidden" name="current_path" value="{{ full }}" />
                    <button class="rounded-lg px-3 py-1.5 border bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200" type="submit">Delete</button>
                  </form>
                  <!-- Copy -->
                  <form method="post" action="{{ url_for('transfer') }}" data-ajax-panel class="text-right">
                    <input type="hidden" name="op" value="copy" />
                    <input type="hidden" name="is_folder" value="1" />
                    <input type="hidden" name="src_bucket" value="{{ _bucket }}" />
                    <input type="hidden" name="src_path" value="{{ full }}" />
                    <div class="flex flex-col items-end gap-2">
                      <select class="rounded-lg border border-gray-300 px-2 py-1" name="dst_bucket" required>
                        {% for bn in _buckets %}
                          <option value="{{ bn }}" {% if bn==_bucket %}selected{% endif %}>{{ bn }}</option>
                        {% endfor %}
                      </select>
                      <input class="rounded-lg border border-gray-300 px-2 py-1 w-56" name="dst_path" placeholder="Target path (optional)" />
                      <button class="rounded-lg px-3 py-1.5 border bg-indigo-600 text-white border-indigo-600 hover:bg-indigo-700" type="submit">Copy</button>
                    </div>
                  </form>
                  <!-- Move -->
                  <form method="post" action="{{ url_for('transfer') }}" data-ajax-panel class="text-right">
                    <input type="hidden" name="op" value="move" />
                    <input type="hidden" name="is_folder" value="1" />
                    <input type="hidden" name="src_bucket" value="{{ _bucket }}" />
                    <input type="hidden" name="src_path" value="{{ full }}" />
                    <div class="flex flex-col items-end gap-2">
                      <select class="rounded-lg border border-gray-300 px-2 py-1" name="dst_bucket" required>
                        {% for bn in _buckets %}
                          <option value="{{ bn }}" {% if bn==_bucket %}selected{% endif %}>{{ bn }}</option>
                        {% endfor %}
                      </select>
                      <input class="rounded-lg border border-gray-300 px-2 py-1 w-56" name="dst_path" placeholder="Target path (optional)" />
                      <button class="rounded-lg px-3 py-1.5 border bg-amber-600 text-white border-amber-600 hover:bg-amber-700" type="submit">Move</button>
                    </div>
                  </form>
                </div>
              </td>
            </tr>
          {% endfor %}

          {% for f in files %}
            {% set full = (_path + '/' if _path else '') + f.name %}
            <tr class="border-t">
              <td class="px-3 py-2">{{ f.name }}</td>
              <td class="px-3 py-2">File</td>
              <td class="px-3 py-2">{{ f.size if f.size is not none else '—' }}</td>
              <td class="px-3 py-2 text-gray-500">{{ f.updated_at or '—' }}</td>
              <td class="px-3 py-2">
                <div class="flex flex-col items-end gap-2">
                  <a class="rounded-lg px-3 py-1.5 border bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200"
                     href="{{ url_for('download', bucket=_bucket, path=full) }}">Download</a>
                  <form method="post" action="{{ url_for('delete_file', bucket=_bucket) }}" data-ajax-panel
                        onsubmit="return confirm('Delete file {{ f.name }}?');">
                    <input type="hidden" name="file_path" value="{{ full }}" />
                    <button class="rounded-lg px-3 py-1.5 border bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200" type="submit">Delete</button>
                  </form>
                  <!-- Copy -->
                  <form method="post" action="{{ url_for('transfer') }}" data-ajax-panel class="text-right">
                    <input type="hidden" name="op" value="copy" />
                    <input type="hidden" name="is_folder" value="0" />
                    <input type="hidden" name="src_bucket" value="{{ _bucket }}" />
                    <input type="hidden" name="src_path" value="{{ full }}" />
                    <div class="flex flex-col items-end gap-2">
                      <select class="rounded-lg border border-gray-300 px-2 py-1" name="dst_bucket" required>
                        {% for bn in _buckets %}
                          <option value="{{ bn }}" {% if bn==_bucket %}selected{% endif %}>{{ bn }}</option>
                        {% endfor %}
                      </select>
                      <input class="rounded-lg border border-gray-300 px-2 py-1 w-56" name="dst_path" placeholder="Target path (optional)" />
                      <button class="rounded-lg px-3 py-1.5 border bg-indigo-600 text-white border-indigo-600 hover:bg-indigo-700" type="submit">Copy</button>
                    </div>
                  </form>
                  <!-- Move -->
                  <form method="post" action="{{ url_for('transfer') }}" data-ajax-panel class="text-right">
                    <input type="hidden" name="op" value="move" />
                    <input type="hidden" name="is_folder" value="0" />
                    <input type="hidden" name="src_bucket" value="{{ _bucket }}" />
                    <input type="hidden" name="src_path" value="{{ full }}" />
                    <div class="flex flex-col items-end gap-2">
                      <select class="rounded-lg border border-gray-300 px-2 py-1" name="dst_bucket" required>
                        {% for bn in _buckets %}
                          <option value="{{ bn }}" {% if bn==_bucket %}selected{% endif %}>{{ bn }}</option>
                        {% endfor %}
                      </select>
                      <input class="rounded-lg border border-gray-300 px-2 py-1 w-56" name="dst_path" placeholder="Target path (optional)" />
                      <button class="rounded-lg px-3 py-1.5 border bg-amber-600 text-white border-amber-600 hover:bg-amber-700" type="submit">Move</button>
                    </div>
                  </form>
                </div>
              </td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    </section>

    <!-- Right: actions -->
    <aside class="space-y-4">
      <section class="rounded-xl border border-gray-200 p-4 bg-white">
        <h3 class="font-semibold mb-2">New folder</h3>
        <form method="post" action="{{ url_for('mkdir', bucket=_bucket) }}" data-ajax-panel class="space-y-2">
          <input type="hidden" name="current_path" value="{{ _path }}" />
          <input class="w-full rounded-lg border border-gray-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                 type="text" name="folder_name" placeholder="folder-name" required />
          <button class="w-full rounded-lg px-4 py-2 font-medium border bg-indigo-600 text-white border-indigo-600 hover:bg-indigo-700" type="submit">Create Folder</button>
          <p class="text-xs text-gray-500">Names: letters, numbers, spaces, . _ - + # @</p>
        </form>
      </section>

      <section class="rounded-xl border border-gray-200 p-4 bg-white">
        <h3 class="font-semibold mb-2">Upload file</h3>
        <form method="post" action="{{ url_for('upload', bucket=_bucket) }}" enctype="multipart/form-data" data-ajax-panel class="space-y-2">
          <input type="hidden" name="current_path" value="{{ _path }}" />
          <input class="w-full rounded-lg border border-gray-300 px-3 py-2" type="file" name="file" required />
          <button class="w-full rounded-lg px-4 py-2 font-medium border bg-indigo-600 text-white border-indigo-600 hover:bg-indigo-700" type="submit">Upload File</button>
        </form>
      </section>
    </aside>
  </div>
</div>
"""

# ---------------------- Routes ----------------------
@app.route("/", methods=["GET"])
def home():
    return render_template_string(PAGE, buckets=get_bucket_names())

@app.route("/create-bucket", methods=["POST"])
def create_bucket():
    name = (request.form.get("bucket_name") or "").strip()
    err = validate_segment(name)
    if err:
        flash(err, "error")
        return redirect(url_for("home"))
    try:
        # compatibility across supabase-py versions
        created = False
        try:
            sb.storage.create_bucket(name)  # v2 signature, may work
            created = True
        except TypeError:
            # older: takes options dict
            sb.storage.create_bucket(name, {"public": False})
            created = True
        if created:
            flash(f"Bucket '{name}' created.", "success")
        else:
            flash("Failed to create bucket (unknown SDK signature).", "error")
    except Exception as ex:
        flash(f"Failed to create bucket: {ex}", "error")
    return redirect(url_for("home"))

@app.route("/delete-bucket/<bucket>", methods=["POST"])
def delete_bucket(bucket: str):
    try:
        empty_bucket(bucket)
        if hasattr(sb.storage, "delete_bucket"):
            sb.storage.delete_bucket(bucket)
        else:
            # Some versions expose remove_bucket
            if hasattr(sb.storage, "remove_bucket"):
                sb.storage.remove_bucket(bucket)
            else:
                raise RuntimeError("SDK does not support bucket deletion in this version.")
        flash(f"Bucket '{bucket}' deleted.", "success")
    except Exception as ex:
        flash(f"Failed to delete bucket '{bucket}': {ex}", "error")
    return redirect(url_for("home"))

@app.route("/b/<bucket>", methods=["GET"])
def browse(bucket: str):
    """Return either full page (unused by main UI) or a panel fragment when ?partial=1."""
    path = (request.args.get("path") or "").strip("/")
    partial = request.args.get("partial")
    segments = split_path(path)
    try:
        folders, files = list_items(bucket, path)
    except Exception as ex:
        flash(f"Failed to list: {ex}", "error")
        folders, files = [], []
    if partial:
        return render_template_string(
            PANEL,
            buckets=get_bucket_names(),
            bucket=bucket,
            path=path,
            segments=segments,
            folders=folders,
            files=files,
        )
    # Fallback full page rendering if someone navigates directly
    return render_template_string(
        PAGE, buckets=get_bucket_names()
    )

@app.route("/b/<bucket>/mkdir", methods=["POST"])
def mkdir(bucket: str):
    current_path = (request.form.get("current_path") or "").strip("/")
    name = (request.form.get("folder_name") or "").strip()
    err = validate_segment(name)
    if err:
        flash(err, "error")
        return ("", 200)
    full = join_path(current_path, name)
    ok, msg = ensure_placeholder_for_folder(bucket, full)
    if ok:
        flash(f"Folder '{name}' created.", "success")
    else:
        flash(msg, "error")
    return ("", 200)

@app.route("/b/<bucket>/upload", methods=["POST"])
def upload(bucket: str):
    current_path = (request.form.get("current_path") or "").strip("/")
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Please choose a file to upload.", "error")
        return ("", 200)
    filename = os.path.basename(file.filename)
    seg_err = validate_segment(filename)
    if seg_err:
        flash(seg_err, "error")
        return ("", 200)
    key = join_path(current_path, filename)
    try:
        data = file.read()
        sb.storage.from_(bucket).upload(key, data)
        flash(f"Uploaded '{filename}'", "success")
    except Exception as ex:
        flash(f"Upload failed: {ex}", "error")
    return ("", 200)

@app.route("/b/<bucket>/delete-file", methods=["POST"])
def delete_file(bucket: str):
    file_path = (request.form.get("file_path") or "").strip("/")
    try:
        sb.storage.from_(bucket).remove([file_path])
        flash(f"Deleted '{file_path}'", "success")
    except Exception as ex:
        flash(f"Delete failed: {ex}", "error")
    return ("", 200)

@app.route("/b/<bucket>/delete-prefix", methods=["POST"])
def delete_prefix(bucket: str):
    current_path = (request.form.get("current_path") or "").strip("/")
    if not current_path:
        flash("Nothing to delete at root. Use file delete instead.", "error")
        return ("", 200)
    deleted, errors = delete_prefix_recursive(bucket, current_path)
    if errors:
        more = " ..." if len(errors) > 3 else ""
        flash(f"Deleted {deleted} objects, with errors: {errors[:3]}{more}", "error")
    else:
        flash(f"Deleted {deleted} objects under '{current_path}'", "success")
    return ("", 200)

@app.route("/transfer", methods=["POST"])
def transfer():
    """
    Copy/Move files or folders.
    Form fields:
      - op: 'copy' | 'move'
      - is_folder: '1' for folder, '0' for file
      - src_bucket, src_path
      - dst_bucket
      - dst_path (optional)
    """
    op = (request.form.get("op") or "").strip().lower()
    is_folder = (request.form.get("is_folder") or "0").strip() == "1"
    src_bucket = (request.form.get("src_bucket") or "").strip()
    dst_bucket = (request.form.get("dst_bucket") or "").strip()
    src_path = (request.form.get("src_path") or "").strip("/")
    dst_path = (request.form.get("dst_path") or "").strip("/")

    if not (op in {"copy", "move"} and src_bucket and dst_bucket and src_path):
        flash("Invalid transfer request.", "error")
        return ("", 200)

    name = os.path.basename(src_path.rstrip("/"))
    # default target path: same name at root (or under provided folder)
    if dst_path:
        if is_folder:
            dst_full = join_path(dst_path, name)
        else:
            # if user passed a directory-ish dst_path (endswith '/'), keep src filename
            if dst_path.endswith("/"):
                dst_full = join_path(dst_path, name)
            else:
                dst_full = dst_path


    else:
        dst_full = name

    try:
        if is_folder:
            copied, errors = copy_folder_recursive(src_bucket, src_path, dst_bucket, dst_full)
            if errors:
                flash(f"Copied {copied} objects with some errors (showing first 3): {errors[:3]}", "error")
            else:
                flash(f"Copied folder '{src_path}' → '{dst_bucket}/{dst_full}' ({copied} objects).", "success")
            if op == "move":
                del_count, del_errors = delete_prefix_recursive(src_bucket, src_path)
                if del_errors:
                    flash(f"Move completed, but cleanup had errors: {del_errors[:3]}", "error")
                else:
                    flash(f"Moved folder. Deleted {del_count} source objects.", "success")
        else:
            # single file
            if not dst_full or dst_full.endswith("/"):
                dst_full = join_path(dst_full, name)
            copy_file(src_bucket, src_path, dst_bucket, dst_full)
            if op == "move":
                try:
                    sb.storage.from_(src_bucket).remove([src_path])
                except Exception as ex:
                    flash(f"Copied, but failed to delete source: {ex}", "error")
                    return ("", 200)
            flash(f"{op.capitalize()}ed file to '{dst_bucket}/{dst_full}'.", "success")
    except Exception as ex:
        flash(f"Transfer failed: {ex}", "error")
    return ("", 200)

@app.route("/download/<bucket>/<path:path>")
def download(bucket: str, path: str):
    try:
        data = sb.storage.from_(bucket).download(path)
        mime, _ = mimetypes.guess_type(path)
        return send_file(
            io.BytesIO(data if isinstance(data, (bytes, bytearray)) else bytes(data)),
            mimetype=mime or "application/octet-stream",
            as_attachment=True,
            download_name=os.path.basename(path),
        )
    except Exception as ex:
        flash(f"Download failed: {ex}", "error")
        return redirect(url_for("home"))

if __name__ == "__main__":
    # Use host='0.0.0.0' to expose on LAN if needed
    app.run(debug=True)
