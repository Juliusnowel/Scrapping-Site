# mirror_assets.py
import os, re, urllib.parse, pathlib, time, threading
import requests, json
from bs4 import BeautifulSoup

WAYBACK_HOST = "web.archive.org"

# -------------------------
# Simple rate limiter
# -------------------------
class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(rps, 0.1)
        self._next = time.monotonic()
        self._lock = threading.Lock()
    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(self._next + self.interval, time.monotonic())

# -------------------------
# Utility functions / regexes
# -------------------------
CSS_URL_RE = re.compile(r"url\(\s*(['\"]?)([^)'\"]+)\1\s*\)", re.IGNORECASE)
CSS_IMP_RE = re.compile(r"@import\s+(?:url\()?['\"]?([^'\"\)]+)['\"]?\)?", re.IGNORECASE)
WB_PATH_RE = re.compile(r"^/web/\d{1,14}[^/]*/(https?:\/\/.+)$", re.IGNORECASE)

def _abs_url(url, base):
    if not url: return None
    return urllib.parse.urljoin(base, url.split("#")[0].strip())

def _is_http(u):
    return u and u.startswith(("http://", "https://"))

def _ensure_dir(p):
    pathlib.Path(p).parent.mkdir(parents=True, exist_ok=True)

def _clean_rel(p):
    # Normalize to / even on Windows
    return p.replace(os.sep, "/")

def _wb_extract_original(abs_url: str):
    """
    If abs_url is a Wayback URL, extract the original URL inside /web/<ts>/ORIG.
    Return (host, path) for local storage or None.
    """
    u = urllib.parse.urlparse(abs_url)
    if u.netloc.lower() != WAYBACK_HOST:
        return None
    m = WB_PATH_RE.match(u.path)
    if not m:
        return None
    orig = m.group(1)
    try:
        ou = urllib.parse.urlparse(orig)
        return (ou.netloc.lower() or "site", ou.path or "/")
    except Exception:
        return None

# -------------------------
# Asset mirrorer
# -------------------------
class AssetMirror:
    """
    Downloads front-end assets (css/js/img/fonts), rewrites HTML & CSS to local paths,
    and logs each asset to an optional CSV writer.
    """
    # Look for //# sourceMappingURL=... or /*# sourceMappingURL=... */
    SMAP_RE = re.compile(r'(?:^|\n)[/@#]\s*sourceMappingURL\s*=\s*([^\s]+)', re.IGNORECASE)

    def __init__(
        self,
        outroot: str,
        root_host: str,
        session: requests.Session,
        rate: RateLimiter,
        assets_writer=None,
        allow_offsite: bool = True,
        cdn_allowlist = (
            "fonts.googleapis.com",
            "fonts.gstatic.com",
            "cdnjs.cloudflare.com",
            "ajax.googleapis.com",
            "cdn.jsdelivr.net",
            "use.fontawesome.com",
            "stackpath.bootstrapcdn.com",
            "unpkg.com",
        ),
    ):
        self.outroot = outroot
        self.root_host = (root_host or "").lower()
        self.session = session
        self.rate = rate
        self.writer = assets_writer
        self.allow_offsite = allow_offsite
        self.cdn_allow = set(cdn_allowlist)
        self._seen = {}  # abs_url -> local_path

    # -------- mapping url -> local file ----------
    def _local_path_for(self, abs_url: str) -> str:
        """
        Prefer original host/path even when downloading from Wayback.
        """
        wb = _wb_extract_original(abs_url)
        if wb:
            host, path = wb
        else:
            u = urllib.parse.urlparse(abs_url)
            host = (u.netloc or "site").lower()
            path = u.path or "/"
        if path.endswith("/"):
            path += "index"
        return os.path.join(self.outroot, host, path.lstrip("/"))

    # -------- fetching ----------
    def _fetch_to_file(self, abs_url: str):
        """
        Download abs_url if not cached. Returns (status, content_type, local_path or "").
        """
        if abs_url in self._seen:
            return (200, "", self._seen[abs_url])

        # host policy (always allow same host; optionally allow CDNs/offsite; Wayback allowed)
        host = urllib.parse.urlparse(abs_url).netloc.lower()
        same_host = (host == self.root_host or host.endswith("." + self.root_host))
        if not (same_host or self.allow_offsite or host in self.cdn_allow or host == WAYBACK_HOST):
            return (0, "", "")

        self.rate.wait()
        try:
            r = self.session.get(abs_url, stream=True, timeout=20, allow_redirects=True)
        except Exception:
            return (0, "", "")
        status = r.status_code
        ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

        if status >= 400:
            try: r.close()
            except: pass
            return (status, ctype, "")

        local_path = self._local_path_for(abs_url)
        root, ext = os.path.splitext(local_path)
        if not ext:
            if "text/css" in ctype:
                local_path = root + ".css"
            elif "javascript" in ctype or ctype == "application/x-javascript":
                local_path = root + ".js"

        _ensure_dir(local_path)
        try:
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        finally:
            try: r.close()
            except: pass

        self._seen[abs_url] = local_path

        if self.writer:
            try:
                self.writer.writerow([abs_url, str(status), ctype, os.path.relpath(local_path, self.outroot)])
            except Exception:
                pass

        # Try to pull source maps & original sources for JS/CSS
        if local_path.lower().endswith((".js", ".css")):
            self._maybe_fetch_sourcemap(abs_url, local_path)

        # If CSS, rewrite imports and url() inside (after saving)
        if local_path.lower().endswith(".css"):
            self._rewrite_css_file(abs_url, local_path)

        return (status, ctype, local_path)

    # -------- source map expansion ----------
    def _maybe_fetch_sourcemap(self, abs_url: str, local_path: str):
        try:
            size = os.path.getsize(local_path)
            with open(local_path, "rb") as f:
                if size > 200_000:
                    f.seek(-200_000, os.SEEK_END)  # read last 200KB
                tail = f.read().decode("utf-8", errors="ignore")
            m = self.SMAP_RE.search(tail)
            if not m:
                return
            sm_rel = m.group(1).strip()
            sm_abs = _abs_url(sm_rel, abs_url)
            if not _is_http(sm_abs):
                return

            self.rate.wait()
            r = self.session.get(sm_abs, timeout=20, allow_redirects=True)
            if r.status_code != 200:
                return
            sm_json = r.json()
        except Exception:
            return
        finally:
            try: r.close()
            except Exception: pass

        # Save map next to file
        sm_local = local_path + ".map"
        _ensure_dir(sm_local)
        try:
            with open(sm_local, "w", encoding="utf-8") as f:
                json.dump(sm_json, f)
        except Exception:
            pass

        # Write original sources if present
        sources = sm_json.get("sources", []) or []
        sources_content = sm_json.get("sourcesContent", []) or []
        base_dir = os.path.join(self.outroot, "_sources")
        if sources_content and len(sources) == len(sources_content):
            for src, content in zip(sources, sources_content):
                try:
                    src_abs = _abs_url(src, sm_abs)
                    u = urllib.parse.urlparse(src_abs)
                    dest = os.path.join(base_dir, (u.netloc or "site"), u.path.lstrip("/"))
                    _ensure_dir(dest)
                    with open(dest, "w", encoding="utf-8", errors="ignore") as f:
                        f.write(content if content is not None else "")
                except Exception:
                    pass
        else:
            # No inline content—try to fetch each source
            for src in sources:
                try:
                    src_abs = _abs_url(src, sm_abs)
                    if not _is_http(src_abs):
                        continue
                    self.rate.wait()
                    rr = self.session.get(src_abs, timeout=20, allow_redirects=True)
                    if rr.status_code == 200:
                        u = urllib.parse.urlparse(src_abs)
                        dest = os.path.join(base_dir, (u.netloc or "site"), u.path.lstrip("/"))
                        _ensure_dir(dest)
                        with open(dest, "wb") as f:
                            f.write(rr.content)
                except Exception:
                    pass

    # -------- CSS rewriting ----------
    def _rewrite_css_file(self, css_abs_url: str, local_css_path: str):
        try:
            with open(local_css_path, "r", encoding="utf-8", errors="ignore") as f:
                css = f.read()
        except Exception:
            return

        base_dir = os.path.dirname(local_css_path)

        def repl_url(m):
            raw = m.group(2).strip()
            if raw.startswith(("data:", "about:", "javascript:")):
                return m.group(0)
            abs_u = _abs_url(raw, css_abs_url)
            if not _is_http(abs_u):
                return m.group(0)
            _, _, asset_local = self._fetch_to_file(abs_u)
            if not asset_local:
                return m.group(0)
            rel = _clean_rel(os.path.relpath(asset_local, base_dir))
            return f"url({rel})"

        def repl_import(m):
            raw = m.group(1).strip()
            if raw.startswith(("data:", "about:", "javascript:")):
                return m.group(0)
            abs_u = _abs_url(raw, css_abs_url)
            if not _is_http(abs_u):
                return m.group(0)
            _, ctype, asset_local = self._fetch_to_file(abs_u)
            if not asset_local:
                return m.group(0)
            if (asset_local.lower().endswith(".css")) or ("text/css" in ctype):
                self._rewrite_css_file(abs_u, asset_local)
            rel = _clean_rel(os.path.relpath(asset_local, base_dir))
            return f"@import url({rel})"

        css2 = CSS_URL_RE.sub(repl_url, css)
        css2 = CSS_IMP_RE.sub(repl_import, css2)

        try:
            with open(local_css_path, "w", encoding="utf-8") as f:
                f.write(css2)
        except Exception:
            pass

    # -------- HTML rewriting ----------
    def _mirror_and_rewrite_attr(self, tag, attr, page_url, page_dest):
        raw = tag.get(attr)
        if not raw:
            return
        abs_u = _abs_url(raw, page_url)
        if not _is_http(abs_u):
            return
        _, _, local_path = self._fetch_to_file(abs_u)
        if not local_path:
            return
        html_dir = os.path.dirname(page_dest)
        rel = _clean_rel(os.path.relpath(local_path, html_dir))
        tag[attr] = rel

        # Local files don't need SRI/CORS; may break loading
        for bad in ("integrity", "crossorigin"):
            if tag.has_attr(bad):
                del tag[bad]

    def _rewrite_inline_style(self, tag, page_url, page_dest):
        style = tag.get("style")
        if not style:
            return
        def repl(m):
            raw = m.group(2).strip()
            if raw.startswith(("data:", "about:", "javascript:")):
                return m.group(0)
            abs_u = _abs_url(raw, page_url)
            if not _is_http(abs_u):
                return m.group(0)
            _, _, local_path = self._fetch_to_file(abs_u)
            if not local_path:
                return m.group(0)
            html_dir = os.path.dirname(page_dest)
            rel = _clean_rel(os.path.relpath(local_path, html_dir))
            return f"url({rel})"
        tag["style"] = CSS_URL_RE.sub(repl, style)

    def mirror_and_rewrite(self, soup: BeautifulSoup, page_url: str, page_dest: str):
        # Stylesheets, icons, and preloads
        for link in soup.find_all("link", href=True):
            rels = {r.lower() for r in (link.get("rel") or [])}
            as_attr = (link.get("as") or "").lower()
            href_lc = link["href"].split("?", 1)[0].lower()
            typ = (link.get("type") or "").lower()

            is_stylesheet = (
                ("stylesheet" in rels) or
                (as_attr == "style") or
                (typ == "text/css") or
                href_lc.endswith(".css")
            )
            is_icon = ("icon" in rels) or ("shortcut icon" in rels)
            is_preload = ("preload" in rels and as_attr in {"style", "script", "font"})

            if is_stylesheet or is_icon or is_preload:
                self._mirror_and_rewrite_attr(link, "href", page_url, page_dest)

        # Scripts
        for s in soup.find_all("script", src=True):
            self._mirror_and_rewrite_attr(s, "src", page_url, page_dest)

        # Images/sources/srcset
        for tag in soup.find_all(["img", "source", "embed", "track"]):
            for a in ("srcset", "src", "data-src"):
                if not tag.has_attr(a):
                    continue
                if a == "srcset":
                    items = []
                    for part in tag["srcset"].split(","):
                        p = part.strip()
                        if not p:
                            continue
                        url_part = p.split(" ")[0]
                        abs_u = _abs_url(url_part, page_url)
                        if _is_http(abs_u):
                            _, _, lp = self._fetch_to_file(abs_u)
                            if lp:
                                rel = _clean_rel(os.path.relpath(lp, os.path.dirname(page_dest)))
                                rest = p[len(url_part):]
                                items.append(f"{rel}{rest}")
                                continue
                        items.append(p)
                    tag["srcset"] = ", ".join(items)
                else:
                    self._mirror_and_rewrite_attr(tag, a, page_url, page_dest)

        # Posters
        for v in soup.find_all("video"):
            if v.has_attr("poster"):
                self._mirror_and_rewrite_attr(v, "poster", page_url, page_dest)

        # Inline style attributes
        for a in soup.find_all(["div", "section", "span", "header", "footer", "figure", "li", "p"]):
            if a.has_attr("style"):
                self._rewrite_inline_style(a, page_url, page_dest)

        # Inline <style> blocks
        for st in soup.find_all("style"):
            if not st.string:
                continue
            css_abs = page_url
            def repl(m):
                raw = m.group(2).strip()
                if raw.startswith(("data:", "about:", "javascript:")):
                    return m.group(0)
                abs_u = _abs_url(raw, css_abs)
                if not _is_http(abs_u):
                    return m.group(0)
                _, _, lp = self._fetch_to_file(abs_u)
                if not lp:
                    return m.group(0)
                rel = _clean_rel(os.path.relpath(lp, os.path.dirname(page_dest)))
                return f"url({rel})"
            st.string = CSS_URL_RE.sub(repl, st.string)
