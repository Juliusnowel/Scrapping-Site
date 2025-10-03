# mirror_assets.py
import os, re, hashlib, pathlib, urllib.parse, threading
from typing import Tuple
import requests

# Controls
MIRROR_EXTERNAL_ASSETS = False        # False = same-origin only
ASSET_RATE_RPS = 8.0                  # throttle for assets
CSS_URL_RE = re.compile(r"url\(\s*(['\"]?)([^)\"']+)\1\s*\)", re.IGNORECASE)

IMAGE_EXT = {".png",".jpg",".jpeg",".gif",".webp",".svg",".ico",".bmp",".tif",".tiff",".avif"}
CSS_EXT   = {".css"}
JS_EXT    = {".js",".mjs"}
FONT_EXT  = {".woff",".woff2",".ttf",".otf",".eot"}
MEDIA_EXT = {".mp4",".webm",".mp3",".ogg",".wav",".mov",".avi",".m4a"}

class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(rps, 0.1)
        self._next = 0.0
        self._lock = threading.Lock()
    def wait(self, now_fn):
        with self._lock:
            now = now_fn()
            if now < self._next:
                import time; time.sleep(self._next - now)
            import time
            self._next = max(self._next + self.interval, time.time())

def _is_http(u:str)->bool:
    return u and u.startswith(("http://","https://"))

def _same_host_or_sub(host: str, root: str)->bool:
    h = host.split(":")[0].lower()
    r = root.split(":")[0].lower()
    if h == r: return True
    return h.endswith("." + r)

def _ext(path: str)->str:
    return os.path.splitext(path)[1].lower()

def _norm_join(base, url):
    return urllib.parse.urljoin(base, url.split("#")[0].strip())

def _ensure_parent(p):
    pathlib.Path(p).parent.mkdir(parents=True, exist_ok=True)

def _pick_bucket(url_path: str):
    e = _ext(url_path)
    if e in CSS_EXT: return "css"
    if e in JS_EXT:  return "js"
    if e in IMAGE_EXT: return "img"
    if e in FONT_EXT:  return "font"
    if e in MEDIA_EXT: return "media"
    return "misc"

class AssetMirror:
    """
    Mirrors CSS/JS/images/fonts/media and rewrites HTML + CSS to local relative paths.
    """
    def __init__(self, outroot: str, root_host: str, session: requests.Session,
                 rate: RateLimiter, assets_writer):
        self.outroot = outroot                       # /tmp/.../site
        self.root_host = root_host                   # example.com
        self.sess = session
        self.rate = rate
        self.assets_writer = assets_writer           # csv.writer or None
        self.map = {}                                # url -> relpath (from outroot)
        self.lock = threading.Lock()

    def _allowed(self, url: str)->bool:
        if MIRROR_EXTERNAL_ASSETS:
            return True
        host = urllib.parse.urlparse(url).netloc
        return _same_host_or_sub(host, self.root_host)

    def _abs_to_rel(self, abs_path: str, page_dest: str)->str:
        # abs_path is under outroot; we need a rel path from the page_dest folder
        return os.path.relpath(abs_path, start=os.path.dirname(page_dest))

    def _target_for(self, url: str)->Tuple[str,str]:
        u = urllib.parse.urlparse(url)
        # Preserve original path under host bucket for predictability
        subdir = _pick_bucket(u.path or "/")
        rel = os.path.join(u.netloc or "site", (u.path or "/").lstrip("/"))
        # Avoid dir endings
        if rel.endswith("/") or not _ext(rel):
            rel = rel.rstrip("/") + "/index"
            # append an extension guess by bucket
            rel += ".css" if subdir=="css" else ".js" if subdir=="js" else ".bin"
        abs_path = os.path.join(self.outroot, rel)
        return abs_path, rel

    def _write_and_log(self, url, abs_path, body: bytes, status: int, ctype: str):
        _ensure_parent(abs_path)
        with open(abs_path, "wb") as f:
            f.write(body or b"")
        if self.assets_writer:
            self.assets_writer.writerow([url, status, ctype, os.path.relpath(abs_path, self.outroot)])

    def _fetch(self, url: str):
        # HEAD → maybe GET; honor limiter
        import time
        self.rate.wait(time.time)
        try:
            r = self.sess.get(url, timeout=20, allow_redirects=True, stream=False)
            status = r.status_code
            ctype  = (r.headers.get("content-type") or "").lower()
            body   = r.content if status == 200 else b""
        except Exception:
            return 0, "", b""
        return status, ctype, body

    def _mirror_binary(self, url: str, page_dest: str)->str:
        abs_path, rel = self._target_for(url)
        with self.lock:
            if url in self.map:
                return self._abs_to_rel(os.path.join(self.outroot, self.map[url]), page_dest)

        status, ctype, body = self._fetch(url)
        if status == 200 and body:
            self._write_and_log(url, abs_path, body, status, ctype)
            with self.lock: self.map[url] = rel
            return self._abs_to_rel(abs_path, page_dest)

        # Log failure, keep original URL
        if self.assets_writer:
            self.assets_writer.writerow([url, status, ctype, ""])
        return url

    def _rewrite_css(self, css_text: str, css_url: str, css_abs_path: str)->bytes:
        # For each url(...) inside CSS, mirror and rewrite relative to this CSS file location.
        base = css_url
        root_dir = os.path.dirname(css_abs_path)

        def repl(m):
            token = m.group(2).strip()
            if not token or token.startswith("data:") or token.startswith("blob:") or token.startswith("mailto:"):
                return m.group(0)
            abs_u = _norm_join(base, token)
            if not _is_http(abs_u) or not self._allowed(abs_u):
                return m.group(0)
            # mirror dependency
            child_abs, child_rel = self._target_for(abs_u)
            # Avoid double fetch if already known
            with self.lock:
                known = self.map.get(abs_u)
            if not known:
                status, ctype, body = self._fetch(abs_u)
                if status == 200 and body:
                    _ensure_parent(child_abs)
                    with open(child_abs, "wb") as f: f.write(body)
                    with self.lock: self.map[abs_u] = child_rel
                    if self.assets_writer:
                        self.assets_writer.writerow([abs_u, status, ctype, os.path.relpath(child_abs, self.outroot)])
                else:
                    if self.assets_writer:
                        self.assets_writer.writerow([abs_u, status, ctype, ""])
                    return m.group(0)

            # rewrite to rel path from CSS file
            repl_path = os.path.relpath(os.path.join(self.outroot, self.map.get(abs_u, child_rel)), root_dir)
            return f"url({repl_path})"

        rewritten = CSS_URL_RE.sub(repl, css_text)
        return rewritten.encode("utf-8", errors="ignore")

    def _mirror_css(self, url: str, page_dest: str)->str:
        abs_path, rel = self._target_for(url)
        with self.lock:
            if url in self.map:
                return self._abs_to_rel(os.path.join(self.outroot, self.map[url]), page_dest)

        status, ctype, body = self._fetch(url)
        if status == 200 and body:
            # rewrite nested urls
            try:
                text = body.decode("utf-8", errors="ignore")
            except Exception:
                text = ""
            body_final = self._rewrite_css(text, url, abs_path)
            self._write_and_log(url, abs_path, body_final, status, ctype)
            with self.lock: self.map[url] = rel
            return self._abs_to_rel(abs_path, page_dest)

        if self.assets_writer:
            self.assets_writer.writerow([url, status, ctype, ""])
        return url

    def mirror_and_rewrite(self, soup, page_url: str, page_dest: str):
        """
        - Mirrors <link rel=stylesheet>, <script src>, <img>, <source>, <video poster>, <audio>, icons, etc.
        - Rewrites attributes to local relative paths when allowed; leaves others as-is.
        """
        def handle(tag, attr, is_css=False):
            raw = tag.get(attr)
            if not raw: return
            u = _norm_join(page_url, raw)
            if not _is_http(u) or not self._allowed(u): return
            # CSS vs binary
            newv = self._mirror_css(u, page_dest) if (is_css or u.endswith(".css")) else self._mirror_binary(u, page_dest)
            if newv: tag[attr] = newv

        # CSS
        for l in soup.find_all("link", href=True):
            rel = (",".join(l.get("rel") or [])).lower()
            if "stylesheet" in rel or _ext(l["href"]).lower() in CSS_EXT:
                handle(l, "href", is_css=True)
            if any(x in rel for x in ("icon","shortcut icon","apple-touch-icon")):
                handle(l, "href")

        # JS
        for s in soup.find_all("script", src=True):
            handle(s, "src")

        # IMG + srcset (first only)
        for img in soup.find_all("img"):
            if img.get("src"): handle(img, "src")
            if img.get("srcset"):
                first = img["srcset"].split(",")[0].strip().split(" ")[0]
                if first: 
                    newv = self._mirror_binary(_norm_join(page_url, first), page_dest)
                    img["srcset"] = newv

        # <source>, <video>, <audio>, posters
        for src in soup.find_all(["source","video","audio"]):
            if src.get("src"): handle(src, "src")
            if src.name == "video" and src.get("poster"): handle(src, "poster")

        # Inline styles with url(...) — light-touch scan
        for t in soup.find_all(style=True):
            style = t.get("style") or ""
            if "url(" not in style: 
                continue
            # create a fake css file co-located with page to compute rel paths
            page_fake_css = urllib.parse.urljoin(page_url, "./__inline__.css")
            page_css_abs  = os.path.join(os.path.dirname(page_dest), "__inline__.css")
            rewritten = self._rewrite_css(style, page_fake_css, page_css_abs).decode("utf-8","ignore")
            t["style"] = rewritten
