import os, csv, queue, threading, urllib.parse, pathlib, re, io, zipfile, tempfile
import requests
import logging, random, time, json
import imghdr
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from mirror_assets import AssetMirror, RateLimiter as AssetRateLimiter

# =========================
# Tunables
# =========================
DEFAULT_UA       = "SiteScraper/1.0 (+https://yourdomain.com)"

# Split rate budgets (throughput control)
PAGE_RATE_RPS    = 3.0     # pages/sec budget
IMG_RATE_RPS     = 12.0    # image probes/sec budget

CONCURRENCY      = 2
MAX_RETRIES      = 5
BACKOFF_BASE     = 1.5
LAZY_ATTRS       = ["data-src", "data-original", "data-srcset", "data-lazy", "data-img", "data-url"]

WAYBACK_CDX      = "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE     = "https://web.archive.org/web/"

IMG_PROBE_ENABLED        = True
IMG_TIMEOUT              = 10
IMG_PROBE_BYTES          = 512
MAX_IMG_PROBES_PER_PAGE  = 50    # safety valve; set 0 to disable capping

# Language gating (English only)
ONLY_ENGLISH = True
LANG_DENY = {
    "ar","bg","cs","da","de","el","es","et","fa","fi","fr","he","hi","hr","hu",
    "id","it","ja","jp","ko","ms","nl","no","pl","pt","ro","ru","sk","sl","sr",
    "sv","th","tr","uk","vi","zh","cn","tw","kr"
}
LANG_SUBDOMAIN_DENY = LANG_DENY

# =========================
# Rate limiters
# =========================
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

# Global defaults (can be overridden per run/mode)
_page_limiter = RateLimiter(PAGE_RATE_RPS)
_img_limiter  = RateLimiter(IMG_RATE_RPS)

# =========================
# Image probe cache (dedupe)
# =========================
_IMG_CACHE = {}
_IMG_CACHE_LOCK = threading.Lock()

# =========================
# Thread-safe CSV helpers
# =========================
class SafeDictWriter:
    def __init__(self, dict_writer, lock):
        self._w = dict_writer
        self._lock = lock
    def writerow(self, rowdict):
        with self._lock:
            self._w.writerow(rowdict)

class SafeWriter:
    def __init__(self, writer, lock):
        self._w = writer
        self._lock = lock
    def writerow(self, row):
        with self._lock:
            self._w.writerow(row)

# =========================
# Helpers
# =========================
def _abs_url(url, base):
    if not url: return None
    return urllib.parse.urljoin(base, url.split("#")[0].strip())

def _is_http(url):
    return url and url.startswith(("http://","https://"))

def _clean_text(s):
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def _pick_image_src(tag, base_url):
    srcset = tag.get("srcset") or tag.get("data-srcset")
    if srcset:
        first = srcset.split(",")[0].strip().split(" ")[0]
        return _abs_url(first, base_url)
    src = tag.get("src")
    if src: return _abs_url(src, base_url)
    for a in LAZY_ATTRS:
        v = tag.get(a)
        if v:
            v = v.split(",")[0].strip().split(" ")[0]
            return _abs_url(v, base_url)
    return None

def _dest_for(url, out_root):
    p = urllib.parse.urlparse(url)
    path = p.path or "/"
    if path.endswith("/"): path += "index.html"
    if not os.path.splitext(path)[1]:
        path = path.rstrip("/") + ".html"
    dest = os.path.join(out_root, p.netloc or "site", path.lstrip("/"))
    pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
    return dest

def _same_domain(url, root_netloc, allow_subdomains=False):
    try:
        host = urllib.parse.urlparse(url).netloc
        def strip_www(h): return h[4:] if h.lower().startswith("www.") else h
        host_s = strip_www(host.lower())
        root_s = strip_www(root_netloc.lower())
        if host_s == root_s:
            return True
        return allow_subdomains and host_s.endswith("." + root_s)
    except Exception:
        return False

# =========================
# English-only gating
# =========================
def _first_path_segment(path: str) -> str:
    seg = (path or "/").lstrip("/").split("/", 1)[0]
    return seg.lower()

def _looks_lang(seg: str) -> str | None:
    s = seg.lower()
    if not s: return None
    core = re.split(r"[-_]", s)[0]
    return core if core in LANG_DENY or core == "en" else None

def _english_allowed(url: str, root_netloc: str) -> bool:
    if not ONLY_ENGLISH:
        return True
    u = urllib.parse.urlparse(url)
    host = u.netloc.lower()
    def strip_www(h): return h[4:] if h.startswith("www.") else h
    host_base = strip_www(host)
    root_base = strip_www(root_netloc.lower())
    # language subdomains
    sub = host_base.split(".", 1)[0]
    if sub in LANG_SUBDOMAIN_DENY and host_base.endswith("." + root_base):
        return False
    # path language markers
    seg = _first_path_segment(u.path)
    lang = _looks_lang(seg)
    if lang is None:
        return True
    return lang == "en"

# =========================
# Wayback utilities
# =========================
def _wb_url(ts, original_url):
    return urllib.parse.urljoin(WAYBACK_BASE, f"{ts}/{original_url}")

def _wb_strip(original_or_snapshot):
    try:
        u = urllib.parse.urlparse(original_or_snapshot)
        if u.netloc != "web.archive.org":
            return original_or_snapshot
        m = re.match(r"^/web/(\d+)[^/]*/(.*)$", u.path) or re.match(r"^/web/(\d+)/(.*)$", u.path)
        if m:
            orig = m.group(2)
            if not orig.startswith(("http://","https://")):
                orig = "http://" + orig
            return orig
        return original_or_snapshot
    except Exception:
        return original_or_snapshot

def _wb_latest_timestamp(domain):
    params = {"url": domain, "output": "json", "fl": "timestamp,original,statuscode",
              "filter": "statuscode:200", "limit": 1, "from": "1996"}
    r = requests.get(WAYBACK_CDX, params=params, headers={"User-Agent": DEFAULT_UA}, timeout=20)
    r.raise_for_status()
    params["limit"] = 50000
    r = requests.get(WAYBACK_CDX, params=params, headers={"User-Agent": DEFAULT_UA}, timeout=30)
    r.raise_for_status()
    rows = r.json()[1:]
    if not rows:
        raise RuntimeError("No Wayback snapshots found.")
    return rows[-1][0]

# =========================
# Page field extraction → dict for pages.csv
# =========================
PAGES_FIELDS = [
    "page_url","status_code","content_type","is_homepage",
    "page_title","meta_title","meta_description","meta_keywords",
    "h1","h2","h3","h4","h5","h6",
    "canonical_url",
    "og_title","og_description","og_image",
    "twitter_card","twitter_title","twitter_description",
    "robots","lang_attr","word_count",
    "links_internal","links_external",
    "images_count","images_missing_alt","images",
    "video_embeds","documents","schema_types",
    "date_published","date_modified",
    "hreflang_tags","pagination_prev","pagination_next",
    "wayback_timestamp"
]

DOC_EXTS = (".pdf",".doc",".docx",".ppt",".pptx",".xls",".xlsx")

def _text_word_count(soup: BeautifulSoup) -> int:
    for tag in soup(["script","style","noscript","nav","footer","header"]):
        tag.extract()
    text = soup.get_text(separator=" ")
    return len(_clean_text(text).split())

def _meta(soup, name=None, prop=None):
    if name:
        tag = soup.find("meta", attrs={"name": name})
        return tag.get("content","").strip() if tag else ""
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
        return tag.get("content","").strip() if tag else ""
    return ""

def _collect_schema_types(soup):
    types = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list):
                for item in data:
                    t = item.get("@type")
                    if t: types.append(t if isinstance(t,str) else ",".join(t))
            elif isinstance(data, dict):
                t = data.get("@type")
                if t: types.append(t if isinstance(t,str) else ",".join(t))
        except Exception:
            pass
    for t in soup.select("[itemscope][itemtype]"):
        types.append(t.get("itemtype",""))
    return ";".join(sorted(set([_clean_text(t) for t in types if t])))

def _probe_image(session: requests.Session, img_url: str, referer: str, rate: RateLimiter,
                 timeout: int = IMG_TIMEOUT):
    headers = {
        "User-Agent": session.headers.get("User-Agent", DEFAULT_UA),
        "Accept": "image/*",
        "Referer": referer or ""
    }

    def sniff_bytes(resp):
        sample = b""
        try:
            for chunk in resp.iter_content(chunk_size=min(IMG_PROBE_BYTES, 1024)):
                if not chunk:
                    break
                sample += chunk
                if len(sample) >= IMG_PROBE_BYTES:
                    break
        except Exception:
            pass
        kind = imghdr.what(None, h=sample)
        return f"image/{kind}" if kind else ""

    try:
        rate.wait()
        r = session.head(img_url, timeout=timeout, allow_redirects=True, headers=headers)
        status = r.status_code
        ctype  = (r.headers.get("content-type") or "").lower()
        need_get = (status == 200 and not ctype) or (status == 200 and not ctype.startswith("image"))
    except (requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError):
        return (0, "", True)
    except Exception:
        return (0, "", True)
    finally:
        try: r.close()
        except Exception: pass

    if need_get:
        try:
            rate.wait()
            rg = session.get(img_url, timeout=timeout, stream=True, allow_redirects=True, headers=headers)
            status = rg.status_code
            ctype  = (rg.headers.get("content-type") or "").lower()
            if status == 200 and not ctype.startswith("image"):
                sniffed = sniff_bytes(rg)
                if sniffed:
                    ctype = sniffed
            return (status, ctype, (status >= 400) or (status == 200 and not ctype.startswith("image")))
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError):
            return (0, "", True)
        except Exception:
            return (0, "", True)
        finally:
            try: rg.close()
            except Exception: pass

    is_broken = (status >= 400) or (status == 200 and not ctype.startswith("image"))
    return (status, ctype, is_broken)

def _extract_page_fields(url, html, soup: BeautifulSoup, status, ctype, root_netloc, mode, ts):
    p = urllib.parse.urlparse(url)
    is_home = (p.netloc == root_netloc and p.path in ("","/","/index.html"))
    def join_texts(tags): return "; ".join([_clean_text(t.get_text(" ", strip=True)) for t in tags][:50])
    h = {f"h{i}": join_texts(soup.find_all(f"h{i}")) for i in range(1,7)}

    internal = external = 0
    documents = []
    for a in soup.find_all("a", href=True):
        link = _abs_url(a["href"], url)
        if not _is_http(link): continue
        if _same_domain(link, root_netloc, allow_subdomains=True): internal += 1
        else: external += 1
        if any(link.lower().endswith(ext) for ext in DOC_EXTS): documents.append(link)

    page_base = (f"{WAYBACK_BASE}{ts}/{url}") if (mode=="wayback") else url
    imgs = []
    missing_alt = 0
    for img in soup.find_all("img"):
        u = _pick_image_src(img, page_base)
        if _is_http(u):
            imgs.append(u)
        if not _clean_text(img.get("alt") or ""):
            missing_alt += 1

    hreflang = []
    for l in soup.find_all("link", rel=lambda x: x and "alternate" in x):
        if l.get("hreflang") and l.get("href"):
            hreflang.append(f'{l["hreflang"]}:{l["href"]}')
    prev_link = next_link = ""
    for l in soup.find_all("link", rel=True):
        rel = ",".join(l.get("rel"))
        if "prev" in rel:  prev_link = l.get("href","")
        if "next" in rel:  next_link = l.get("href","")

    fields = {
        "page_url": url,
        "status_code": str(status),
        "content_type": ctype,
        "is_homepage": "yes" if is_home else "",
        "page_title": _clean_text(soup.title.get_text()) if soup.title else "",
        "meta_title": _meta(soup, name="title") or _meta(soup, prop="og:title"),
        "meta_description": _meta(soup, name="description") or _meta(soup, prop="og:description"),
        "meta_keywords": _meta(soup, name="keywords"),
        **h,
        "canonical_url": (soup.find("link", rel=lambda x: x and "canonical" in x) or {}).get("href",""),
        "og_title": _meta(soup, prop="og:title"),
        "og_description": _meta(soup, prop="og:description"),
        "og_image": _meta(soup, prop="og:image"),
        "twitter_card": _meta(soup, name="twitter:card"),
        "twitter_title": _meta(soup, name="twitter:title"),
        "twitter_description": _meta(soup, name="twitter:description"),
        "robots": _meta(soup, name="robots"),
        "lang_attr": (soup.html.get("lang","") if soup.html else ""),
        "word_count": str(_text_word_count(soup)),
        "links_internal": str(internal),
        "links_external": str(external),
        "images_count": str(len(imgs)),
        "images_missing_alt": str(missing_alt),
        "images": ";".join(imgs[:200]),
        "video_embeds": ";".join([_abs_url(i.get("src",""), url) for i in soup.find_all("iframe") if i.get("src")]),
        "documents": ";".join(documents[:200]),
        "schema_types": _collect_schema_types(soup),
        "date_published": _meta(soup, prop="article:published_time") or _meta(soup, name="date"),
        "date_modified": _meta(soup, prop="article:modified_time") or _meta(soup, name="last-modified"),
        "hreflang_tags": ";".join(hreflang),
        "pagination_prev": prev_link,
        "pagination_next": next_link,
        "wayback_timestamp": ts or ""
    }
    for k in PAGES_FIELDS:
        fields.setdefault(k, "")
    return fields

# =========================
# Core crawler
# =========================
def crawl_to_zip(start_url: str, max_pages: int = 2000, concurrency: int = 8, timeout: int = 15,
                 mode: str = "live", wayback_timestamp: str | None = None,
                 allow_subdomains: bool = False) -> bytes:
    """
    mode: 'live' | 'wayback'
    wayback_timestamp: YYYYMMDDhhmmss (Wayback only; None → latest)
    """
    asset_rate = AssetRateLimiter(8.0 if mode=="live" else 2.0)
    parsed_root = urllib.parse.urlparse(start_url if start_url.startswith("http") else "http://" + start_url)
    root_netloc = parsed_root.netloc
    if mode not in ("live", "wayback"):
        raise ValueError("mode must be 'live' or 'wayback'")

    # choose limiters for this run
    page_rate = _page_limiter
    img_rate  = _img_limiter

    ts = None
    if mode == "wayback":
        ts = wayback_timestamp or _wb_latest_timestamp(root_netloc)
        logging.info(f"[wayback] using timestamp {ts} for {root_netloc}")
        timeout = max(timeout, 45)
        page_rate = RateLimiter(0.5)
        img_rate  = RateLimiter(1.0)
        concurrency = min(concurrency, 2)

    seen, in_queue, dropped = set(), set(), []
    q = queue.Queue()
    seed_url = parsed_root.geturl()

    if ONLY_ENGLISH and not _english_allowed(seed_url, root_netloc):
        raise ValueError("Seed URL is not English (path/subdomain suggests non-EN locale).")

    q.put(seed_url); in_queue.add(seed_url)

    tmpdir  = tempfile.mkdtemp(prefix="site_scrape_")
    outroot = os.path.join(tmpdir, "site")
    pathlib.Path(outroot).mkdir(parents=True, exist_ok=True)
    links_csv   = os.path.join(tmpdir, "links.csv")
    images_csv  = os.path.join(tmpdir, "images.csv")
    pages_csv   = os.path.join(tmpdir, "pages.csv")
    assets_csv  = os.path.join(tmpdir, "assets.csv")

    # files
    link_fp   = open(links_csv,  "w", newline="", encoding="utf-8")
    image_fp  = open(images_csv, "w", newline="", encoding="utf-8")
    pages_fp  = open(pages_csv,  "w", newline="", encoding="utf-8")
    assets_fp = open(assets_csv, "w", newline="", encoding="utf-8")

    # single lock shared by all writers
    write_lock = threading.Lock()

    # writers (thread-safe)
    link_writer   = SafeWriter(csv.writer(link_fp), write_lock)
    image_writer  = SafeWriter(csv.writer(image_fp), write_lock)
    pages_writer  = SafeDictWriter(csv.DictWriter(pages_fp, fieldnames=PAGES_FIELDS), write_lock)
    assets_writer = SafeWriter(csv.writer(assets_fp), write_lock)

    # headers
    with write_lock:
        csv.writer(link_fp).writerow(["page_url","link_url","link_text"])
        csv.writer(image_fp).writerow(["page_url","image_url","alt_text","status_code","content_type","is_broken"])
        csv.DictWriter(pages_fp, fieldnames=PAGES_FIELDS).writeheader()
        csv.writer(assets_fp).writerow(["asset_url","status_code","content_type","local_path"])

    def worker():
        session = requests.Session()
        session.headers.update({"User-Agent": DEFAULT_UA, "Accept": "text/html,application/xhtml+xml"})
        adapter = HTTPAdapter(pool_connections=concurrency*4, pool_maxsize=concurrency*8, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        mirror = AssetMirror(
            outroot=outroot,
            root_host=("web.archive.org" if mode=="wayback" else root_netloc),
            session=session,
            rate=asset_rate,
            assets_writer=assets_writer
        )

        while True:
            try:
                url = q.get(timeout=1)
            except queue.Empty:
                return

            if ONLY_ENGLISH and not _english_allowed(url, root_netloc):
                q.task_done()
                continue

            try:
                attempt = 0
                while True:
                    attempt += 1
                    page_rate.wait()

                    fetch_url = _wb_url(ts, url) if mode == "wayback" else url
                    session.headers["Referer"] = fetch_url

                    try:
                        r = session.get(fetch_url, timeout=timeout, allow_redirects=True)
                        status = r.status_code
                        ctype  = (r.headers.get("content-type") or "").lower()
                    except (requests.exceptions.ReadTimeout,
                            requests.exceptions.ConnectTimeout,
                            requests.exceptions.ConnectionError) as net_err:
                        sleep_for = min(int((BACKOFF_BASE ** (attempt - 1))) + random.uniform(0, 0.5), 30)
                        logging.info(f"[net-retry] {type(net_err).__name__} {fetch_url}; "
                                     f"attempt {attempt}/{MAX_RETRIES}; sleep {sleep_for:.1f}s")
                        if attempt < MAX_RETRIES:
                            time.sleep(sleep_for); continue
                        dropped.append((url, "net-timeout after retries"))
                        break

                    if status in (429, 503):
                        retry_after = r.headers.get("retry-after")
                        if retry_after and retry_after.isdigit():
                            sleep_for = min(int(retry_after), 30)
                        else:
                            sleep_for = min(int((BACKOFF_BASE ** (attempt - 1))) + random.uniform(0, 0.5), 30)
                        logging.info(f"[throttle] {status} {fetch_url}; attempt {attempt}/{MAX_RETRIES}; "
                                     f"sleep {sleep_for:.1f}s")
                        if attempt < MAX_RETRIES:
                            time.sleep(sleep_for); continue
                        dropped.append((url, f"{status} after retries"))
                        break

                    if status != 200 or "text/html" not in ctype:
                        logging.debug(f"[skip] {status} {ctype} {fetch_url}")
                        # (Optional: write a minimal pages row here)
                        break

                    html = r.text
                    if "429 Too Many Requests" in html or "temporarily restricted your access" in html:
                        sleep_for = min(int((BACKOFF_BASE ** (attempt - 1))) + random.uniform(0, 0.5), 30)
                        logging.info(f"[throttle-body] {fetch_url}; attempt {attempt}/{MAX_RETRIES}; "
                                     f"sleep {sleep_for:.1f}s")
                        if attempt < MAX_RETRIES:
                            time.sleep(sleep_for); continue
                        dropped.append((url, "429 body after retries"))
                        break

                    # Success path
                    seen.add(url)
                    dest = _dest_for(url, outroot)

                    soup = BeautifulSoup(html, "html.parser")
                    if mode == "wayback":
                        base_prefix = f"{WAYBACK_BASE}{ts}/"
                        head = soup.find("head") or soup
                        if not soup.find("base"):
                            base = soup.new_tag("base", href=base_prefix)
                            (head.insert(0, base) if head.contents else head.append(base))

                        def absolutize(tag, attr):
                            if not tag.has_attr(attr): return
                            rawv = tag.get(attr)
                            if not rawv or rawv.startswith(("http://","https://","data:","mailto:","tel:","#")):
                                return
                            if rawv.startswith("/web/"):
                                tag[attr] = "https://web.archive.org" + rawv
                                return
                            if rawv.startswith("//"):
                                tag[attr] = "https:" + rawv
                                return
                            tag[attr] = urllib.parse.urljoin(base_prefix, rawv)

                        for link in soup.find_all("link"):   absolutize(link, "href")
                        for script in soup.find_all("script"):absolutize(script, "src")
                        for img in soup.find_all("img"):      absolutize(img, "src")
                        for bar in soup.select("#wm-ipp, .wm-ipp, #playback, #wm-capinfo, #wm-toolbar"):
                            bar.decompose()

                    # ---- pages.csv row
                    row = _extract_page_fields(url, html, soup, status, ctype, root_netloc, mode, ts)
                    pages_writer.writerow(row)

                    # ---- links.csv + enqueue
                    for a in soup.find_all("a", href=True):
                        raw = a["href"]
                        if mode == "wayback":
                            if raw.startswith(("http://web.archive.org","https://web.archive.org")):
                                link_url = _wb_strip(raw)
                            elif raw.startswith("/web/"):
                                abs_wb = urllib.parse.urljoin(WAYBACK_BASE, raw.lstrip("/"))
                                link_url = _wb_strip(abs_wb)
                            else:
                                link_url = _abs_url(raw, url)
                        else:
                            link_url = _abs_url(raw, url)

                        if not _is_http(link_url): continue
                        if not _english_allowed(link_url, root_netloc): continue

                        link_writer.writerow([url, link_url, _clean_text(a.get_text())])
                        if _same_domain(link_url, root_netloc, allow_subdomains=True) and \
                           link_url not in seen and link_url not in in_queue and len(seen) < max_pages:
                            q.put(link_url); in_queue.add(link_url)

                    # ---- images.csv rows (with status & broken flag)
                    page_base = (f"{WAYBACK_BASE}{ts}/{url}") if mode == "wayback" else url
                    probed_this_page = 0
                    for img in soup.find_all("img"):
                        img_url = _pick_image_src(img, page_base)
                        if not _is_http(img_url):
                            continue
                        alt_text = _clean_text(img.get("alt") or "")
                        if IMG_PROBE_ENABLED and (MAX_IMG_PROBES_PER_PAGE <= 0 or probed_this_page < MAX_IMG_PROBES_PER_PAGE):
                            with _IMG_CACHE_LOCK:
                                cached = _IMG_CACHE.get(img_url)
                            if cached:
                                status_i, ctype_i, broken_i = cached
                            else:
                                status_i, ctype_i, broken_i = _probe_image(
                                    session, img_url, session.headers.get("Referer",""), img_rate, timeout=IMG_TIMEOUT
                                )
                                with _IMG_CACHE_LOCK:
                                    _IMG_CACHE[img_url] = (status_i, ctype_i, broken_i)
                            probed_this_page += 1
                            image_writer.writerow([url, img_url, alt_text, str(status_i), ctype_i, "yes" if broken_i else ""])
                        else:
                            image_writer.writerow([url, img_url, alt_text, "", "", ""])

                    # ---- mirror assets & rewrite references (AFTER CSV extraction)
                    try:
                        mirror.mirror_and_rewrite(
                            soup,
                            page_url=(url if mode=="live" else f"{WAYBACK_BASE}{ts}/{url}"),
                            page_dest=dest
                        )
                    except Exception as _e:
                        logging.debug(f"[mirror] asset rewrite skipped for {url}: {_e}")

                    # ---- write final HTML (always the rewritten soup)
                    with open(dest, "w", encoding="utf-8") as f:
                        f.write(str(soup))

                    break  # finished this URL

            except Exception as e:
                logging.exception(f"[error] {url}: {e}")
            finally:
                q.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for t in threads: t.start()
    q.join()
    time.sleep(0.2)

    link_fp.close(); image_fp.close(); pages_fp.close(); assets_fp.close()

    # package ZIP
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(outroot):
            for fn in files:
                fp = os.path.join(root, fn)
                arcname = os.path.relpath(fp, tmpdir)
                z.write(fp, arcname)
        z.write(links_csv,  "links.csv")
        z.write(images_csv, "images.csv")
        z.write(pages_csv,  "pages.csv")
        z.write(assets_csv, "assets.csv")
        manifest = (
            f"mode,{mode}\n"
            f"start_url,{start_url}\n"
            f"wayback_timestamp,{ts or ''}\n"
            f"pages_crawled,{len(seen)}\n"
        )
        z.writestr("manifest.csv", manifest)
        if dropped:
            fail_csv = os.path.join(tmpdir, "failures.csv")
            with open(fail_csv, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f); w.writerow(["url","reason"]); w.writerows(dropped)
            z.write(fail_csv, "failures.csv")

    logging.info(f"[crawler] finished mode={mode} ts={ts} pages_total={len(seen)}")
    return buf.getvalue()
