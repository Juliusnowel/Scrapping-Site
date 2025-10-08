import requests, os, re, time, logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from collections import deque
import requests
import textstat
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)
HEADERS = {"User-Agent": "SiteScraperBot/1.0 (+https://example.com)"}


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


def same_domain(url, root):
    try:
        return urlparse(url).netloc.split(":")[0].endswith(urlparse(root).netloc.split(":")[0])
    except Exception:
        return False


# ------------------------- SCRAPE MODES -------------------------

def scrape_html_content(soup, url, pattern=None):
    """Extract HTML elements: headings, paragraphs, images."""
    def match(text):
        return not pattern or (text and pattern.search(text.lower()))

    def match_alt(alt):
        return not pattern or (alt and pattern.search(alt.lower()))

    return {
        "h1": [clean(t.get_text()) for t in soup.find_all("h1") if match(t.get_text())],
        "h2": [clean(t.get_text()) for t in soup.find_all("h2") if match(t.get_text())],
        "h3": [clean(t.get_text()) for t in soup.find_all("h3") if match(t.get_text())],
        "h4": [clean(t.get_text()) for t in soup.find_all("h4") if match(t.get_text())],
        "h5": [clean(t.get_text()) for t in soup.find_all("h5") if match(t.get_text())],
        "h6": [clean(t.get_text()) for t in soup.find_all("h6") if match(t.get_text())],
        "p": [clean(p.get_text()) for p in soup.find_all("p") if match(p.get_text())],
        "images": [
            (urljoin(url, i.get("src", "")), clean(i.get("alt")))
            for i in soup.find_all("img") if i.get("src") and match_alt(i.get("alt"))
        ],
    }

def scrape_url_info(resp, soup):
    """Structured output with exact 'URL & Crawl Info' headers."""
    title = soup.title.get_text(strip=True) if soup.title else ""
    metas = {m.get("name", "").lower(): m.get("content", "") for m in soup.find_all("meta") if m.get("name")}
    canonical = [l.get("href") for l in soup.find_all("link", rel="canonical")]
    robots = metas.get("robots", "")
    desc = metas.get("description", "")
    words = len(soup.get_text().split())
    sentences = max(soup.get_text().count(".") or 1, 1)

    h2_tags = [h2.get_text(strip=True) for h2 in soup.find_all("h2")]
    h2_1 = h2_tags[0] if len(h2_tags) > 0 else ""
    h2_2 = h2_tags[1] if len(h2_tags) > 1 else ""

    meta_refresh = ""
    meta_refresh_tag = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
    if meta_refresh_tag:
        meta_refresh = meta_refresh_tag.get("content", "")

    rel_next = soup.find("link", rel="next")
    rel_prev = soup.find("link", rel="prev")
    amp_link = soup.find("link", rel="amphtml")

    all_links = [a.get("href") for a in soup.find_all("a", href=True)]
    absolute_links = [urljoin(resp.url, a) for a in all_links]
    internal_links = [u for u in absolute_links if urljoin(resp.url, "/") in u]
    external_links = [u for u in absolute_links if urljoin(resp.url, "/") not in u]

    html_length = len(resp.text)
    visible_text = len(soup.get_text())
    text_ratio = round((visible_text / html_length) * 100, 2) if html_length else 0

    try:
        flesch = round(textstat.flesch_reading_ease(soup.get_text()), 2)
        readability_label = (
            "Very Easy" if flesch > 90 else
            "Easy" if flesch > 80 else
            "Fairly Easy" if flesch > 70 else
            "Standard" if flesch > 60 else
            "Fairly Difficult" if flesch > 50 else
            "Difficult" if flesch > 30 else "Very Difficult"
        )
    except Exception:
        flesch, readability_label = "", ""

    page_size = len(resp.content)
    co2_mg = round(page_size / 1000 * 0.2, 2)
    carbon_rating = (
        "A" if co2_mg < 100 else
        "B" if co2_mg < 200 else
        "C" if co2_mg < 300 else
        "D" if co2_mg < 400 else "E"
    )

    mobile_alt = soup.find("link", rel="alternate", media=re.compile("mobile", re.I))
    semantic_similarity = len(set([w.lower() for w in title.split()]) & set([w.lower() for w in desc.split()]))

    # ---- Final structured row ----
    return {
        "URL": resp.url,
        "Content Type": resp.headers.get("Content-Type", ""),
        "Status Code": resp.status_code,
        "Status": "OK" if resp.ok else "Error",
        "Indexability": "Indexable" if "noindex" not in robots else "Noindex",
        "Indexability Status": robots,
        "Title 1": title,
        "Title 1 Length": len(title),
        "Title 1 Pixel Width": len(title) * 9,
        "Meta Description 1": desc,
        "Meta Description 1 Length": len(desc),
        "Meta Description 1 Pixel Width": len(desc) * 8,
        "Meta Description 2": metas.get("og:description", ""),
        "Meta Description 2 Length": len(metas.get("og:description", "")),
        "Meta Description 2 Pixel Width": len(metas.get("og:description", "")) * 8,
        "Meta Keywords 1": metas.get("keywords", ""),
        "Meta Keywords 1 Length": len(metas.get("keywords", "")),
        "H1-1": (soup.h1.get_text(strip=True) if soup.h1 else ""),
        "H1-1 Length": (len(soup.h1.get_text()) if soup.h1 else 0),
        "H2-1": h2_1,
        "H2-1 Length": len(h2_1),
        "H2-2": h2_2,
        "H2-2 Length": len(h2_2),
        "Meta Robots 1": robots,
        "X-Robots-Tag 1": resp.headers.get("X-Robots-Tag", ""),
        "Meta Refresh 1": meta_refresh,
        "Canonical Link Element 1": canonical[0] if canonical else "",
        "Canonical Link Element 2": canonical[1] if len(canonical) > 1 else "",
        'rel="next" 1': rel_next["href"] if rel_next else "",
        'rel="prev" 1': rel_prev["href"] if rel_prev else "",
        'HTTP rel="next" 1': "",
        'HTTP rel="prev" 1': "",
        "amphtml Link Element": amp_link["href"] if amp_link else "",
        "Size (bytes)": page_size,
        "Transferred (bytes)": page_size,
        "Total Transferred (bytes)": page_size,
        "CO2 (mg)": co2_mg,
        "Carbon Rating": carbon_rating,
        "Word Count": words,
        "Sentence Count": sentences,
        "Average Words Per Sentence": round(words / sentences, 2),
        "Flesch Reading Ease Score": flesch,
        "Readability": readability_label,
        "Text Ratio": text_ratio,
        "Crawl Depth": resp.url.count("/") - 2,
        "Folder Depth": resp.url.count("/"),
        "Link Score": len(all_links),
        "Inlinks Unique": len(internal_links),
        "Inlinks Unique JS": 0,
        "Inlinks % of Total": round((len(internal_links) / len(all_links)) * 100, 2) if all_links else 0,
        "Outlinks": len(all_links),
        "Unique Outlinks": len(set(all_links)),
        "Unique JS Outlinks": 0,
        "External Outlinks": len(external_links),
        "Unique External Outlinks": len(set(external_links)),
        "Unique External JS Outlinks": 0,
        "Closest Near Duplicate Match": "",
        "No. Near Duplicates": "",
        "Spelling Errors": 0,
        "Grammar Errors": 0,
        "Hash": hash(resp.text),
        "Response Time": resp.elapsed.total_seconds(),
        "Last Modified": resp.headers.get("Last-Modified", ""),
        "Redirect URL": resp.history[0].headers.get("Location", "") if resp.history else "",
        "Redirect Type": resp.history[0].status_code if resp.history else "",
        "Cookies Language": resp.headers.get("Content-Language", ""),
        "HTTP Version": getattr(resp.raw, "version", ""),
        "Mobile Alternate Link": mobile_alt["href"] if mobile_alt else "",
        "Closest Semantically Similar Address": canonical[0] if canonical else "",
        "Semantic Similarity Score": semantic_similarity,
        "No. Semantically Similar": len(set([semantic_similarity])),
        "Semantic Relevance Score": round(semantic_similarity / max(len(title.split()), 1), 2) if title else "",
        "URL Encoded Address": requests.utils.quote(resp.url),
        "Crawl Timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

def scrape_performance(resp, soup):
    """Structured 'Performance' output with fixed column order."""
    html_size = len(resp.text)
    img_tags = soup.find_all("img")
    js_tags = soup.find_all("script", src=True)
    css_tags = soup.find_all("link", rel="stylesheet")
    img_count = len(img_tags)

    def parse_width(val):
        if not val:
            return 0
        val = str(val).lower().replace("px", "").replace("%", "").strip()
        try:
            return int(val)
        except ValueError:
            return 0

    large_imgs = [i for i in img_tags if parse_width(i.get("width")) > 2000]

    # Check for uncompressed images
    uncompressed_images = 0
    session = requests.Session()
    session.headers.update(HEADERS)
    for img in img_tags[:5]:
        src = urljoin(resp.url, img.get("src", ""))
        try:
            r = session.head(src, timeout=10, allow_redirects=True)
            size = int(r.headers.get("Content-Length", 0))
            if size >= 500_000:
                uncompressed_images += 1
        except Exception:
            continue
    session.close()

    # Approximate JS/CSS combined asset size
    total_asset_size = 0
    checked = 0
    for tag in js_tags + css_tags:
        if checked >= 5:
            break
        src = urljoin(resp.url, tag.get("src") or tag.get("href"))
        try:
            r = requests.head(src, timeout=10, allow_redirects=True)
            size = int(r.headers.get("Content-Length", 0))
            total_asset_size += size
            checked += 1
        except Exception:
            continue

    return {
        "Slow Page": html_size > 2_000_000,
        "Large Page Size": html_size,
        "Large Image Size": len(large_imgs),
        "Uncompressed Page": not resp.headers.get("Content-Encoding"),
        "Uncompressed Images": uncompressed_images > 0,
        "Too Many Resources": img_count > 100,
        "Too Many DOM Elements": len(soup.find_all()) > 1500,
        "Excessive HTML Size": html_size > 2_000_000,
        "Excessive JS/CSS Size": total_asset_size > 2_000_000,
    }

def scrape_image_analysis(soup, url, timeout=10):
    data = []
    session = requests.Session()
    session.headers.update({"User-Agent": "SiteScraperBot/1.0 (+https://example.com)"})

    for img in soup.find_all("img"):
        src = urljoin(url, img.get("src", ""))
        alt = clean(img.get("alt", ""))
        if urlparse(src).netloc != urlparse(url).netloc:
            continue

        broken, redirected, large_file = False, False, False
        try:
            r = session.head(src, allow_redirects=True, timeout=timeout)
            broken = not r.ok
            redirected = len(r.history) > 0
            size = int(r.headers.get("Content-Length", 0))
            if size > 1_000_000:
                large_file = True
        except requests.RequestException:
            broken = True

        entry = {
            "Source URL": src,
            "Missing Alt Text": alt == "",
            "Broken Image": broken,
            "Large Image File": large_file,
            "Image with Redirect": redirected,
            "Image with No Alt Attribute": "alt" not in img.attrs,
            "Image with Empty Alt Text": alt == "",
            "Image with Non-Descriptive Alt Text": len(alt) < 5 and alt != "",
            "Alt Text": alt,
        }
        data.append(entry)

    session.close()
    return data


# ------------------------- MASTER SCRAPER -------------------------

def scrape_page(url, keywords=None, crawl_types=None):
    logging.info(f"Scraping {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"Failed to load {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    pattern = None
    if keywords:
        pattern = re.compile("|".join([re.escape(k.lower()) for k in keywords]), re.IGNORECASE)

    results = {}

    if "html" in crawl_types:
        results["html"] = scrape_html_content(soup, url, pattern)
    if "url_info" in crawl_types:
        results["url_info"] = scrape_url_info(resp, soup)
    if "performance" in crawl_types:
        results["performance"] = scrape_performance(resp, soup)
    if "images" in crawl_types:
        results["images"] = scrape_image_analysis(soup, url)

    results["links"] = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]

    return results


# ------------------------- WRITERS -------------------------

def write_excel(page_name, data, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    wb = Workbook()

    for sheet_name, sheet_data in data.items():
        ws = wb.create_sheet(title=sheet_name[:31])
        if isinstance(sheet_data, dict):
            ws.append(["Field", "Value"])
            for k, v in sheet_data.items():
                ws.append([k, str(v)])
        elif isinstance(sheet_data, list):
            if len(sheet_data) > 0 and isinstance(sheet_data[0], dict):
                ws.append(list(sheet_data[0].keys()))
                for row in sheet_data:
                    ws.append(list(row.values()))
            else:
                ws.append(["Value"])
                for val in sheet_data:
                    ws.append([val])

        for col in range(1, 10):
            ws.column_dimensions[get_column_letter(col)].width = 40

    if "Sheet" in wb.sheetnames:
        wb.remove(wb["Sheet"])

    path = os.path.join(out_dir, f"{page_name}.xlsx")
    wb.save(path)
    logging.info(f"Saved {path}")


# ✅ NEW: Write combined workbook (all.xlsx)
def write_master_excel(all_data, out_dir):
    wb = Workbook()
    ws = wb.active
    ws.title = "All Pages"
    ws.append(["Page", "Field", "Value"])

    for page_name, page_content in all_data:
        for sheet_name, sheet_data in page_content.items():
            if isinstance(sheet_data, dict):
                for k, v in sheet_data.items():
                    ws.append([page_name, f"{sheet_name}:{k}", str(v)])
            elif isinstance(sheet_data, list):
                if len(sheet_data) > 0 and isinstance(sheet_data[0], dict):
                    for row in sheet_data:
                        for k, v in row.items():
                            ws.append([page_name, f"{sheet_name}:{k}", str(v)])
                else:
                    for v in sheet_data:
                        ws.append([page_name, sheet_name, str(v)])

    for col in range(1, 4):
        ws.column_dimensions[get_column_letter(col)].width = 50

    path = os.path.join(out_dir, "all.xlsx")
    wb.save(path)
    logging.info(f"✅ Combined workbook saved → {path}")


# ------------------------- MAIN CRAWLER -------------------------

def is_allowed_language(url, root_url, language_filter):
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        if language_filter == "all":
            return True
        if language_filter == "ko":
            return path.startswith("/ko/")
        if language_filter == "ja":
            return path.startswith("/ja/")
        return not (path.startswith("/ko/") or path.startswith("/ja/") or path.startswith("/zh/"))
    except Exception:
        return True


def crawl_pages(start_urls, out_dir="output_excels", max_pages=50,
                keyword_filter="", language_filter="default", crawl_types=None):
    if crawl_types is None:
        crawl_types = ["html"]

    keywords = [k.strip() for k in keyword_filter.split(",") if k.strip()]
    seen, queue = set(), deque(start_urls)
    root = start_urls[0]
    all_data = []

    while queue and len(seen) < max_pages:
        url = queue.popleft()
        if url in seen or not same_domain(url, root):
            continue
        if not is_allowed_language(url, root, language_filter):
            logging.info(f"Skipping {url} due to language filter ({language_filter})")
            continue

        page_data = scrape_page(url, keywords, crawl_types)
        if not page_data:
            continue

        seen.add(url)
        page_name = urlparse(url).path.strip("/") or "index"
        page_name = page_name.replace("/", "_")[:80]
        write_excel(page_name, page_data, out_dir)
        all_data.append((page_name, page_data))

        # enqueue discovered links from the current page
        for link in page_data.get("links", []):
            if (
                link not in seen
                and same_domain(link, root)
                and is_allowed_language(link, root, language_filter)
            ):
                queue.append(link)

        time.sleep(0.5)

    # ✅ Write master workbook at the end
    if all_data:
        write_master_excel(all_data, out_dir)

    logging.info(f"✅ Done: Crawled {len(seen)} pages using modes {crawl_types}")
