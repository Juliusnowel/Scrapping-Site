# crawler_excel.py
import requests, os, io, re, logging, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from collections import deque

logging.basicConfig(level=logging.INFO)

HEADERS = {"User-Agent": "SiteScraperBot/1.0 (+https://example.com)"}


def clean_text(text: str):
    return re.sub(r'\s+', ' ', text or '').strip()


def same_domain(url, root):
    """Check if a URL belongs to the same domain (ignores subdomains)."""
    try:
        uhost = urlparse(url).netloc.split(":")[0].lower()
        rhost = urlparse(root).netloc.split(":")[0].lower()
        return uhost.endswith(rhost)
    except Exception:
        return False


def scrape_page(url: str):
    """Fetch a page and return structured data."""
    logging.info(f"Scraping {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"Failed {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    headings = []
    for i in range(1, 7):
        for tag in soup.find_all(f'h{i}'):
            headings.append({'tag': f'h{i}', 'text': clean_text(tag.get_text())})

    paragraphs = [{'tag': 'p', 'text': clean_text(p.get_text())}
                  for p in soup.find_all('p') if clean_text(p.get_text())]

    images = []
    for img in soup.find_all('img'):
        src = img.get('src')
        alt = clean_text(img.get('alt') or '')
        if src:
            src = urljoin(url, src)
            images.append({'src': src, 'alt': alt})

    # collect all links on the page
    links = []
    for a in soup.find_all('a', href=True):
        href = urljoin(url, a['href'])
        if href.startswith('http'):
            links.append(href)

    return {'headings': headings, 'paragraphs': paragraphs, 'images': images, 'links': links}


def save_page_to_excel(page_data: dict, page_name: str, out_dir: str):
    """Write one page's scraped data to an Excel file."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Content"

    ws.append(["Tag", "Text / Src", "Alt Text (if image)"])

    for h in page_data['headings']:
        ws.append([h['tag'], h['text'], ""])
    for p in page_data['paragraphs']:
        ws.append([p['tag'], p['text'], ""])
    for img in page_data['images']:
        ws.append(["img", img['src'], img['alt']])

    for col in range(1, 4):
        ws.column_dimensions[get_column_letter(col)].width = 60

    os.makedirs(out_dir, exist_ok=True)
    filename = os.path.join(out_dir, f"{page_name}.xlsx")
    wb.save(filename)
    logging.info(f"Saved {filename}")


def update_master_excel(all_data: list, out_dir: str):
    """Create or update the master all.xlsx combining all pages."""
    all_path = os.path.join(out_dir, "all.xlsx")
    wb = Workbook()
    ws = wb.active
    ws.title = "All Pages"
    ws.append(["Page Name", "Tag", "Text / Src", "Alt Text (if image)"])

    for page_name, pdata in all_data:
        for h in pdata['headings']:
            ws.append([page_name, h['tag'], h['text'], ""])
        for p in pdata['paragraphs']:
            ws.append([page_name, p['tag'], p['text'], ""])
        for img in pdata['images']:
            ws.append([page_name, "img", img['src'], img['alt']])

    for col in range(1, 5):
        ws.column_dimensions[get_column_letter(col)].width = 60

    wb.save(all_path)
    logging.info(f"Saved master workbook {all_path}")


def crawl_pages(start_urls: list, out_dir="output_excels", max_pages=50):
    """Breadth-first crawl: visit internal pages and export each to Excel."""
    seen = set()
    queue = deque(start_urls)
    all_data = []
    root = start_urls[0]

    while queue and len(seen) < max_pages:
        url = queue.popleft()
        if url in seen:
            continue
        if not same_domain(url, root):
            continue

        page_data = scrape_page(url)
        if not page_data:
            continue

        seen.add(url)
        page_name = urlparse(url).path.strip("/") or "index"
        page_name = page_name.replace("/", "_")
        save_page_to_excel(page_data, page_name, out_dir)
        all_data.append((page_name, page_data))

        # Add new links to queue
        for link in page_data['links']:
            if link not in seen and same_domain(link, root):
                queue.append(link)

        time.sleep(1)  # polite delay

    update_master_excel(all_data, out_dir)
    logging.info(f"Crawled {len(seen)} pages.")


if __name__ == "__main__":
    crawl_pages(["https://example.com"], max_pages=10)
