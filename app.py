from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlparse
from crawler_excel import crawl_pages
from shop_scraper import scrape_shop
import tempfile, logging, os, zipfile

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# NEW: marketplace UI
@app.get("/shop", response_class=HTMLResponse)
def shop_page(request: Request):
    return templates.TemplateResponse("shop.html", {"request": request})

@app.post("/crawl")
def crawl(
    start_url: str = Form(...),
    max_pages: int = Form(200),
    keyword: str = Form(""),
    language: str = Form("default"),
    page_scope: str = Form("both"),
    crawl_type: str = Form(["html"]),
    save_individual: bool = Form(False),
):
    try:
        logging.info(f"[crawl] start={start_url} keyword={keyword} lang={language} types={crawl_type}")
        tmpdir = tempfile.mkdtemp(prefix="site_scraper_")

        crawl_pages(
            [start_url],
            out_dir=tmpdir,
            max_pages=max_pages,
            keyword_filter=keyword,
            language_filter=language,
            crawl_types=[crawl_type],
            page_scope=page_scope,
            zip_results=False,
            save_individual=save_individual,
        )

        from zipfile import ZipFile
        zip_path = os.path.join(tmpdir, "site_excels.zip")
        with ZipFile(zip_path, "w") as z:
            for root, _, files in os.walk(tmpdir):
                for fn in files:
                    if fn.endswith(".xlsx"):
                        fp = os.path.join(root, fn)
                        z.write(fp, arcname=os.path.relpath(fp, start=tmpdir))

        return FileResponse(zip_path, media_type="application/zip", filename="site_excels.zip")

    except Exception as e:
        logging.exception("[crawl] failed")
        return RedirectResponse(url=f"/?error={str(e)}", status_code=303)

@app.post("/scrape_shop")
def scrape_shop_route(
    shop_url: str = Form(...),
    include_excel: bool = Form(False),
    include_images: bool = Form(False),
    product_urls: str = Form(""),   # NEW
):
    # if both unchecked, default to both
    if not include_excel and not include_images:
        include_excel = True
        include_images = True

    try:
        tmpdir = tempfile.mkdtemp(prefix="shop_scraper_")

        # Manual product URLs from textarea
        manual_urls = [u.strip() for u in product_urls.splitlines() if u.strip()]

        excel_path, images_zip = scrape_shop(
            shop_url,
            out_dir=tmpdir,
            max_pages=10,
            include_excel=include_excel,
            include_images=include_images,
            manual_product_urls=manual_urls or None,
        )

        if not excel_path and not images_zip:
            raise ValueError("No products scraped from this shop URL.")

        final_zip = os.path.join(tmpdir, "shop_data.zip")
        with zipfile.ZipFile(final_zip, "w") as z:
            if excel_path:
                z.write(excel_path, arcname=os.path.basename(excel_path))
            if images_zip:
                z.write(images_zip, arcname=os.path.basename(images_zip))

        return FileResponse(final_zip, media_type="application/zip", filename="shop_data.zip")

    except Exception as e:
        logging.exception("[scrape_shop] failed")
        return RedirectResponse(url=f"/shop?error={e}", status_code=303)