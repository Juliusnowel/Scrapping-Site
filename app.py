from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlparse
from crawler_excel import crawl_pages
import os, tempfile, logging

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


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
