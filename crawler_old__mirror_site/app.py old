from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlparse
from crawler import crawl_to_zip, CONCURRENCY
import io, logging

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
    mode: str = Form("live"),              # "live" | "wayback"
    wayback_ts: str = Form("")             # optional YYYYMMDDhhmmss
):
    try:
        logging.info(f"[crawl] start url={start_url} mode={mode} ts={wayback_ts or 'latest'} max_pages={max_pages} conc={CONCURRENCY}")
        zip_bytes = crawl_to_zip(
            start_url=start_url,
            max_pages=max_pages,
            concurrency=CONCURRENCY,
            mode=mode,
            wayback_timestamp=(wayback_ts.strip() or None),
        )
        domain = urlparse(start_url).netloc.replace(":", "_") or "site"
        suffix = f"-wb{wayback_ts}" if mode == "wayback" and wayback_ts else ("-wblast" if mode=="wayback" else "")
        filename = f"site-snapshot-{domain}{suffix}.zip"
        return StreamingResponse(
            io.BytesIO(zip_bytes),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"',
                     "Content-Length": str(len(zip_bytes))}
        )
    except Exception as e:
        logging.exception("[crawl] failed")
        return RedirectResponse(url=f"/?error={str(e)}", status_code=303)

# Run: python -m uvicorn app:app --reload --port 8000
