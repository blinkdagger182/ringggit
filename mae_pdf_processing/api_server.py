from datetime import datetime

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from api_parser import MODE_HANDLERS


app = FastAPI(title="MAE PDF Processing API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/modes")
def modes() -> dict[str, list[str]]:
    return {"modes": sorted(MODE_HANDLERS.keys())}


@app.post("/process")
async def process_pdfs(
    mode: str = Form(...),
    files: list[UploadFile] = File(...),
) -> Response:
    if mode not in MODE_HANDLERS:
        raise HTTPException(status_code=400, detail=f"Unsupported mode '{mode}'. Use /modes.")
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    parser = MODE_HANDLERS[mode]
    all_dataframes: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []

    for uploaded in files:
        filename = uploaded.filename or "uploaded.pdf"
        if not filename.lower().endswith(".pdf"):
            errors.append({"file": filename, "error": "Only PDF files are supported"})
            continue

        payload = await uploaded.read()
        if not payload:
            errors.append({"file": filename, "error": "File is empty"})
            continue

        try:
            df = parser(payload, filename)
            if df is not None and not df.empty:
                all_dataframes.append(df)
            else:
                errors.append({"file": filename, "error": "No rows extracted"})
        except Exception as exc:
            errors.append({"file": filename, "error": str(exc)})

    if not all_dataframes:
        return JSONResponse(status_code=422, content={"message": "No data extracted from uploaded PDFs.", "errors": errors})

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    csv_output = combined_df.to_csv(index=False)
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    output_name = f"{mode}-{timestamp}.csv"

    headers = {"Content-Disposition": f'attachment; filename="{output_name}"'}
    if errors:
        headers["X-Partial-Errors"] = str(len(errors))
    return Response(content=csv_output, media_type="text/csv", headers=headers)
