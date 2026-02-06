from datetime import date, datetime
from typing import Any

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


@app.get("/")
def root() -> dict[str, object]:
    return {"service": "mae-pdf-processing-api", "status": "ok", "health": "/health", "modes": "/modes", "process": "/process"}


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/modes")
def modes() -> dict[str, list[str]]:
    return {"modes": sorted(MODE_HANDLERS.keys())}


@app.post("/process")
async def process_pdfs(
    mode: str = Form(...),
    response_format: str = Form("csv"),
    files: list[UploadFile] = File(...),
) -> Response:
    if mode not in MODE_HANDLERS:
        raise HTTPException(status_code=400, detail=f"Unsupported mode '{mode}'. Use /modes.")
    if response_format not in {"csv", "json"}:
        raise HTTPException(status_code=400, detail="Invalid response_format. Use 'csv' or 'json'.")
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
    import_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    def normalize_for_json(value: Any) -> Any:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (datetime, date, pd.Timestamp)):
            return value.isoformat()
        return value

    if response_format == "json":
        rows = []
        for record in combined_df.to_dict(orient="records"):
            rows.append({key: normalize_for_json(value) for key, value in record.items()})

        return JSONResponse(
            content={
                "import_id": import_id,
                "mode": mode,
                "row_count": len(rows),
                "rows": rows,
                "errors": errors,
            }
        )

    csv_output = combined_df.to_csv(index=False)
    output_name = f"{mode}-{import_id}.csv"

    headers = {"Content-Disposition": f'attachment; filename="{output_name}"'}
    if errors:
        headers["X-Partial-Errors"] = str(len(errors))
    return Response(content=csv_output, media_type="text/csv", headers=headers)
