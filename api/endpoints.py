# api/endpoints.py
from fastapi import APIRouter, File, UploadFile, Form, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from urllib.parse import quote

# 1. Импортируем нашу НОВУЮ асинхронную функцию
from services.parser import process_dataframe_async

router = APIRouter()


@router.post("/preview")
def preview_file(file: UploadFile = File(...)):
    try:
        file_contents = file.file.read()
        xls = pd.ExcelFile(io.BytesIO(file_contents))
        sheet_names = xls.sheet_names
        df = pd.read_excel(xls, sheet_name=sheet_names[0], nrows=5)
        df = df.fillna("")
        preview_html = df.to_html(classes="preview-table", index=False)
        return {"sheets": sheet_names, "preview": preview_html}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/parse")
async def parse_file(file: UploadFile = File(...), ssbe_input: str = Form(...)): # 2. Добавили async
    try:
        # 3. Читаем загруженный файл асинхронно
        file_contents = await file.read()
        xls = pd.ExcelFile(io.BytesIO(file_contents))

        target_ssbe = ssbe_input.strip()
        if target_ssbe not in xls.sheet_names:
            raise HTTPException(status_code=400, detail=f"Лист '{target_ssbe}' не найден.")

        df_actual = pd.read_excel(xls, sheet_name=target_ssbe)
        op_col = 'Наименование ОП' if 'Наименование ОП' in df_actual.columns else df_actual.columns[0]

        # 4. ВЫЗЫВАЕМ С AWAIT (Ждем, пока параллельный парсинг завершится)
        df_result = await process_dataframe_async(df_actual, op_col)

        output_buffer = io.BytesIO()
        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
            df_result.to_excel(writer, sheet_name=target_ssbe[:31], index=False)
        output_buffer.seek(0)

        safe_filename = quote(f"Анализ_ОП_{target_ssbe}.xlsx")
        headers = {'Content-Disposition': f"attachment; filename*=utf-8''{safe_filename}"}

        return StreamingResponse(
            output_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))