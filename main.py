from fastapi import FastAPI
from fastapi.responses import FileResponse
from api.endpoints import router
from core.config import settings

app = FastAPI(title=settings.PROJECT_NAME)

# Подключаем наши маршруты парсинга
app.include_router(router, prefix="/api")

# Когда пользователь заходит на главную страницу, отдаем ему наш HTML
@app.get("/")
def serve_frontend():
    return FileResponse("index.html")