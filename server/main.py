from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import chat
import dotenv

dotenv.load_dotenv()

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat.router)
