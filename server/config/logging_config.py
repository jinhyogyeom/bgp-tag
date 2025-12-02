"""로깅 설정 모듈"""
import logging

def setup_logging():
    """로깅 설정 - 깔끔한 출력을 위해 대부분 비활성화"""
    logging.basicConfig(level=logging.CRITICAL)
    
    # 모든 불필요한 로그 완전 비활성화
    loggers_to_disable = [
        "uvicorn.access",
        "uvicorn",
        "uvicorn.error",
        "httpx",
        "fastmcp",
        "mcp",
        "mcp.client",
        "mcp.client.streamable_http",
        "sqlalchemy",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "sqlalchemy.dialects"
    ]
    
    for logger_name in loggers_to_disable:
        logging.getLogger(logger_name).disabled = True
    
    return logging.getLogger(__name__)

