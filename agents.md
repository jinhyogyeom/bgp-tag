# MCP 기반 BGP 데이터 분석 시스템

이 프로젝트는 실시간 BGP 업데이트를 수집·가공하고, MCP(Multi-Modal Communication Protocol) 도구를 통해 LLM 에이전트가 데이터베이스 질의, 통계 분석, 시각화를 스스로 조합하도록 설계된 분석 플랫폼이다. 컨테이너로 구동되는 ClickHouse/TimescaleDB/Milvus 스택 위에서 FastAPI 앱과 MCP 서버가 협력하며 사용자에게 자연어 분석 경험을 제공한다.

## 시스템 스냅샷
- **데이터 수집**: `streamer/bgpstream.py`가 pybgpstream을 이용해 RIS Live 업데이트를 지속적으로 받아 ClickHouse `bgp_updates` 테이블에 적재한다. `clickhouse/schema.sql`에 테이블과 머티리얼라이즈드 뷰 정의가 포함된다.
- **배치 분석 파이프라인**: `bgp-mcp/server/scripts/run_pipeline.py`가 날짜 범위를 받아 루프/플랩/MOAS/Origin Hijack 분석 스크립트(`scripts/scenarios/*`)를 차례로 실행하고 TimescaleDB 스키마(`scripts/scenarios/ddl.sql`)에 결과를 축적한다.
- **지식 스토어**: `retriever.py`가 Milvus 컬렉션(`bgp_reports_{YYYYMMDD}`)을 조회하고 HuggingFace 임베딩 + OpenAI/Ollama LLM으로 RAG 체인을 구성한다.
- **API 오케스트레이터**: `bgp-mcp/server/main.py`는 FastAPI 애플리케이션으로 `/invoke`, `/chat`, `/chatrooms` 등을 제공한다. 기동 시 MCP 서버를 서브프로세스로 올리고, LangGraph `create_react_agent`를 통해 GPT-4o 기반 Reason+Act 에이전트를 초기화한다.
- **MCP 툴 서버**: `bgp-mcp/server/mcp/server.py`에서 FastMCP 기반 도구 모음이 노출된다. 에이전트는 HTTP(포트 8001)로 이 서버에 연결해 SQL 실행, 통계 코드 생성, 프리픽스 변환 등을 자동화한다.

## 에이전트 컴포넌트
### FastAPI + LangGraph ReAct 에이전트
1. `/invoke` 요청이 들어오면 `get_system_instructions()` 호출을 강제하는 프롬프트를 구성해 LangGraph 에이전트를 실행한다.
2. `langchain_mcp_adapters.MultiServerMCPClient`가 `bgp_analysis` 서버(HTTP, 0.0.0.0:8001/mcp/)에서 도구 스펙을 가져온다.
3. GPT-4o 기반 ReAct 루프가 사용자의 자연어 질문을 따라 필요한 MCP 도구를 연속 호출하고, 마지막 메시지를 FastAPI 응답으로 반환한다.

### RAG 챗봇
- `/chatrooms`, `/chats` 엔드포인트(`routers/chat.py`)는 `retriever.rag_chain`을 사용한다. 선택한 날짜 범위, Milvus 컬렉션, 임베딩/LLM 모델을 기반으로 문서 컨텍스트를 찾고 한국어 분석 리포트를 생성한다.
- 채팅방 메타/히스토리(`models/chat_room.py`)와 요청/응답 스키마(`models/chat.py`)는 Pydantic 모델로 관리된다.

### MCP 도구 요약
| 도구 | 설명 | 입출력 |
| --- | --- | --- |
| `get_system_instructions` | 오케스트레이터 역할과 분석 절차/가이드라인을 JSON으로 반환. | 출력: 역할/프로세스/용어 정의 JSON |
| `get_bgp_schema` | TimescaleDB 테이블 및 BGP 개념 레퍼런스. | 출력: 스키마 JSON |
| `get_sql_examples` | 자주 쓰는 질의 패턴과 Few-shot 예제. | 출력: 예제 SQL/설명 JSON |
| `execute_bgp_query` | `query_execution.execute_query`를 호출해 TimescaleDB/Timescale URI에 SQL을 실행하고 결과를 직렬화. | 입력: SQL 문자열과 파라미터 JSON, 출력: 성공 여부와 데이터 배열 |
| `get_prefix` | `mcp/prefix.json`에서 한국 기관 ↔ AS 번호 매핑을 반환해 자연어 엔티티를 AS로 치환. | 출력: [[기관, AS], …] |
| `statistics_code_generation` | `execute_bgp_query` 결과와 사용자 의도를 받아 pandas 코드 생성을 위한 메타데이터(스키마/샘플) 제공. | 입력: 질의 결과 JSON, 사용자 자연어; 출력: 코드 생성 지침 |
| `execute_statistics` | 사용자가 생성한 pandas 코드를 df에 실행하고 결과를 JSON화. | 입력: 질의 결과 JSON, 실행 코드 문자열 |
| `get_bgpplay` | `"prefix,start,end"` 입력을 받아 BGPPlay 시각화에 필요한 매개변수(ISO string)에 매핑. | 출력: `{prefix,start_time,end_time}` JSON |
| `get_visualize` | 향후 pandas DataFrame → HTML/JS 시각화 생성용. 현재는 TODO 상태로 기본 `Hello, World` 스텁만 포함되어 있다. |

## 데이터 저장소
- **ClickHouse**: 실시간 원시 업데이트(`bgp_updates`). `streamer` 컨테이너가 직접 Insert. 빠른 시계열 검색을 담당.
- **TimescaleDB(PostgreSQL)**: 정제된 이벤트(`hijack_events`, `loop_analysis_results`, `flap_analysis_results`). MCP 에이전트가 SQL을 실행하는 기본 저장소이며 `scripts/scenarios/ddl.sql`로 초기화된다.
- **Milvus**: `rag_chain`이 참조하는 벡터 스토어. 날짜별 컬렉션에 BGP 분석 리포트 조각이 저장된다.
- **파일 자산**: `routeviews_data`(원본 덤프), `prefix.json`(기관-AS 매핑), `scripts/vector_db`(벡터 적재 스크립트) 등이 에이전트의 도우미 컨텍스트로 사용된다.

## 주요 플로우
1. **스트리밍 → 저장**: `streamer` 서비스가 ClickHouse에 실시간 업데이트를 밀어넣고, 별도 파이프라인이 TimescaleDB로 정규화된 이벤트 테이블을 작성한다.
2. **MCP 질의**: 사용자가 `/invoke`로 질문 → LangGraph 에이전트 → `get_system_instructions` → (필요시) `get_prefix`, `get_sql_examples` → `execute_bgp_query` → 선택적으로 `statistics_code_generation`/`execute_statistics` → 최종 응답.
3. **RAG 챗**: `/chatrooms`에서 시간 범위를 지정하고 `/chats`에 메시지를 보내면 `retriever.rag_chain`이 Milvus에서 k개의 문서를 검색, 선택한 LLM으로 한국어 요약을 제공한다.

## 작업 메모
- `mcp/get_visualize`는 아직 완성되지 않았으므로 실제 시각화 기능을 사용하려면 구현이 필요하다.
- FastAPI 서버는 `startup_event`에서 `/app/mcp/server.py`를 subprocess로 띄우므로 Docker 이미지 빌드 시 이 경로가 유효한지 확인해야 한다.
- `execute_bgp_query`는 `TIMESCALE_URI` 환경변수(default `postgresql://postgres:postgres@timescaledb:5432/bgp_timeseries`)를 사용한다. 로컬 ClickHouse와 TimescaleDB를 동시에 운영할 경우 연결 정보를 명확히 분리해야 한다.

이 문서는 에이전트가 호출하는 도구와 백엔드 파이프라인의 상관관계를 파악할 수 있도록 작성되었다. 신규 도구 추가나 지침 변경 시 이 파일을 함께 업데이트해 두면 유지보수에 도움이 된다.
