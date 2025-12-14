import json
import os
import re
from functools import lru_cache
from typing import Dict, List, Set, Tuple

from query_execution import execute_query


@lru_cache(maxsize=1)
def load_prefix_data() -> List[Tuple[str, str]]:
    """Load institution/ASN pairs from prefix.json."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    prefix_file = os.path.join(current_dir, "..", "prefix.json")
    prefix_file = os.path.normpath(prefix_file)

    with open(prefix_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_text(text: str) -> str:
    return re.sub(r"[^\w가-힣]", "", text or "").lower()


def _extract_tokens(text: str) -> List[str]:
    tokens = []
    for raw in re.split(r"[\s,]+", text or ""):
        cleaned = _normalize_text(raw)
        if len(cleaned) >= 2:
            tokens.append(cleaned)
    return tokens


def _extract_as_numbers(text: str) -> Set[int]:
    matches: Set[int] = set()
    for candidate in re.findall(r"as\s*(\d+)", text or "", flags=re.IGNORECASE):
        try:
            matches.add(int(candidate))
        except ValueError:
            continue
    return matches


def resolve_prefix_lookup(user_input: str) -> Dict:
    """Match an organization in the user input to ASNs and fetch prefixes."""
    try:
        prefix_data = load_prefix_data()
    except FileNotFoundError:
        return {"success": False, "error": "prefix.json 파일을 찾을 수 없습니다."}
    except Exception as exc:
        return {"success": False, "error": str(exc)}

    if not user_input or not user_input.strip():
        return {
            "success": True,
            "query": user_input,
            "matches": [],
            "reference": prefix_data,
        }

    normalized_query = _normalize_text(user_input)
    tokens = _extract_tokens(user_input)
    explicit_as_numbers = _extract_as_numbers(user_input)

    matches = []
    seen_asns = set()

    for organization, asn_code in prefix_data:
        digits = "".join(filter(str.isdigit, asn_code))
        if not digits:
            continue
        asn = int(digits)
        normalized_name = _normalize_text(organization)

        name_hit = normalized_name and normalized_name in normalized_query
        token_hit = normalized_name and any(token in normalized_name for token in tokens)
        asn_hit = asn in explicit_as_numbers

        if not (name_hit or token_hit or asn_hit):
            continue

        if asn in seen_asns:
            continue
        seen_asns.add(asn)

        sql = """
            SELECT DISTINCT origin_as, prefix
            FROM bgp_updates
            WHERE origin_as = %s
            ORDER BY prefix
        """
        df = execute_query(sql, (asn,))
        prefix_records = df.to_dict("records") if not df.empty else []
        prefixes = [row.get("prefix") for row in prefix_records if row.get("prefix")]

        matches.append(
            {
                "organization": organization,
                "asn": asn,
                "row_count": len(prefix_records),
                "prefixes": prefixes,
            }
        )

    if not matches:
        return {
            "success": False,
            "query": user_input,
            "matches": [],
            "message": "요청과 일치하는 기관을 찾을 수 없습니다.",
            "reference_sample": prefix_data[:20],
        }

    return {
        "success": True,
        "query": user_input,
        "match_count": len(matches),
        "matches": matches,
    }
