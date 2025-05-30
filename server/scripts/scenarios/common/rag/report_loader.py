#!/usr/bin/env python3
import os
import json
import pickle
from typing import List, Tuple, Optional
from dataclasses import dataclass

@dataclass
class ReportMetadata:
    scenario_type: str
    timestamp: str
    prefix: Optional[str] = None
    asn: Optional[str] = None
    risk_score: Optional[float] = None

class ReportLoader:
    def __init__(self, report_file: str, meta_file: str):
        self.report_file = report_file
        self.meta_file = meta_file

    def load_reports(self) -> Tuple[List[str], List[ReportMetadata]]:
        """보고서와 메타데이터 로드"""
        texts = self._load_reports()
        metas = self._load_metadata()
        return texts, metas

    def _load_reports(self) -> List[str]:
        """보고서 로드"""
        texts = []
        if not os.path.exists(self.report_file):
            raise FileNotFoundError(f"Report file not found: {self.report_file}")
            
        with open(self.report_file, 'r') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    rpt = rec.get('report')
                    if rpt:
                        texts.append(rpt)
                except json.JSONDecodeError:
                    continue
        return texts

    def _load_metadata(self) -> List[ReportMetadata]:
        """메타데이터 로드"""
        if not os.path.exists(self.meta_file):
            raise FileNotFoundError(f"Metadata file not found: {self.meta_file}")
            
        with open(self.meta_file, 'rb') as f:
            try:
                raw_metas = pickle.load(f)
                return [ReportMetadata(**meta) for meta in raw_metas]
            except Exception as e:
                raise RuntimeError(f"Failed to load metadata: {e}") 