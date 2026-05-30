"""Source extractors for the ETL platform.

Every extractor subclasses :class:`ETLS.core.base_extractor.BaseExtractor` and
returns an :class:`ETLS.core.base_extractor.ExtractionResult`.
"""

from ETLS.Extraction_Scripts.API_Extractor import APIExtractor
from ETLS.Extraction_Scripts.DataBase_Extractor import DatabaseExtractor
from ETLS.Extraction_Scripts.ERP_SAP_Extractor import SAPODataExtractor
from ETLS.Extraction_Scripts.Excel_Extractor import ExcelExtractor

__all__ = [
    "ExcelExtractor",
    "DatabaseExtractor",
    "APIExtractor",
    "SAPODataExtractor",
]
