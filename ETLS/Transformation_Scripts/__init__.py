"""Data transformers for the ETL platform.

Every transformer subclasses :class:`ETLS.core.base_transformer.BaseTransformer`
and returns a :class:`ETLS.core.base_transformer.TransformationResult`.

Quick reference
---------------
* :class:`CleaningTransformer`     — nulls, duplicates, type casting, column normalization.
* :class:`MappingTransformer`      — rename, select/drop columns, value remapping, add columns.
* :class:`AggregationTransformer`  — groupby+agg or pivot table with optional sort.
* :class:`PipelineTransformer`     — chain any of the above in sequence.
"""

from ETLS.Transformation_Scripts.aggregation_transformer import AggregationTransformer
from ETLS.Transformation_Scripts.cleaning_transformer import CleaningTransformer
from ETLS.Transformation_Scripts.mapping_transformer import MappingTransformer
from ETLS.Transformation_Scripts.pipeline_transformer import PipelineTransformer

__all__ = [
    "CleaningTransformer",
    "MappingTransformer",
    "AggregationTransformer",
    "PipelineTransformer",
]
