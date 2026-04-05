from pydantic import BaseModel
from typing import Optional, Any
from uuid import UUID


# --- Shared primitives ---

class AliasOut(BaseModel):
    alias_id: str
    alias_name: str
    language_code: Optional[str] = None
    alias_type: Optional[str] = None


class ExternalIdOut(BaseModel):
    ext_id: str
    source_name: str
    external_key: str
    url: Optional[str] = None


class RegionOut(BaseModel):
    region_id: int
    region_name: str
    modern_country: Optional[str] = None
    region_type: Optional[str] = None


# --- GET /person/{id} ---

class PersonOut(BaseModel):
    person_id: str
    full_name: str
    birth_name: Optional[str] = None
    is_historical: bool
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    gender: Optional[str] = None
    wikidata_qid: Optional[str] = None
    birth_region: Optional[RegionOut] = None
    aliases: list[AliasOut] = []
    external_ids: list[ExternalIdOut] = []


# --- GET /person/{id}/genome ---

class GenomeOut(BaseModel):
    genome_id: str
    y_haplogroup: Optional[str] = None
    mt_haplogroup: Optional[str] = None
    coverage_depth: Optional[float] = None
    coverage_breadth: Optional[float] = None
    endogenous_dna_pct: Optional[float] = None
    damage_pattern: Optional[str] = None
    assembly_reference: Optional[str] = None
    dataset_id: Optional[int] = None
    raw_metadata: Optional[dict] = None
    ingested_at: Optional[str] = None


class PersonGenomeOut(BaseModel):
    person_id: str
    genomes: list[GenomeOut] = []


# --- GET /person/{id}/matches ---

class MatchSummaryOut(BaseModel):
    match_id: str
    counterpart_person_id: str
    counterpart_name: str
    similarity_score: Optional[float] = None
    shared_segment_count: Optional[int] = None
    total_shared_cm: Optional[float] = None
    matching_method: Optional[str] = None
    confidence_score: Optional[float] = None


class PersonMatchesOut(BaseModel):
    person_id: str
    matches: list[MatchSummaryOut] = []


# --- GET /person/{id}/ancestry ---

class AncestryNodeOut(BaseModel):
    generation: int
    person_id: str
    full_name: str
    relationship_type: Optional[str] = None
    confidence_level: Optional[float] = None
    inference_method: Optional[str] = None


class PersonAncestryOut(BaseModel):
    person_id: str
    ancestry_chain: list[AncestryNodeOut] = []


# --- GET /person/{id}/lineage ---

class DynastyMembershipOut(BaseModel):
    dynasty_id: str
    dynasty_name: str
    role: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    origin_region: Optional[str] = None


class PersonLineageOut(BaseModel):
    person_id: str
    memberships: list[DynastyMembershipOut] = []


# --- GET /match/{id} ---

class MatchDetailOut(BaseModel):
    match_id: str
    person_a_id: str
    person_a_name: str
    person_b_id: str
    person_b_name: str
    similarity_score: Optional[float] = None
    shared_segment_count: Optional[int] = None
    total_shared_cm: Optional[float] = None
    longest_segment_cm: Optional[float] = None
    snp_overlap_count: Optional[int] = None
    matching_method: Optional[str] = None
    confidence_score: Optional[float] = None
    computed_at: Optional[str] = None


# --- GET /relationship/{id} ---

class RelationshipDetailOut(BaseModel):
    relationship_id: str
    person_a_id: str
    person_a_name: str
    person_b_id: str
    person_b_name: str
    relationship_type: Optional[str] = None
    generational_distance: Optional[int] = None
    confidence_level: Optional[float] = None
    inference_method: Optional[str] = None
    supporting_evidence: Optional[dict] = None
    inferred_at: Optional[str] = None


# --- GET /haplogroup/{code}/members ---

class HaplogroupMemberOut(BaseModel):
    person_id: str
    full_name: str
    is_historical: bool
    haplogroup_exact_code: Optional[str] = None


class HaplogroupMembersOut(BaseModel):
    haplogroup_code: str
    member_count: int
    members: list[HaplogroupMemberOut] = []


# --- GET /cluster/{id} ---

class ClusterMemberOut(BaseModel):
    person_id: str
    full_name: str
    genome_id: str
    membership_probability: Optional[float] = None


class ClusterDetailOut(BaseModel):
    cluster_id: str
    cluster_code: str
    cluster_name: str
    member_count: int
    members: list[ClusterMemberOut] = []


# --- GET /historical ---

class HistoricalPersonOut(BaseModel):
    person_id: str
    full_name: str
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    birth_region: Optional[str] = None
    wikidata_qid: Optional[str] = None


class HistoricalListOut(BaseModel):
    total_count: int
    page: int
    results: list[HistoricalPersonOut] = []


# --- GET /search ---

class SearchResultOut(BaseModel):
    result_type: str
    id: str
    label: str
    rank_score: Optional[float] = None
    snippet: Optional[str] = None


class SearchOut(BaseModel):
    query: str
    results: list[SearchResultOut] = []


# --- GET /timeline ---

class TimelineEventOut(BaseModel):
    event_type: str
    event_year: Optional[int] = None
    event_year_end: Optional[int] = None
    region_name: Optional[str] = None
    certainty: Optional[str] = None


class TimelineOut(BaseModel):
    person_id: str
    events: list[TimelineEventOut] = []


# --- POST /query/nl ---

class NLQueryRequest(BaseModel):
    query: str
    api_key: str


class NLQueryOut(BaseModel):
    query: str
    generated_sql: str
    cached: bool
    results: list[dict[str, Any]] = []
    row_count: int


class NLQueryErrorOut(BaseModel):
    error: str
    reason: str