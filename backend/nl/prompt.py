SCHEMA_CONTEXT = """
TABLE person (person_id UUID PK, full_name TEXT, birth_name TEXT, is_historical BOOLEAN,
  birth_year INTEGER, death_year INTEGER, birth_region_id INTEGER FK→region, gender TEXT,
  wikidata_qid TEXT, bio_text TEXT, bio_tsv TSVECTOR, source_dataset_id INTEGER FK→dataset_source)

TABLE person_alias (alias_id UUID PK, person_id UUID FK→person, alias_name TEXT,
  language_code TEXT, alias_type TEXT)

TABLE person_external_id (ext_id UUID PK, person_id UUID FK→person, source_name TEXT,
  external_key TEXT, url TEXT)

TABLE genome (genome_id UUID PK, person_id UUID FK→person, dataset_id INTEGER FK→dataset_source,
  y_haplogroup TEXT, mt_haplogroup TEXT, coverage_depth NUMERIC, coverage_breadth NUMERIC,
  endogenous_dna_pct NUMERIC, damage_pattern TEXT, assembly_reference TEXT, raw_metadata JSONB)

TABLE snp_marker (snp_id UUID PK, rs_id TEXT UNIQUE, chromosome TEXT, position_grch38 BIGINT,
  ref_allele TEXT, alt_allele TEXT, gene_context TEXT, clinical_significance TEXT,
  population_freq JSONB)

TABLE genome_snp (genome_snp_id UUID PK, genome_id UUID FK→genome, snp_id UUID FK→snp_marker,
  observed_genotype TEXT, quality_score NUMERIC, is_derived BOOLEAN)

TABLE haplogroup_reference (haplogroup_id UUID PK, haplogroup_code TEXT UNIQUE,
  haplogroup_type TEXT CHECK('Y-DNA','mtDNA'), parent_haplogroup_id UUID FK→self,
  defining_snp TEXT, age_estimate_ybp INTEGER, geographic_origin TEXT)

TABLE genome_match (match_id UUID PK, genome_a_id UUID FK→genome, genome_b_id UUID FK→genome,
  similarity_score NUMERIC, shared_segment_count INTEGER, total_shared_cm NUMERIC,
  longest_segment_cm NUMERIC, snp_overlap_count INTEGER,
  matching_method TEXT CHECK('ibd','haplogroup','pca','admixture'),
  confidence_score NUMERIC, computed_at TIMESTAMPTZ)

TABLE population_cluster (cluster_id UUID PK, cluster_code TEXT UNIQUE, cluster_name TEXT,
  haplogroup_signature TEXT[], geographic_centroid_lat NUMERIC, geographic_centroid_lon NUMERIC,
  time_period_start INTEGER, time_period_end INTEGER, source_dataset_id INTEGER FK→dataset_source,
  description TEXT)

TABLE genome_cluster_assignment (assignment_id UUID PK, genome_id UUID FK→genome,
  cluster_id UUID FK→population_cluster, membership_probability NUMERIC,
  assignment_method TEXT, assigned_at TIMESTAMPTZ)

TABLE inferred_relationship (relationship_id UUID PK, person_a_id UUID FK→person,
  person_b_id UUID FK→person, match_id UUID FK→genome_match,
  relationship_type TEXT CHECK('ancestor','descendant','sibling','cousin',
    'haplogroup_shared','population_cluster','distant_relative'),
  generational_distance INTEGER, confidence_level NUMERIC,
  inference_method TEXT CHECK('ibd_segment','haplogroup_tree','pca_proximity','rule_based'),
  supporting_evidence JSONB, inferred_at TIMESTAMPTZ)

TABLE dynasty (dynasty_id UUID PK, dynasty_name TEXT, founding_year INTEGER,
  dissolution_year INTEGER, origin_region_id INTEGER FK→region, wikidata_qid TEXT,
  description TEXT, description_tsv TSVECTOR)

TABLE dynasty_membership (membership_id UUID PK, person_id UUID FK→person,
  dynasty_id UUID FK→dynasty, role TEXT, start_year INTEGER, end_year INTEGER,
  is_founding_member BOOLEAN, source_dataset_id INTEGER FK→dataset_source)

TABLE region (region_id SERIAL PK, region_name TEXT, modern_country TEXT,
  region_type TEXT CHECK('country','province','empire','geographic_zone'),
  centroid_lat NUMERIC, centroid_lon NUMERIC, geojson_outline JSONB,
  parent_region_id INTEGER FK→self)

TABLE person_location_timeline (location_event_id UUID PK, person_id UUID FK→person,
  region_id INTEGER FK→region,
  event_type TEXT CHECK('birth','death','ruled','migration','residence'),
  event_year INTEGER, event_year_end INTEGER,
  certainty TEXT CHECK('confirmed','estimated','disputed'),
  source_dataset_id INTEGER FK→dataset_source)

TABLE historical_conflict (conflict_id UUID PK, conflict_name TEXT, start_year INTEGER,
  end_year INTEGER, region_id INTEGER FK→region,
  conflict_type TEXT CHECK('war','civil_war','battle','campaign'),
  initiator_faction TEXT, target_faction TEXT,
  source_dataset_id INTEGER FK→dataset_source)

TABLE dataset_source (dataset_id SERIAL PK, dataset_name TEXT UNIQUE, short_code TEXT UNIQUE,
  description TEXT, description_tsv TSVECTOR, source_url TEXT, publication_doi TEXT,
  reliability_score NUMERIC, data_type TEXT, record_count_estimate INTEGER,
  ingested_at TIMESTAMPTZ, last_refreshed_at TIMESTAMPTZ)

FK CONSTRAINTS:
- person.birth_region_id → region.region_id
- person.source_dataset_id → dataset_source.dataset_id
- genome.person_id → person.person_id
- genome.dataset_id → dataset_source.dataset_id
- genome_snp.genome_id → genome.genome_id
- genome_snp.snp_id → snp_marker.snp_id
- genome_match.genome_a_id → genome.genome_id
- genome_match.genome_b_id → genome.genome_id
- haplogroup_reference.parent_haplogroup_id → haplogroup_reference.haplogroup_id
- genome_cluster_assignment.genome_id → genome.genome_id
- genome_cluster_assignment.cluster_id → population_cluster.cluster_id
- inferred_relationship.person_a_id → person.person_id
- inferred_relationship.person_b_id → person.person_id
- inferred_relationship.match_id → genome_match.match_id
- dynasty.origin_region_id → region.region_id
- dynasty_membership.person_id → person.person_id
- dynasty_membership.dynasty_id → dynasty.dynasty_id
- person_location_timeline.person_id → person.person_id
- person_location_timeline.region_id → region.region_id
- historical_conflict.region_id → region.region_id
- region.parent_region_id → region.region_id
- population_cluster.source_dataset_id → dataset_source.dataset_id

NOTES:
- birth_year and death_year use negative integers for BCE dates (e.g. -500 = 500 BCE)
- is_historical = true for ancient/historical figures, false for modern persons
- inferred_relationship stores person_a as the subject, person_b as the related person
- relationship_type = 'ancestor' means person_b is an ancestor of person_a
- haplogroup_type distinguishes Y-chromosome ('Y-DNA') from mitochondrial ('mtDNA') lineages
"""

FEW_SHOT_EXAMPLES = """
EXAMPLES:

Q: Which historical figures share haplogroup R1b?
A: SELECT p.full_name, g.y_haplogroup FROM genome g JOIN person p ON p.person_id = g.person_id WHERE g.y_haplogroup LIKE 'R1b%' AND p.is_historical = true

Q: Who are the ancestors of Genghis Khan?
A: SELECT p.full_name, ir.generational_distance, ir.confidence_level FROM inferred_relationship ir JOIN person p ON p.person_id = ir.person_b_id JOIN person target ON target.person_id = ir.person_a_id WHERE target.full_name ILIKE '%Genghis Khan%' AND ir.relationship_type = 'ancestor' ORDER BY ir.generational_distance ASC

Q: List all persons born before 500 BCE in a Roman region?
A: SELECT p.full_name, p.birth_year, r.region_name FROM person p JOIN region r ON r.region_id = p.birth_region_id WHERE p.birth_year < -500 AND r.region_name ILIKE '%roman%' ORDER BY p.birth_year ASC

Q: Which genome matches have a similarity score above 0.8 using the haplogroup method?
A: SELECT gm.match_id, pa.full_name AS person_a, pb.full_name AS person_b, gm.similarity_score FROM genome_match gm JOIN genome ga ON ga.genome_id = gm.genome_a_id JOIN genome gb ON gb.genome_id = gm.genome_b_id JOIN person pa ON pa.person_id = ga.person_id JOIN person pb ON pb.person_id = gb.person_id WHERE gm.similarity_score > 0.8 AND gm.matching_method = 'haplogroup' ORDER BY gm.similarity_score DESC

Q: Which dynasties originated in the Roman Empire region?
A: SELECT d.dynasty_name, d.founding_year, d.dissolution_year, r.region_name FROM dynasty d JOIN region r ON r.region_id = d.origin_region_id WHERE r.region_name ILIKE '%roman%' ORDER BY d.founding_year ASC
"""

SYSTEM_INSTRUCTION = """
You are a read-only SQL generator for a PostgreSQL 15 database.
Generate a single SELECT statement only.
Rules:
- No semicolons except optionally at the very end
- No CTEs unless the query genuinely requires recursive traversal
- No subquery deeper than 2 levels
- No comments in the SQL output
- Do not use DROP, DELETE, ALTER, TRUNCATE, UPDATE, INSERT, CREATE, GRANT, or REVOKE
- Output SQL only — no explanation, no markdown, no code fences
"""


def build_prompt(nl_query: str) -> str:
    return f"{SYSTEM_INSTRUCTION}\n\n{SCHEMA_CONTEXT}\n\n{FEW_SHOT_EXAMPLES}\n\nQ: {nl_query}\nA:"