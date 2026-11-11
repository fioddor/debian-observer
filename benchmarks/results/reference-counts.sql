-- Reference counts for R2 (spec s4-project2.v0.2, Volume §2.1).
-- Re-measured against the local UDD restore (udd-postgresql
-- container, udd.dump of 2026-10-02) on 2026-11-09 with the
-- implementation of T3. Run with:
--   docker cp reference-counts.sql udd-postgresql:/tmp/
--   docker exec udd-postgresql psql -U udd -d udd -f /tmp/reference-counts.sql

-- binaries in the trixie amd64 graph (amd64 + all)
SELECT 'trixie_amd64_all_packages' AS measure, count(*)
FROM packages
WHERE distribution='debian' AND release='trixie'
  AND architecture IN ('amd64','all');

-- Depends clauses, split on ', '
SELECT 'trixie_amd64_all_depends_clauses' AS measure, count(*)
FROM (
  SELECT unnest(string_to_array(depends, ', ')) c FROM packages
  WHERE distribution='debian' AND release='trixie'
    AND architecture IN ('amd64','all')
) t;

-- ... of which complex (contain '|')
SELECT 'trixie_amd64_all_complex_clauses' AS measure, count(*)
FROM (
  SELECT unnest(string_to_array(depends, ', ')) c FROM packages
  WHERE distribution='debian' AND release='trixie'
    AND architecture IN ('amd64','all')
) t
WHERE c LIKE '%|%';

-- Pre-Depends clauses
SELECT 'trixie_amd64_all_pre_depends_clauses' AS measure, count(*)
FROM (
  SELECT unnest(string_to_array(pre_depends, ', ')) c FROM packages
  WHERE distribution='debian' AND release='trixie'
    AND architecture IN ('amd64','all')
) t;

-- event volume, all distributions (depends + pre-depends)
SELECT 'all_depends_clauses' AS measure, count(*)
FROM (
  SELECT unnest(string_to_array(depends, ', ')) c FROM packages
) t;

SELECT 'all_pre_depends_clauses' AS measure, count(*)
FROM (
  SELECT unnest(string_to_array(pre_depends, ', ')) c FROM packages
) t;

-- graph coordinates (base releases, excluding 'all')
SELECT 'graph_coordinates' AS measure, count(DISTINCT (release, architecture))
FROM packages
WHERE distribution='debian' AND architecture<>'all'
  AND release IN ('bullseye','bookworm','trixie','forky','sid');

-- popcon rows
SELECT 'popcon_rows' AS measure, count(*) FROM popcon;