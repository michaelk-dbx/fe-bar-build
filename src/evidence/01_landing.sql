-- name: Read-back of the raw FNOL files in the landing volume
-- note: Includes resubmissions and malformed records, which the pipeline handles in step 2.
SELECT substr(submitted_at, 1, 4) AS submitted_year, count(*) AS fnol_records,
       count_if(fnol_version = 2) AS resubmissions, count_if(claim_id IS NULL) AS missing_claim_id,
       round(sum(claimed_amount)) AS claimed_usd, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/{catalog}/{schema}/landing/claims_fnol/', format => 'json')
GROUP BY 1 ORDER BY 1;

-- name: Files per source system in the landing volume
SELECT split(_metadata.file_path, '/')[5] AS source_folder, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/{catalog}/{schema}/landing/', format => 'text', recursiveFileLookup => true)
GROUP BY 1 ORDER BY 1;
