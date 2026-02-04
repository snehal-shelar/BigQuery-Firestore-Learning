SELECT
  Id,
  serviceStartDate AS claimDos,
  renderingProviderNpi AS claimProviderNPI,
  @encounter_dos AS encounterDos,
  @encounter_npi AS encounterNPI,
  -- Matching Logic Condition
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM UNNEST(renderingProviderNpi) AS npi
      WHERE npi = @encounter_npi
    ) THEN 'perfect_match'
    ELSE 'review_required'
  END AS match_status,
  -- Review Flag
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM UNNEST(renderingProviderNpi) AS npi
      WHERE npi = @encounter_npi
    ) THEN FALSE
    ELSE TRUE
  END AS review_required
FROM `dataflow-pipeline-485105.allymar_patient_claims.allymar_patient_data`
-- Filter claims where the DOS matches the encounter DOS
WHERE serviceStartDate = @encounter_dos



WITH BaseProcessing AS (
  SELECT
    *,
    -- Mapping: claimDos = serviceStartDate
    serviceStartDate as claimDos,
    -- Extracting the NPI from the first index of the array
    renderingProviderNpi[SAFE_OFFSET(0)] as claimProviderNPI,
    -- Calculate days difference from encounter payload
    ABS(DATE_DIFF(serviceStartDate, @encounter_dos, DAY)) as days_diff,
    -- Calculate Fuzzy Name Match (0.0 to 1.0)
    (1.0 - (EDIT_DISTANCE(renderingProviderName, @encounter_provider_name) /
     GREATEST(LENGTH(renderingProviderName), 1))) as name_fuzzy_score
  FROM `dataflow-pipeline-485105.allymar_patient_claims.allymar_patient_data`
),
ScoredClaims AS (
  SELECT
    *,
    -- Logic Waterfall
    CASE
      -- 1. PERFECT MATCH CASE
      WHEN days_diff = 0 AND claimProviderNPI = @encounter_npi
        THEN 'PERFECT_MATCH'

      -- 2. NEAREST CLAIM CASE (Within Threshold, e.g., 7 days)
      WHEN days_diff > 0 AND days_diff <= @date_threshold
        THEN 'NEAREST_CLAIM_FOUND'

      -- 3. FUZZY & NPPES LOGIC (When DOS Mismatch or Date out of threshold)
      WHEN name_fuzzy_score >= 0.9
        THEN 'NPPES_MATCH_HIGH_CONFIDENCE'

      WHEN name_fuzzy_score >= 0.8
        THEN 'FUZZY_PROVIDER_MATCH'

      WHEN name_fuzzy_score >= 0.6 AND memberState = @encounter_state
        THEN 'FUZZY_MATCH_STATE_VERIFIED'

      ELSE 'NO_RELATED_CLAIM'
    END AS match_status
  FROM BaseProcessing
)
SELECT
  Id,
  claimDos,
  claimProviderNPI,
  match_status,
  days_diff,
  name_fuzzy_score,
  -- Review Flag: False only for Perfect Match
  IF(match_status = 'PERFECT_MATCH', FALSE, TRUE) as review_required
FROM ScoredClaims
ORDER BY
  CASE WHEN match_status = 'PERFECT_MATCH' THEN 1
       WHEN match_status = 'NEAREST_CLAIM_FOUND' THEN 2
       ELSE 3 END,
  days_diff ASC,
  name_fuzzy_score DESC
LIMIT 1;