WITH
  JoinedClaims AS (
    -- Join the raw claims with the provider details table to get providerName
    SELECT
      c.Id,
      c.serviceStartDate AS claimDos,
      c.renderingProviderNpi[SAFE_OFFSET(0)] AS claimProviderNPI,
      p.providerName,

      -- Calculate days difference from payload
      ABS(DATE_DIFF(c.serviceStartDate, "2024-02-01", DAY)) AS days_diff,

      -- Fuzzy similarity score (1.0 = exact match)
      (
        1.0 - (
          EDIT_DISTANCE(LOWER(p.providerName), LOWER("jOhn"))
          / GREATEST(LENGTH(p.providerName), 1))) AS name_fuzzy_score

    FROM `dataflow-pipeline-485105.allymar_patient_claims.allymar_patient_data` AS c
    LEFT JOIN
      `dataflow-pipeline-485105.allymar_patient_claims.claim_provider_details`
        AS p
      ON
        c.memberId = p.memberId
  ),
  ClaimsMatchLogic AS (
    SELECT
      *,
      CASE
        -- 1. PERFECT MATCH
        WHEN days_diff = 0 AND claimProviderNPI = 1000000001
          THEN 'PERFECT_MATCH'

        -- 2. NEAREST CLAIM BY DOS (Within threshold, e.g., 7 days)
        WHEN days_diff > 0 AND days_diff <= 0
          THEN 'NEAREST_CLAIM_FOUND'

        -- 3. FUZZY MATCH LOGIC
        WHEN name_fuzzy_score >= 0.9
          THEN 'NPPES_MATCH_HIGH_CONFIDENCE'
        WHEN name_fuzzy_score >= 0.8
          THEN 'FUZZY_PROVIDER_MATCH_HIGH'
        WHEN name_fuzzy_score >= 0.7
          THEN 'FUZZY_PROVIDER_MATCH_MED'

        -- 4. LOW THRESHOLD WITH STATE VERIFICATION
        WHEN name_fuzzy_score >= 0.6
          THEN 'FUZZY_MATCH_STATE_VERIFIED'
        ELSE 'NO_RELATED_CLAIM'
        END AS match_status
    FROM JoinedClaims
  )
SELECT
  Id,
  claimDos,
  providerName,
  match_status,
  name_fuzzy_score,
  IF(match_status = 'PERFECT_MATCH', FALSE, TRUE) AS review_required
FROM ClaimsMatchLogic
ORDER BY
  CASE
    WHEN match_status = 'PERFECT_MATCH' THEN 1
    WHEN match_status = 'NEAREST_CLAIM_FOUND' THEN 2
    ELSE 3
    END,
  days_diff ASC,
  name_fuzzy_score DESC
LIMIT 1;