

WITH
  -- 1. OPTIMIZATION: Filter Patients Early
  base_patients AS (
    SELECT
      memberId, firstName, lastName, dob,
      SPLIT(path, '/')[SAFE_OFFSET(1)] AS patient_key
    FROM `client_vim_access_0011_2025.SyncPatients`
    WHERE SPLIT(path, '/')[SAFE_OFFSET(1)] NOT IN
          ('LH9iuFnSlvIXjJpc8dUy', 'NjIRWCCdOfIgUZuYRcpz', 'ThTLp2wihdUDYDqqug3t', 'NjIRWCCdOfIgUguYRapz', 'dj18yIuUkhoDMECrz7bD')
  ),

  base_hcc AS (
    SELECT
      *,
      SPLIT(path, '/')[SAFE_OFFSET(1)] AS patient_key,
      SPLIT(path, '/')[SAFE_OFFSET(3)] AS hcc_key
    FROM `client_vim_access_0011_2025.SyncHCCGaps`
  ),

  -- 2. PRE-PROCESSING: Bucket Sources & Normalize Codes
  base_dx_bucketed AS (
    SELECT
      *,
      SPLIT(path, '/')[SAFE_OFFSET(3)] AS hcc_key,
      SPLIT(path, '/')[SAFE_OFFSET(5)] AS dx_key,
      UPPER(TRIM(icdCode)) AS normalized_code, -- Fix for Case/Space mismatches
      -- Helper: Pre-calculate the bucket
      CASE
        WHEN LOWER(source) IN ('p360', 'accraf', 'p3', 'clientgap', 'client suspects - p3', 'p3 feedback', 'adobe', 'athena', 'poc', 'swds') THEN 'P3'
        WHEN LOWER(source) IN ('mao004 - disallowed', 'medical claims - not allowable', 'medical claims - atrio', 'vim-allymar', 'allymar-asm', 'allymar', 'allymar ai') THEN 'Allymar'
        WHEN LOWER(source) IN ('mao004', 'final-mor', 'initial-mor', 'midyear-mor') THEN 'P3'
        ELSE 'Other'
      END AS source_bucket_helper
    FROM `client_vim_access_0011_2025.SyncDxGaps`
  ),

  base_hcc_feedbacks AS (
    SELECT
      *,
      SPLIT(path, '/')[SAFE_OFFSET(1)] AS patient_key,
      SPLIT(path, '/')[SAFE_OFFSET(3)] AS hcc_key,
      CASE
        WHEN actionReason IN ('ADD', 'AUTO_RESOLVED') THEN 'ACCEPTED'
        WHEN actionReason IN ('AGREE_WITHOUT_WRITEBACK', 'PENDING_ADDITIONAL_TESTING_OR_CONSULT', 'WILL_EVALUATE_NEXT_VISIT') THEN 'VALID'
        WHEN actionReason IN ('CONDITION_RESOLVED_OR_NOT_PRESENT', 'OTHER_/_MISSING_INFORMATION', 'PATIENT_UNQUALIFIED') THEN 'DISMISSED'
        ELSE 'UNCATEGORIZED'
      END AS decision_category
    FROM `allymargenairisknquality.client_vim_access_0011_2025.SyncHCCFeedback`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY SPLIT(path, '/')[SAFE_OFFSET(1)], SPLIT(path, '/')[SAFE_OFFSET(3)]
      ORDER BY updateTimestamp DESC
    ) = 1
  ),

  -------------------------------------------------------
  -- LOGIC ENGINE: Determine Source at HCC Level
  -------------------------------------------------------

  -- Step A: Unnest Feedback Codes
  feedback_codes_expanded AS (
    SELECT
      hf.hcc_key,
      UPPER(TRIM(code)) AS selected_code -- Fix for Case/Space mismatches
    FROM base_hcc_feedbacks hf,
    UNNEST(hf.selectedMedicalCodes) AS code
    WHERE hf.decision_category IN ('ACCEPTED', 'VALID', 'DISMISSED', 'UNCATEGORIZED')
  ),

  -- Step B: Aggregate & Apply Rules
  hcc_source_determination AS (
    SELECT
      bh.hcc_key,

      -- 1. Metrics for Feedback Logic (Closed)
      -- Count how many codes are in the feedback
      COUNT(DISTINCT fce.selected_code) AS total_feedback_codes_count,

      -- Count how many of those codes ACTUALLY exist in the Dx table
      COUNT(DISTINCT CASE WHEN bd.normalized_code = fce.selected_code THEN fce.selected_code END) AS matched_codes_count,

      -- Among the MATCHED codes, how many distinct sources?
      COUNT(DISTINCT CASE WHEN bd.normalized_code = fce.selected_code THEN bd.source END) AS distinct_sources_matched,

      -- Among the MATCHED codes, is Allymar present?
      MAX(CASE WHEN bd.normalized_code = fce.selected_code AND (bd.source_bucket_helper = 'Allymar' OR bd.source = 'Allymar AI') THEN 1 ELSE 0 END) AS has_allymar_in_match,
      MAX(CASE WHEN bd.normalized_code = fce.selected_code AND (bd.source_bucket_helper = 'P3') THEN 1 ELSE 0 END) AS has_p3_in_match,
      -- 2. Metrics for Open/No Feedback Logic (All Dx)
      MAX(CASE WHEN bd.source_bucket_helper = 'Allymar' OR bd.source = 'Allymar AI' THEN 1 ELSE 0 END) AS has_allymar_any,
      MAX(CASE WHEN bd.source_bucket_helper = 'P3' THEN 1 ELSE 0 END) AS has_p3_any,

      -- Verification String
      STRING_AGG(
        DISTINCT CONCAT(IFNULL(bd.icdCode, 'No-Code'), ' (', IFNULL(bd.source, 'No-Source'), ')'),
        ' | '
        ORDER BY CONCAT(IFNULL(bd.icdCode, 'No-Code'), ' (', IFNULL(bd.source, 'No-Source'), ')')
      ) AS dx_source_verification,

      COUNT(DISTINCT bd.source) AS dx_source_count

    FROM base_hcc bh
    LEFT JOIN base_hcc_feedbacks hf ON bh.hcc_key = hf.hcc_key
    LEFT JOIN base_dx_bucketed bd ON bh.hcc_key = bd.hcc_key
    LEFT JOIN feedback_codes_expanded fce ON bh.hcc_key = fce.hcc_key
    GROUP BY 1
  ),

  final_logic_application AS (
    SELECT
      bh.hcc_key,
      CASE
        --------------------------------------------------------
        -- SCENARIO 1 & 3: Closed Feedback (Known OR Suspect)
        --------------------------------------------------------
        WHEN hf.hcc_key IS NOT NULL AND hf.decision_category IN ('ACCEPTED', 'VALID', 'DISMISSED', 'UNCATEGORIZED') THEN
           CASE
             -- Rule: If ANY selected code is not present in Dx -> Default Allymar
             -- Logic: If Matched Count < Total Expected Count -> Allymar
             WHEN src.matched_codes_count < src.total_feedback_codes_count THEN 'Allymar'

             -- Rule: If Multiple Distinct Sources for the MATCHED codes AND Allymar is one of them -> Allymar
             WHEN src.distinct_sources_matched > 1 AND src.has_allymar_in_match = 1 THEN 'Allymar'

             WHEN src.distinct_sources_matched >= 1 AND src.has_allymar_in_match = 0 AND src.has_p3_any = 1 AND src.has_p3_in_match = 1 THEN 'P3'

             -- Rule: Otherwise (Single source, or multiple non-Allymar sources) -> P3
             ELSE 'P3'
           END

        --------------------------------------------------------
        -- SCENARIO 4: Open/No Feedback AND Suspect
        --------------------------------------------------------
        WHEN (bh.gapType LIKE '%Suspect%' OR bh.gapType LIKE '%SUSPECT%') THEN
           CASE
             -- Hierarchy: Allymar -> P3
             WHEN src.has_allymar_any = 1 THEN 'Allymar'
             WHEN src.has_p3_any = 1 THEN 'P3'
             ELSE 'P3'
           END

        --------------------------------------------------------
        -- SCENARIO 2: Open/No Feedback AND Known (Not Suspect)
        --------------------------------------------------------
        ELSE -- Implied known condition
           CASE
             -- Logic: If Allymar present -> Allymar, Else -> P3
             WHEN src.has_allymar_any = 1 THEN 'Allymar'
             ELSE 'P3'
           END
      END AS hcc_calculated_source,

      src.dx_source_verification,
      src.dx_source_count,
      src.has_p3_any,
      src.has_allymar_any,
      src.has_allymar_in_match

    FROM base_hcc bh
    LEFT JOIN base_hcc_feedbacks hf ON bh.hcc_key = hf.hcc_key
    LEFT JOIN hcc_source_determination src ON bh.hcc_key = src.hcc_key
  ),

  ################### FOR TIN & MARKET ##################
  providerAttribution AS (
    SELECT
      memberID,
      providerNPI,
      CAST(RIGHT(effectiveDate,4) AS INT64) attributedYear,
      ROW_NUMBER() OVER(PARTITION BY memberId, RIGHT(effectiveDate,4) ORDER BY PARSE_DATE('%m-%d-%Y',endDate) DESC)  AS activeRank
    FROM `allymargenairisknquality.client_velocity_access_0011_2025_ana.FactMemberProvider`
  ),
  providerAttributionDim AS (
    SELECT DISTINCT
      a.memberID,
      a.attributedYear,
      a.providerNPI,
      b.TIN,
      b.ProviderGroupName,
      b.providerName,
      b.Tier
    FROM providerAttribution as a
    LEFT JOIN `allymargenairisknquality.client_velocity_access_0011_2025_ana.DimProvider` as b
      ON a.providerNPI = b.providerNPI
    WHERE activeRank = 1
  ),
  #########################################################

  FINAL_RECORDS AS (
    SELECT
      ####### PATIENT FIELD #########
      bp.memberId,
      bp.firstName,
      bp.lastName,
      bp.dob,

      ####### HCC FIELD #########
      bh.* EXCEPT (gapType, path, gapStatus, patient_key, hcc_key, isDeleted),
      bh.gapType AS hccGapType,
      bh.gapStatus AS hccGapStatus,
      bh.path AS hccPath,

      -- New Calculated Fields
      fla.hcc_calculated_source AS hcc_level_source,
      fla.dx_source_verification,
      -- fla.dx_source_count,
      -- fla.has_p3_any,
      -- fla.has_allymar_any,
      -- fla.has_allymar_in_match,


      ####### FEEDBACK FIELD #########
      hf.* EXCEPT (gapType, path, source, patient_key, hcc_key, decision_category),
      hf.gapType AS feedbackGapType,
      hf.source AS feedbackSource,
      hf.decision_category,

      ############# TIN & MARKET #########
      dim.TIN,
      dy.market,
      ####################################

      -- Bucket Logic
      CASE
        WHEN fla.hcc_calculated_source = 'P3' THEN 'P3'
        WHEN fla.hcc_calculated_source = 'Allymar' THEN 'Allymar'
        ELSE fla.hcc_calculated_source
      END AS source_bucket

    FROM base_hcc bh
    LEFT JOIN base_patients bp ON bp.patient_key = bh.patient_key
    LEFT JOIN base_hcc_feedbacks hf ON hf.hcc_key = bh.hcc_key
    LEFT JOIN final_logic_application fla ON fla.hcc_key = bh.hcc_key
    LEFT JOIN providerAttributionDim dim ON dim.memberID = bp.memberId AND CAST(dim.attributedYear AS STRING) = bh.yos
    LEFT JOIN `allymargenairisknquality.client_velocity_access_0011_2025_ana.DimMemberYear` dy ON dy.memberId = bp.memberId AND CAST(dy.paymentYear AS STRING) = bh.yos
  )

-- SELECT * FROM FINAL_RECORDS

######## QUERY FOR REQUIREMENT 1 ########
-- SELECT
--   market as market,
--   tin as tin,
--   hccGapType,
--     -- Raw Counts
--   COUNTIF(decision_category = 'ACCEPTED') AS count_accepted,
--   COUNTIF(decision_category = 'VALID') AS count_valid,
--   COUNTIF(decision_category = 'DISMISSED') AS count_dismissed,

--   -- Percentages
--   ROUND(SAFE_DIVIDE(COUNTIF(decision_category = 'ACCEPTED'), COUNT(*)) * 100, 2) AS pct_accepted,
--   ROUND(SAFE_DIVIDE(COUNTIF(decision_category = 'VALID'), COUNT(*)) * 100, 2) AS pct_valid,
--   ROUND(SAFE_DIVIDE(COUNTIF(decision_category = 'DISMISSED'), COUNT(*)) * 100, 2) AS pct_dismissed
-- FROM FINAL_RECORDS
-- WHERE hccGapType IN ('Known', 'Suspected') -- Updated Filter
-- GROUP BY 1, 2, 3
-- ORDER BY 1, 2, 3;
######## QUERY FOR REQUIREMENT 1 ########

######## QUERY FOR REQUIREMENT 2 & 3 ########
-- SELECT
--   source_bucket,
--   hccGapType,

--   -- DISMISSED
--   COUNTIF(decision_category = 'DISMISSED') AS total_dismissed,
--   COUNTIF(actionReason = 'CONDITION_RESOLVED_OR_NOT_PRESENT') AS dismissed_condition_resolved,
--   COUNTIF(actionReason = 'OTHER_/_MISSING_INFORMATION') AS dismissed_missing_info,
--   COUNTIF(actionReason = 'PATIENT_UNQUALIFIED') AS dismissed_patient_unqualified,

--   -- VALID
--   COUNTIF(decision_category = 'VALID') AS total_valid,
--   COUNTIF(actionReason = 'AGREE_WITHOUT_WRITEBACK') AS valid_agree_no_wb,
--   COUNTIF(actionReason = 'PENDING_ADDITIONAL_TESTING_OR_CONSULT') AS valid_pending_additional_testing_or_consult,
--   COUNTIF(actionReason = 'WILL_EVALUATE_NEXT_VISIT') AS valid_will_evaluate_next_visit,

--   -- ACCEPTED
--   COUNTIF(decision_category = 'ACCEPTED') AS total_accepted,
--   COUNTIF(actionReason = 'ADD') AS accept_add,
--   COUNTIF(actionReason = 'AUTO_RESOLVED') AS accept_auto_resolved

-- FROM FINAL_RECORDS
-- -- Updated Filter
-- GROUP BY 1, 2
-- ORDER BY 1, 2;
######## QUERY FOR REQUIREMENT 2 & 3 ########

######## QUERY FOR REQUIREMENT 4 ########
-- SELECT
--   hccCode,
--   hccDescription,
--   COUNT(*) AS total_codes,

--   -- Action Reason Distribution (Array Aggregation for clean single-row view)
--   ARRAY_TO_STRING(ARRAY_AGG(DISTINCT actionReason IGNORE NULLS), ', ') AS action_reasons_present,

--   COUNTIF(decision_category = 'ACCEPTED') AS accept_count,
--   COUNTIF(decision_category = 'VALID') AS valid_count,
--   COUNTIF(decision_category = 'DISMISSED') AS dismissed_count,

-- FROM FINAL_RECORDS
-- GROUP BY 1, 2
-- ORDER BY total_codes DESC;
######## QUERY FOR REQUIREMENT 4 ########

######## QUERY FOR REQUIREMENT 5 ########
-- SELECT
--   market, -- Included for filtering capability
--   tin,    -- Included for filtering capability

--   COUNTIF(hccGapType = 'Suspected') AS total_suspects,
--   ROUND(SAFE_DIVIDE(COUNTIF(hccGapType = 'Suspected'), COUNT(*)) * 100, 2) AS suspect_pct,

--   COUNTIF(hccGapType IN ('Known')) AS total_known,
--   ROUND(SAFE_DIVIDE(COUNTIF(hccGapType IN ('Known')), COUNT(*)) * 100, 2) AS known_pct,

--   -- Assuming 'Closed' is determined by gapStatus, not gapType
--   COUNTIF(UPPER(hccGapStatus) = 'CLOSED') AS total_closed

-- FROM FINAL_RECORDS
-- GROUP BY 1, 2;
######## QUERY FOR REQUIREMENT 5 ########

######## QUERY FOR REQUIREMENT 6 ########
-- SELECT
--   source_bucket,
--   COUNT(*) AS suspect_count,
-- FROM FINAL_RECORDS
-- WHERE hccGapType = 'Suspected'
-- GROUP BY 1
-- ORDER BY 2 DESC;
######## QUERY FOR REQUIREMENT 6 ########

######## QUERY FOR REQUIREMENT 7 ########
-- SELECT
--   market,
--   tin,
--   -- Metric Tier (Total, Suspects, Known)
--   category_scope,

--   -- Raw Counts
--   COUNTIF(decision_category = 'ACCEPTED') AS count_accepted,
--   COUNTIF(decision_category = 'VALID') AS count_valid,
--   COUNTIF(decision_category = 'DISMISSED') AS count_dismissed,

-- FROM FINAL_RECORDS t,
-- UNNEST([
--   STRUCT('TOTAL' AS category_scope, TRUE AS filter),
--   STRUCT('SUSPECTS', hccGapType = 'Suspected'),
--   STRUCT('KNOWN', hccGapType IN ('Known'))
-- ]) map
-- WHERE map.filter AND decision_category != 'UNCATEGORIZED'
-- GROUP BY 1,2,3;
######## QUERY FOR REQUIREMENT 7 ########


######## QUERY FOR REQUIREMENT 8 ########
SELECT
  concat(userFirstName, ' ', userLastName) as providerFullName,
  userNpi,
  COUNT(*) AS total_codes,

  -- Action Reason Distribution (Array Aggregation for clean single-row view)
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT actionReason IGNORE NULLS), ', ') AS action_reasons_present,

  COUNTIF(decision_category = 'ACCEPTED') AS accept_count,
  COUNTIF(decision_category = 'VALID') AS valid_count,
  COUNTIF(decision_category = 'DISMISSED') AS dismissed_count,

FROM FINAL_RECORDS WHERE decision_category != 'UNCATEGORIZED'
GROUP BY 1, 2
ORDER BY total_codes DESC;
######## QUERY FOR REQUIREMENT 8 ########
