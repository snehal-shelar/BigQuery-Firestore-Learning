WITH
  -- 1. OPTIMIZATION: Filter Patients Early
  base_patients AS (
    SELECT
      memberId,
      firstName,
      lastName,
      dob,
      SPLIT(path, '/')[SAFE_OFFSET(1)] AS patient_key
    FROM `client_vim_access_0011_2025.SyncPatients`
    WHERE
      SPLIT(path, '/')[SAFE_OFFSET(1)]
      NOT IN (
        'LH9iuFnSlvIXjJpc8dUy', 'NjIRWCCdOfIgUZuYRcpz', 'ThTLp2wihdUDYDqqug3t',
        'NjIRWCCdOfIgUguYRapz', 'dj18yIuUkhoDMECrz7bD')
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
      UPPER(TRIM(icdCode)) AS normalized_code,  -- Fix for Case/Space mismatches
      -- Helper: Pre-calculate the bucket
      CASE
        WHEN
          LOWER(source)
          IN (
            'p360', 'accraf', 'p3', 'clientgap', 'client suspects - p3',
            'p3 feedback', 'adobe', 'athena', 'poc', 'swds')
          THEN 'P3'
        WHEN
          LOWER(source)
          IN (
            'mao004 - disallowed', 'medical claims - not allowable',
            'medical claims - atrio', 'vim-allymar', 'allymar-asm', 'allymar',
            'allymar ai')
          THEN 'Allymar'
        WHEN
          LOWER(source)
          IN ('mao004', 'final-mor', 'initial-mor', 'midyear-mor', 'cms')
          THEN 'CMS'
        WHEN LOWER(source) IN ('medical claims') THEN 'Medical Claims'
        ELSE 'OTHER'
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
        WHEN
          actionReason IN (
            'AGREE_WITHOUT_WRITEBACK', 'PENDING_ADDITIONAL_TESTING_OR_CONSULT',
            'WILL_EVALUATE_NEXT_VISIT', 'OTHER_/_MISSING_INFORMATION')
          THEN 'VALID'
        WHEN
          actionReason IN (
            'CONDITION_RESOLVED_OR_NOT_PRESENT', 'PATIENT_UNQUALIFIED')
          THEN 'DISMISSED'
        ELSE 'UNCATEGORIZED'
        END AS decision_category
    FROM `allymargenairisknquality.client_vim_access_0011_2025.SyncHCCFeedback`
    QUALIFY
    -- this is the window function.
      ROW_NUMBER()
        OVER (
          PARTITION BY
            SPLIT(path, '/')[SAFE_OFFSET(1)], SPLIT(path, '/')[SAFE_OFFSET(3)]
          ORDER BY updateTimestamp DESC
        )
      = 1
  ),

  -------------------------------------------------------
  -- LOGIC ENGINE: Determine Source at HCC Level
  -------------------------------------------------------
  -- Step A: Unnest Feedback Codes
  feedback_codes_expanded AS (
    SELECT
      hf.hcc_key,
      UPPER(TRIM(code)) AS selected_code  -- Fix for Case/Space mismatches
    FROM
      base_hcc_feedbacks hf,
      UNNEST(hf.selectedMedicalCodes) AS code
    WHERE
      hf.decision_category IN (
        'ACCEPTED', 'VALID', 'DISMISSED', 'UNCATEGORIZED')
  ),

  -- Step B: Aggregate & Apply Rules
  hcc_source_determination AS (
    SELECT
      bh.hcc_key,

      -- 1. Metrics for Feedback Logic (Closed)
      -- Count how many codes are in the feedback
      COUNT(DISTINCT fce.selected_code) AS total_feedback_codes_count,

      -- Count how many of those codes ACTUALLY exist in the Dx table
      COUNT(
        DISTINCT
          CASE
            WHEN bd.normalized_code = fce.selected_code THEN fce.selected_code
            END) AS matched_codes_count,

      -- Among the MATCHED codes, how many distinct sources?
      COUNT(
        DISTINCT
          CASE WHEN bd.normalized_code = fce.selected_code THEN bd.source END)
        AS distinct_sources_matched,

      -- Among the MATCHED codes, is Allymar present?
      MAX(
        CASE
          WHEN
            bd.normalized_code = fce.selected_code
            AND (bd.source_bucket_helper = 'CMS')
            THEN 1
          ELSE 0
          END) AS has_cms_in_match,
      MAX(
        CASE
          WHEN
            bd.normalized_code = fce.selected_code
            AND (bd.source_bucket_helper = 'Medical Claims')
            THEN 1
          ELSE 0
          END) AS has_medical_claims_in_match,
      MAX(
        CASE
          WHEN
            bd.normalized_code = fce.selected_code
            AND (
              bd.source_bucket_helper = 'Allymar' OR bd.source = 'Allymar AI')
            THEN 1
          ELSE 0
          END) AS has_allymar_in_match,
      MAX(
        CASE
          WHEN
            bd.normalized_code = fce.selected_code
            AND (bd.source_bucket_helper = 'P3')
            THEN 1
          ELSE 0
          END) AS has_p3_in_match,
      -- 2. Metrics for Open/No Feedback Logic (All Dx)
      MAX(
        CASE
          WHEN bd.source_bucket_helper = 'Allymar' OR bd.source = 'Allymar AI'
            THEN 1
          ELSE 0
          END) AS has_allymar_any,
      MAX(CASE WHEN bd.source_bucket_helper = 'P3' THEN 1 ELSE 0 END)
        AS has_p3_any,
      MAX(
        CASE WHEN bd.source_bucket_helper = 'Medical Claims' THEN 1 ELSE 0 END)
        AS has_medical_claims_any,
      MAX(CASE WHEN bd.source_bucket_helper = 'CMS' THEN 1 ELSE 0 END)
        AS has_cms_any,

      -- Verification String
      STRING_AGG(
        DISTINCT
          CONCAT(
            IFNULL(bd.icdCode, 'No-Code'),
            ' (',
            IFNULL(bd.source, 'No-Source'),
            ')'),
        ' | '
        ORDER BY
          CONCAT(
            IFNULL(bd.icdCode, 'No-Code'),
            ' (',
            IFNULL(bd.source, 'No-Source'),
            ')')) AS dx_source_verification,
      COUNT(DISTINCT bd.source) AS dx_source_count
    FROM base_hcc bh
    LEFT JOIN base_hcc_feedbacks hf
      ON bh.hcc_key = hf.hcc_key
    LEFT JOIN base_dx_bucketed bd
      ON bh.hcc_key = bd.hcc_key
    LEFT JOIN feedback_codes_expanded fce
      ON bh.hcc_key = fce.hcc_key
    GROUP BY 1
  ),
  final_logic_application AS (
    SELECT
      bh.hcc_key,
      CASE
        SQL LEARNING: BIG QUERY, gcp exam, bigquery job profiles.
        --------------------------------------------------------
        -- SCENARIO 1: Closed Feedback (Known) hierarchy : CMS > Medical Claims > Allymar > P3
        --------------------------------------------------------
        WHEN
          hf.hcc_key IS NOT NULL
          AND hf.decision_category IN (
            'ACCEPTED', 'VALID', 'DISMISSED', 'UNCATEGORIZED')
--           AND (bh.gapType LIKE '%Known%' OR bh.gapType LIKE '%KNOWN%')
          AND (LOWER(bh.gapType) LIKE '%known%')
          THEN
            CASE
              -- Rule: If ANY selected code is not present in Dx -> Default Allymar
              -- Logic: If Matched Count < Total Expected Count -> Allymar
              --  WHEN src.matched_codes_count < src.total_feedback_codes_count THEN 'Allymar'
              -- Rule: If Multiple Distinct Sources for the MATCHED codes AND Allymar is one of them -> Allymar
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_cms_in_match = 1
                THEN 'CMS'
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_medical_claims_in_match = 1
                THEN 'Medical Claims'
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_allymar_in_match = 1
                THEN 'Allymar'
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_p3_any = 1
                AND src.has_p3_in_match = 1
                THEN 'P3'
              WHEN src.has_cms_any = 1 AND lower(hf.gapType) LIKE '%known%'
                THEN 'CMS'
              -- Rule: Otherwise (Single source, or multiple non-Allymar sources) -> P3
              ELSE 'Allymar'
              END

        --------------------------------------------------------
        -- SCENARIO 3: Closed Feedback (Suspect) hierarchy : Allymar > P3
        --------------------------------------------------------
        WHEN
          hf.hcc_key IS NOT NULL
          AND hf.decision_category IN (
            'ACCEPTED', 'VALID', 'DISMISSED', 'UNCATEGORIZED')
--           AND (bh.gapType LIKE '%Suspect%' OR bh.gapType LIKE '%SUSPECT%')
          AND (LOWER(bh.gapType) LIKE '%suspect%')
          THEN
            CASE
              -- Rule: If ANY selected code is not present in Dx -> Default Allymar
              -- Logic: If Matched Count < Total Expected Count -> Allymar
              WHEN src.matched_codes_count < src.total_feedback_codes_count
                THEN 'Allymar'

              -- Rule: If Multiple Distinct Sources for the MATCHED codes AND Allymar is one of them -> Allymar
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_allymar_in_match = 1
                THEN 'Allymar'
              WHEN
                src.distinct_sources_matched >= 1
                AND src.has_allymar_in_match = 0
                AND src.has_p3_any = 1
                AND src.has_p3_in_match = 1
                THEN 'P3'

              -- Rule: Otherwise (Single source, or multiple non-Allymar sources) -> P3
              ELSE 'Allymar'
              END

        --------------------------------------------------------
        -- SCENARIO 4: Open/No Feedback AND Suspect
        --------------------------------------------------------
--         WHEN (bh.gapType LIKE '%Suspect%' OR bh.gapType LIKE '%SUSPECT%')
        WHEN (LOWER(bh.gapType) LIKE '%suspect%')  -- we can optimize this using LOWER()
          THEN
            CASE
              -- Hierarchy: Allymar > P3
              WHEN src.has_allymar_any = 1 THEN 'Allymar'
              WHEN src.has_p3_any = 1 THEN 'P3'
              ELSE 'Allymar'
              END

        --------------------------------------------------------
        -- SCENARIO 2: Open/No Feedback AND Known (Not Suspect)
        --------------------------------------------------------
        ELSE  -- Implied known condition
          CASE
            -- Logic: CMS > Medical Claims > Allymar > P3
            WHEN src.has_cms_any = 1 THEN 'CMS'
            WHEN src.has_medical_claims_any = 1 THEN 'Medical Claims'
            WHEN src.has_allymar_any = 1 THEN 'Allymar'
            WHEN src.has_p3_any = 1 THEN 'P3'
            ELSE 'Allymar'
            END
        END AS hcc_calculated_source,
      src.dx_source_verification,
      src.dx_source_count,
      src.has_p3_any,
      src.has_allymar_any,
      src.has_allymar_in_match,
      src.has_cms_in_match,
      src.has_medical_claims_in_match,
      src.has_p3_in_match
    FROM base_hcc bh
    LEFT JOIN base_hcc_feedbacks hf
      ON bh.hcc_key = hf.hcc_key
    LEFT JOIN hcc_source_determination src
      ON bh.hcc_key = src.hcc_key
  ),

  ################### FOR TIN & MARKET ##################
  providerAttribution AS (
    SELECT
      memberID,
      providerNPI,
      CAST(RIGHT(effectiveDate, 4) AS INT64) attributedYear,
      ROW_NUMBER()
        OVER (
          PARTITION BY memberId, RIGHT(effectiveDate, 4)
          ORDER BY PARSE_DATE('%m-%d-%Y', endDate) DESC
        ) AS activeRank
    FROM
      `allymargenairisknquality.client_velocity_access_0011_2025_ana.FactMemberProvider`
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
    FROM providerAttribution AS a
    LEFT JOIN
      `allymargenairisknquality.client_velocity_access_0011_2025_ana.DimProvider`
        AS b
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
      fla.dx_source_count,
      fla.has_p3_any,
      fla.has_allymar_any,
      fla.has_allymar_in_match,
      fla.has_cms_in_match,
      fla.has_medical_claims_in_match,
      fla.has_p3_in_match,

      ####### FEEDBACK FIELD #########
      hf.*
        EXCEPT (gapType, path, source, patient_key, hcc_key, decision_category),
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
        WHEN fla.hcc_calculated_source = 'CMS' THEN 'CMS'
        WHEN fla.hcc_calculated_source = 'Medical Claims' THEN 'Medical Claims'
        ELSE fla.hcc_calculated_source
        END AS source_bucket
    FROM base_hcc bh
    LEFT JOIN base_patients bp
      ON bp.patient_key = bh.patient_key
    LEFT JOIN base_hcc_feedbacks hf
      ON hf.hcc_key = bh.hcc_key
    LEFT JOIN final_logic_application fla
      ON fla.hcc_key = bh.hcc_key
    LEFT JOIN providerAttributionDim dim
      ON
        dim.memberID = bp.memberId
        AND CAST(dim.attributedYear AS STRING) = bh.yos
    LEFT JOIN
      `allymargenairisknquality.client_velocity_access_0011_2025_ana.DimMemberYear`
        dy
      ON dy.memberId = bp.memberId AND CAST(dy.paymentYear AS STRING) = bh.yos
  ),
  REJECTED_RECORDS AS (
    SELECT *
    FROM FINAL_RECORDS
    WHERE decision_category = 'DISMISSED' AND hccGapType = 'Known'
  ),
  MEMBER_GAPS_2023 AS (
    SELECT
      cmg.* EXCEPT (encounter, hccDetails),
      e.*,
      (
        SELECT AS STRUCT
          h.hccCode,
          h.hccDescription,
          h.hccModelVersion
        FROM UNNEST(hccDetails) AS h
        ORDER BY
          CASE
            WHEN h.hccModelVersion = 'v28' THEN 1
            WHEN h.hccModelVersion = 'v24' THEN 2
            WHEN h.hccModelVersion = 'v08' THEN 3
            ELSE 4
            END
        LIMIT 1
      ).*,
      (
        SELECT AS STRUCT
          cd.claimNumber, pd.renderingNPI, pd.renderingProviderName
        FROM UNNEST(e.claimDetails) AS cd, UNNEST(cd.providerDetails) AS pd
        ORDER BY e.dosDetails.serviceEndDate DESC NULLS LAST
        LIMIT 1
      ).*
    FROM
      `allymargenairisknquality.client_access_0011_2025_edw.CanonicalMemberGaps`
        cmg,
      UNNEST(encounter) AS e
    WHERE serviceYear = 2023
  ),
  MEMBER_GAPS_2024 AS (
    SELECT
      cmg.* EXCEPT (encounter, hccDetails),
      e.*,
      (
        SELECT AS STRUCT
          h.hccCode,
          h.hccDescription,
          h.hccModelVersion
        FROM UNNEST(hccDetails) AS h
        ORDER BY
          CASE
            WHEN h.hccModelVersion = 'v28' THEN 1
            WHEN h.hccModelVersion = 'v24' THEN 2
            WHEN h.hccModelVersion = 'v08' THEN 3
            ELSE 4
            END
        LIMIT 1
      ).*,
      (
        SELECT AS STRUCT
          cd.claimNumber, pd.renderingNPI, pd.renderingProviderName
        FROM UNNEST(e.claimDetails) AS cd, UNNEST(cd.providerDetails) AS pd
        ORDER BY e.dosDetails.serviceEndDate DESC NULLS LAST
        LIMIT 1
      ).*
    FROM
      `allymargenairisknquality.client_access_0011_2025_edw.CanonicalMemberGaps`
        cmg,
      UNNEST(encounter) AS e
    WHERE serviceYear = 2024
  ),
  MEMBER_GAPS_2025 AS (
    SELECT
      cmg.* EXCEPT (encounter, hccDetails),
      e.*,
      (
        SELECT AS STRUCT
          h.hccCode,
          h.hccDescription,
          h.hccModelVersion
        FROM UNNEST(hccDetails) AS h
        ORDER BY
          CASE
            WHEN h.hccModelVersion = 'v28' THEN 1
            WHEN h.hccModelVersion = 'v24' THEN 2
            WHEN h.hccModelVersion = 'v08' THEN 3
            ELSE 4
            END
        LIMIT 1
      ).*,
      (
        SELECT AS STRUCT
          cd.claimNumber, pd.renderingNPI, pd.renderingProviderName
        FROM UNNEST(e.claimDetails) AS cd, UNNEST(cd.providerDetails) AS pd
        ORDER BY e.dosDetails.serviceEndDate DESC NULLS LAST
        LIMIT 1
      ).*
    FROM
      `allymargenairisknquality.client_access_0011_2025_edw.CanonicalMemberGaps`
        cmg,
      UNNEST(encounter) AS e
    WHERE serviceYear = 2025
  ),
  ALL_HISTORY_UNNESTED AS (
    -- Combine the already unnested tables.
    -- Since 'e.*' represents an encounter, 1 row = 1 encounter.
    SELECT *, 2023 AS year_tag FROM MEMBER_GAPS_2023
    UNION ALL
    SELECT *, 2024 AS year_tag FROM MEMBER_GAPS_2024
    UNION ALL
    SELECT *, 2025 AS year_tag FROM MEMBER_GAPS_2025
  ),
  HISTORY_AGGREGATED AS (
    SELECT
      memberId,
      -- Normalize code to ensure join works even if casing differs
      UPPER(TRIM(hccCode)) AS hcc_code,
      UPPER(TRIM(REPLACE(hccModelVersion, 'v',''))) as hcc_model_version,
      COUNTIF(year_tag = 2023) AS count_2023,
      COUNTIF(year_tag = 2024) AS count_2024,
      COUNTIF(year_tag = 2025) AS count_2025,

      -- Data Sources Aggregated Per Year
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2023 THEN dataSource END, ', ') AS sources_2023,
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2024 THEN dataSource END, ', ') AS sources_2024,
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2025 THEN dataSource END, ', ') AS sources_2025,
      STRING_AGG(DISTINCT dataSource, ', ') AS aggregated_sources,

      -- Provider NPI Aggregated Per Year
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2023 THEN renderingNPI END, ' || ') AS provider_npi_2023,
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2024 THEN renderingNPI END, ' || ') AS provider_npi_2024,
      STRING_AGG(DISTINCT CASE WHEN year_tag = 2025 THEN renderingNPI END, ' || ') AS provider_npi_2025,
      STRING_AGG(DISTINCT renderingNPI, ', ') AS aggregated_provider_npi

    FROM ALL_HISTORY_UNNESTED
    GROUP BY 1, 2, 3
  )
SELECT
  r.memberId,
  r.hccCode,
  r.hccModelVersion,
  r.hccGapType,
  r.hccGapStatus,
  r.decision_category,
  r.actionReason,
  concat(r.userFirstName, ' ', r.userLastName) as providerName,
  r.userNpi as providerNPI,


  -- Populate 0 if no match found in history
  COALESCE(h.count_2023, 0) AS count_2023,
  COALESCE(h.count_2024, 0) AS count_2024,
  COALESCE(h.count_2025, 0) AS count_2025,

  -- Populate 0 if no match found in history
  COALESCE(h.sources_2023, null) AS sources_2023,
  COALESCE(h.sources_2024, null) AS sources_2024,
  COALESCE(h.sources_2025, null) AS sources_2025,


  -- Populate 0 if no match found in history
  COALESCE(h.provider_npi_2023, Null) AS provider_npi_2023,
  COALESCE(h.provider_npi_2024, Null) AS provider_npi_2024,
  COALESCE(h.provider_npi_2025, Null) AS provider_npi_2025,

FROM REJECTED_RECORDS r
LEFT JOIN HISTORY_AGGREGATED h
  ON
    r.memberId = h.memberId
    AND UPPER(TRIM(CAST(r.hccCode AS STRING))) = h.hcc_code AND UPPER(TRIM(CAST(r.hccModelVersion AS STRING))) = h.hcc_model_version
