ADDRESSES = {
    "prefix": "ADR",
    "business_key": ["address_id"],
    "fk_lookups": [],
    "column_mapping": {
        "address_id":        "ADR_address_Id",
        "country":           "ADR_country_Code",
        "county_name":       "ADR_county_Desc",
        "latitude":          "ADR_latitude_Dec",
        "longitude":         "ADR_longitude_Dec",
        "municipality_name": "ADR_municipality_Desc",
        "postal_code":       "ADR_postal_code_Code",
        "street_address":    "ADR_street_Desc",
    },
}

SPORT_TYPES = {
    "prefix": "SPTP",
    "business_key": ["sport_type_id"],
    "fk_lookups": [],
    "column_mapping": {
        "sport_type_id":     "SPTP_sport_type_Id",
        "sport_type_name":   "SPTP_name_Desc",
        "sport_mode":        "SPTP_mode_Code",
        "season_peak":       "SPTP_peak_season_Code",
        "is_outdoor":        "SPTP_is_outdoor_Flag",
        "popularity_weight": "SPTP_popularity_weight_Dec",
    },
}

MEMBERS = {
    "prefix": "MEM",
    "business_key": ["member_id"],
    "fk_lookups": [
        {
            "source_col":     "MEM_address_Id",
            "lookup_table":   "sport_lakehouse.silver.addresses",
            "lookup_bk_col":  "ADR_address_Id",
            "lookup_rk_col":  "ADR_Rk",
            "target_fk_col":  "MEM_ADR_Rk",
            "event_time_col": "_ingest_ts",
        }
    ],
    "column_mapping": {
        "member_id":            "MEM_member_Id",
        "first_name":           "MEM_first_name_Desc",
        "last_name":            "MEM_last_name_Desc",
        "gender":               "MEM_gender_Code",
        "nationality":          "MEM_nationality_Code",
        "country_of_birth":     "MEM_country_of_birth_Code",
        "moved_to_norway":      "MEM_has_relocated_Flag",
        "moved_to_norway_year": "MEM_relocation_year_No",
        "address_id":           "MEM_address_Id",
        "email":                "MEM_email_Desc",
        "birth_date":           "MEM_birth_Dt",
        "created_at":           "MEM_created_Dts",
        "phone":                "MEM_phone_Desc",
        "marketing_opt_in":     "MEM_accepts_marketing_Flag",
    },
}

CLUBS = {
    "prefix": "CLB",
    "business_key": ["club_id"],
    "fk_lookups": [
        {
            "source_col":     "CLB_address_Id",
            "lookup_table":   "sport_lakehouse.silver.addresses",
            "lookup_bk_col":  "ADR_address_Id",
            "lookup_rk_col":  "ADR_Rk",
            "target_fk_col":  "CLB_ADR_Rk",
            "event_time_col": "_ingest_ts",
        }
    ],
    "column_mapping": {
        "club_id":    "CLB_club_Id",
        "club_name":  "CLB_name_Desc",
        "club_type":  "CLB_type_Code",
        "address_id": "CLB_address_Id",
        "website":    "CLB_website_url_Desc",
        "created_at": "CLB_created_Dts",
        "founded_year": "CLB_founded_year_No",
        "division":     "CLB_division_Code",
        "is_active":    "CLB_is_active_Flag",
    },
}

CLUB_SPORTS = {
    "prefix": "CLSP",
    "business_key": ["club_id", "sport_type_id"],
    "fk_lookups": [
        {
            "source_col":     "CLSP_club_Id",
            "lookup_table":   "sport_lakehouse.silver.clubs",
            "lookup_bk_col":  "CLB_club_Id",
            "lookup_rk_col":  "CLB_Rk",
            "target_fk_col":  "CLSP_CLB_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "CLSP_sport_type_Id",
            "lookup_table":   "sport_lakehouse.silver.sport_types",
            "lookup_bk_col":  "SPTP_sport_type_Id",
            "lookup_rk_col":  "SPTP_Rk",
            "target_fk_col":  "CLSP_SPTP_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "club_id":       "CLSP_club_Id",
        "sport_type_id": "CLSP_sport_type_Id",
    },
}

AFFILIATIONS = {
    "prefix": "AFF",
    "business_key": ["affiliation_id"],
    "fk_lookups": [
        {
            "source_col":     "AFF_member_Id",
            "lookup_table":   "sport_lakehouse.silver.members",
            "lookup_bk_col":  "MEM_member_Id",
            "lookup_rk_col":  "MEM_Rk",
            "target_fk_col":  "AFF_MEM_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "AFF_club_Id",
            "lookup_table":   "sport_lakehouse.silver.clubs",
            "lookup_bk_col":  "CLB_club_Id",
            "lookup_rk_col":  "CLB_Rk",
            "target_fk_col":  "AFF_CLB_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "affiliation_id": "AFF_affiliation_Id",
        "member_id":      "AFF_member_Id",
        "club_id":        "AFF_club_Id",
        "start_date":     "AFF_start_Dt",
        "end_date":       "AFF_end_Dt",
        "reason":         "AFF_reason_Desc",
        "created_at":     "AFF_created_Dts",
        "updated_at":     "AFF_updated_Dts",
    },
}

MEMBERSHIPS = {
    "prefix": "MEMB",
    "business_key": ["membership_id"],
    "fk_lookups": [
        {
            "source_col":     "MEMB_member_Id",
            "lookup_table":   "sport_lakehouse.silver.members",
            "lookup_bk_col":  "MEM_member_Id",
            "lookup_rk_col":  "MEM_Rk",
            "target_fk_col":  "MEMB_MEM_Rk",
            "event_time_col": "_ingest_ts",
        }
    ],
    "column_mapping": {
        "membership_id":   "MEMB_membership_Id",
        "member_id":       "MEMB_member_Id",
        "membership_type": "MEMB_type_Code",
        "status":          "MEMB_status_Code",
        "start_date":      "MEMB_start_Dt",
        "end_date":        "MEMB_end_Dt",
        "price_nok":       "MEMB_price_Dec",
        "created_at":      "MEMB_created_Dts",
        "updated_at":      "MEMB_updated_Dts",
    },
}

MEMBERSHIP_PAYMENTS = {
    "prefix": "MEPA",
    "business_key": ["payment_id"],
    "fk_lookups": [
        {
            "source_col":     "MEPA_member_Id",
            "lookup_table":   "sport_lakehouse.silver.members",
            "lookup_bk_col":  "MEM_member_Id",
            "lookup_rk_col":  "MEM_Rk",
            "target_fk_col":  "MEPA_MEM_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "MEPA_membership_Id",
            "lookup_table":   "sport_lakehouse.silver.memberships",
            "lookup_bk_col":  "MEMB_membership_Id",
            "lookup_rk_col":  "MEMB_Rk",
            "target_fk_col":  "MEPA_MEMB_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "payment_id":    "MEPA_payment_Id",
        "membership_id": "MEPA_membership_Id",
        "member_id":     "MEPA_member_Id",
        "amount_nok":    "MEPA_amount_Dec",
        "currency":      "MEPA_currency_Code",
        "method":        "MEPA_method_Code",
        "status":        "MEPA_status_Code",
        "attempt":       "MEPA_attempt_No",
        "created_at":    "MEPA_created_Dts",
        "paid_at":       "MEPA_paid_Dts",
        "updated_at":    "MEPA_updated_Dts",
    },
}

COMPETITIONS = {
    "prefix": "COMP",
    "business_key": ["competition_id"],
    "fk_lookups": [
        {
            "source_col":     "COMP_address_Id",
            "lookup_table":   "sport_lakehouse.silver.addresses",
            "lookup_bk_col":  "ADR_address_Id",
            "lookup_rk_col":  "ADR_Rk",
            "target_fk_col":  "COMP_ADR_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "COMP_host_club_Id",
            "lookup_table":   "sport_lakehouse.silver.clubs",
            "lookup_bk_col":  "CLB_club_Id",
            "lookup_rk_col":  "CLB_Rk",
            "target_fk_col":  "COMP_CLB_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "COMP_sport_type_Id",
            "lookup_table":   "sport_lakehouse.silver.sport_types",
            "lookup_bk_col":  "SPTP_sport_type_Id",
            "lookup_rk_col":  "SPTP_Rk",
            "target_fk_col":  "COMP_SPTP_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "competition_id":        "COMP_competition_Id",
        "name":                  "COMP_name_Desc",
        "sport_type_id":         "COMP_sport_type_Id",
        "host_club_id":          "COMP_host_club_Id",
        "address_id":            "COMP_address_Id",
        "venue":                 "COMP_venue_Desc",
        "level":                 "COMP_level_Code",
        "status":                "COMP_status_Code",
        "capacity":              "COMP_capacity_No",
        "start_date":            "COMP_start_Dt",
        "end_date":              "COMP_end_Dt",
        "registration_deadline": "COMP_reg_deadline_Dt",
        "created_at":            "COMP_created_Dt",
        "updated_at":            "COMP_updated_Dts",
        "entry_fee_nok":         "COMP_entry_fee_No",
        "prize_pool_nok":        "COMP_prize_pool_No",
        "is_outdoor":            "COMP_is_outdoor_Flag",
    },
}

COMPETITION_WAITLIST = {
    "prefix": "COWL",
    "business_key": ["waitlist_id"],
    "fk_lookups": [
        {
            "source_col":     "COWL_competition_Id",
            "lookup_table":   "sport_lakehouse.silver.competitions",
            "lookup_bk_col":  "COMP_competition_Id",
            "lookup_rk_col":  "COMP_Rk",
            "target_fk_col":  "COWL_COMP_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "COWL_member_Id",
            "lookup_table":   "sport_lakehouse.silver.members",
            "lookup_bk_col":  "MEM_member_Id",
            "lookup_rk_col":  "MEM_Rk",
            "target_fk_col":  "COWL_MEM_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "COWL_club_Id",
            "lookup_table":   "sport_lakehouse.silver.clubs",
            "lookup_bk_col":  "CLB_club_Id",
            "lookup_rk_col":  "CLB_Rk",
            "target_fk_col":  "COWL_CLB_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "waitlist_id":               "COWL_waitlist_Id",
        "competition_id":            "COWL_competition_Id",
        "member_id":                 "COWL_member_Id",
        "club_id":                   "COWL_club_Id",
        "added_at":                  "COWL_added_Dts",
        "status":                    "COWL_status_Code",
        "promoted_participation_id": "COWL_promoted_part_Id",
        "updated_at":                "COWL_updated_Dts",
    },
}

PARTICIPATION = {
    "prefix": "PART",
    "business_key": ["participation_id"],
    "fk_lookups": [
        {
            "source_col":     "PART_competition_Id",
            "lookup_table":   "sport_lakehouse.silver.competitions",
            "lookup_bk_col":  "COMP_competition_Id",
            "lookup_rk_col":  "COMP_Rk",
            "target_fk_col":  "PART_COMP_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "PART_member_Id",
            "lookup_table":   "sport_lakehouse.silver.members",
            "lookup_bk_col":  "MEM_member_Id",
            "lookup_rk_col":  "MEM_Rk",
            "target_fk_col":  "PART_MEM_Rk",
            "event_time_col": "_ingest_ts",
        },
        {
            "source_col":     "PART_club_Id",
            "lookup_table":   "sport_lakehouse.silver.clubs",
            "lookup_bk_col":  "CLB_club_Id",
            "lookup_rk_col":  "CLB_Rk",
            "target_fk_col":  "PART_CLB_Rk",
            "event_time_col": "_ingest_ts",
        },
    ],
    "column_mapping": {
        "participation_id":  "PART_participation_Id",
        "competition_id":    "PART_competition_Id",
        "member_id":         "PART_member_Id",
        "club_id":           "PART_club_Id",
        "registered_at":     "PART_registered_Dts",
        "status":            "PART_status_Code",
        "status_updated_at": "PART_status_updated_Dts",
        "source":            "PART_source_Code",
        "bib_number":        "PART_bib_No",
    },
}

RESULTS = {
    "prefix": "RES",
    "business_key": ["result_id"],
    "fk_lookups": [
        {
            "source_col":     "RES_participation_Id",
            "lookup_table":   "sport_lakehouse.silver.participation",
            "lookup_bk_col":  "PART_participation_Id",
            "lookup_rk_col":  "PART_Rk",
            "target_fk_col":  "RES_PART_Rk",
            "event_time_col": "_ingest_ts",
        }
    ],
    "column_mapping": {
        "result_id":        "RES_result_Id",
        "participation_id": "RES_participation_Id",
        "position":         "RES_position_No",
        "score":            "RES_score_No",
        "time_seconds":     "RES_time_seconds_No",
        "notes":            "RES_notes_Desc",
        "is_official":      "RES_is_official_Flag",
        "recorded_at":      "RES_recorded_Dts",
        "corrected_at":     "RES_corrected_Dts",
    },
}

# -----------------------------------------------------------------------
# Aktiveres etter at daily notebooks er køyrt:
# CLUB_WEATHER_DAILY, CLUB_WEATHER_MONITORING, WEATHER_STATIONS
# -----------------------------------------------------------------------
