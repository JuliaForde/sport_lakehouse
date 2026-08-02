ADDRESSES = {
    "prefix": "ADR",
    "business_key": ["address_id"],
    "fk_rks": [],
    "column_mapping": {
        "address_id":        "ADR_Address_Id",
        "country":           "ADR_Country_Code",
        "county_name":       "ADR_County_Name",
        "latitude":          "ADR_Latitude_Dec",
        "longitude":         "ADR_Longitude_Dec",
        "municipality_name": "ADR_Municipality_Name",
        "postal_code":       "ADR_PostalCode_Code",
        "street_address":    "ADR_StreetAddress_Desc",
    },
}

SPORT_TYPES = {
    "prefix": "SPTP",
    "business_key": ["sport_type_id"],
    "fk_rks": [],
    "column_mapping": {
        "sport_type_id":     "SPTP_SportType_Id",
        "sport_type_name":   "SPTP_SportType_Name",
        "sport_mode":        "SPTP_SportMode_Code",
        "season_peak":       "SPTP_SeasonPeak_Code",
        "is_outdoor":        "SPTP_IsOutdoor_Flag",
        "popularity_weight": "SPTP_PopularityWeight_Dec",
    },
}

MEMBERS = {
    "prefix": "MEM",
    "business_key": ["member_id"],
    "fk_rks": [
        {"source_col": "MEM_Address_Id", "target_col": "MEM_ADR_Rk"},
    ],
    "column_mapping": {
        "member_id":            "MEM_Member_Id",
        "first_name":           "MEM_FirstName_Name",
        "last_name":            "MEM_LastName_Name",
        "gender":               "MEM_Gender_Code",
        "nationality":          "MEM_Nationality_Name",
        "country_of_birth":     "MEM_CountryOfBirth_Name",
        "moved_to_norway":      "MEM_HasRelocated_Flag",
        "moved_to_norway_year": "MEM_RelocationYear_No",
        "address_id":           "MEM_Address_Id",
        "email":                "MEM_Email_Desc",
        "birth_date":           "MEM_BirthDate_Dt",
        "created_at":           "MEM_CreatedAt_Ts",
        "phone":                "MEM_Phone_Desc",
        "marketing_opt_in":     "MEM_AcceptsMarketing_Flag",
    },
}

CLUBS = {
    "prefix": "CLB",
    "business_key": ["club_id"],
    "fk_rks": [
        {"source_col": "CLB_Address_Id", "target_col": "CLB_ADR_Rk"},
    ],
    "column_mapping": {
        "club_id":      "CLB_Club_Id",
        "club_name":    "CLB_Club_Name",
        "club_type":    "CLB_ClubType_Code",
        "address_id":   "CLB_Address_Id",
        "website":      "CLB_Website_Desc",
        "created_at":   "CLB_CreatedAt_Ts",
        "founded_year": "CLB_FoundedYear_No",
        "division":     "CLB_Division_Code",
        "is_active":    "CLB_IsActive_Flag",
    },
}

CLUB_SPORTS = {
    "prefix": "CLSP",
    "business_key": ["club_id", "sport_type_id"],
    "fk_rks": [
        {"source_col": "CLSP_Club_Id",     "target_col": "CLSP_CLB_Rk"},
        {"source_col": "CLSP_SportType_Id", "target_col": "CLSP_SPTP_Rk"},
    ],
    "column_mapping": {
        "club_id":       "CLSP_Club_Id",
        "sport_type_id": "CLSP_SportType_Id",
    },
}

AFFILIATIONS = {
    "prefix": "AFF",
    "business_key": ["affiliation_id"],
    "fk_rks": [
        {"source_col": "AFF_Member_Id", "target_col": "AFF_MEM_Rk"},
        {"source_col": "AFF_Club_Id",   "target_col": "AFF_CLB_Rk"},
    ],
    "column_mapping": {
        "affiliation_id": "AFF_Affiliation_Id",
        "member_id":      "AFF_Member_Id",
        "club_id":        "AFF_Club_Id",
        "start_date":     "AFF_StartDate_Dt",
        "end_date":       "AFF_EndDate_Dt",
        "reason":         "AFF_Reason_Desc",
        "created_at":     "AFF_CreatedAt_Ts",
        "updated_at":     "AFF_UpdatedAt_Ts",
    },
}

MEMBERSHIPS = {
    "prefix": "MEMB",
    "business_key": ["membership_id"],
    "fk_rks": [
        {"source_col": "MEMB_Member_Id", "target_col": "MEMB_MEM_Rk"},
    ],
    "column_mapping": {
        "membership_id":   "MEMB_Membership_Id",
        "member_id":       "MEMB_Member_Id",
        "membership_type": "MEMB_MembershipType_Code",
        "status":          "MEMB_Status_Code",
        "start_date":      "MEMB_StartDate_Dt",
        "end_date":        "MEMB_EndDate_Dt",
        "price_nok":       "MEMB_PriceNok_Dec",
        "created_at":      "MEMB_CreatedAt_Ts",
        "updated_at":      "MEMB_UpdatedAt_Ts",
    },
}

MEMBERSHIP_PAYMENTS = {
    "prefix": "MEPA",
    "business_key": ["payment_id"],
    "fk_rks": [
        {"source_col": "MEPA_Member_Id",     "target_col": "MEPA_MEM_Rk"},
        {"source_col": "MEPA_Membership_Id", "target_col": "MEPA_MEMB_Rk"},
    ],
    "column_mapping": {
        "payment_id":    "MEPA_Payment_Id",
        "membership_id": "MEPA_Membership_Id",
        "member_id":     "MEPA_Member_Id",
        "amount_nok":    "MEPA_AmountNok_Dec",
        "currency":      "MEPA_Currency_Code",
        "method":        "MEPA_Method_Code",
        "status":        "MEPA_Status_Code",
        "attempt":       "MEPA_Attempt_No",
        "created_at":    "MEPA_CreatedAt_Ts",
        "paid_at":       "MEPA_PaidAt_Ts",
        "updated_at":    "MEPA_UpdatedAt_Ts",
    },
}

COMPETITIONS = {
    "prefix": "COMP",
    "business_key": ["competition_id"],
    "fk_rks": [
        {"source_col": "COMP_Address_Id",   "target_col": "COMP_ADR_Rk"},
        {"source_col": "COMP_HostClub_Id",  "target_col": "COMP_CLB_Rk"},
        {"source_col": "COMP_SportType_Id", "target_col": "COMP_SPTP_Rk"},
    ],
    "column_mapping": {
        "competition_id":        "COMP_Competition_Id",
        "name":                  "COMP_Competition_Name",
        "sport_type_id":         "COMP_SportType_Id",
        "host_club_id":          "COMP_HostClub_Id",
        "address_id":            "COMP_Address_Id",
        "venue":                 "COMP_Venue_Desc",
        "level":                 "COMP_Level_Code",
        "status":                "COMP_Status_Code",
        "capacity":              "COMP_Capacity_No",
        "start_date":            "COMP_StartDate_Dt",
        "end_date":              "COMP_EndDate_Dt",
        "registration_deadline": "COMP_RegistrationDeadline_Dt",
        "created_at":            "COMP_CreatedAt_Ts",
        "updated_at":            "COMP_UpdatedAt_Ts",
        "entry_fee_nok":         "COMP_EntryFeeNok_Dec",
        "prize_pool_nok":        "COMP_PrizePoolNok_Dec",
        "is_outdoor":            "COMP_IsOutdoor_Flag",
    },
}

COMPETITION_WAITLIST = {
    "prefix": "COWL",
    "business_key": ["waitlist_id"],
    "fk_rks": [
        {"source_col": "COWL_Competition_Id",        "target_col": "COWL_COMP_Rk"},
        {"source_col": "COWL_Member_Id",             "target_col": "COWL_MEM_Rk"},
        {"source_col": "COWL_Club_Id",               "target_col": "COWL_CLB_Rk"},
        {"source_col": "COWL_PromotedParticipation_Id", "target_col": "COWL_PART_Rk"},
    ],
    "column_mapping": {
        "waitlist_id":               "COWL_Waitlist_Id",
        "competition_id":            "COWL_Competition_Id",
        "member_id":                 "COWL_Member_Id",
        "club_id":                   "COWL_Club_Id",
        "added_at":                  "COWL_AddedAt_Ts",
        "status":                    "COWL_Status_Code",
        "promoted_participation_id": "COWL_PromotedParticipation_Id",
        "updated_at":                "COWL_UpdatedAt_Ts",
    },
}

PARTICIPATION = {
    "prefix": "PART",
    "business_key": ["participation_id"],
    "fk_rks": [
        {"source_col": "PART_Competition_Id", "target_col": "PART_COMP_Rk"},
        {"source_col": "PART_Member_Id",      "target_col": "PART_MEM_Rk"},
        {"source_col": "PART_Club_Id",        "target_col": "PART_CLB_Rk"},
    ],
    "column_mapping": {
        "participation_id":  "PART_Participation_Id",
        "competition_id":    "PART_Competition_Id",
        "member_id":         "PART_Member_Id",
        "club_id":           "PART_Club_Id",
        "registered_at":     "PART_RegisteredAt_Ts",
        "status":            "PART_Status_Code",
        "status_updated_at": "PART_StatusUpdatedAt_Ts",
        "source":            "PART_Source_Code",
        "bib_number":        "PART_BibNumber_No",
    },
}

RESULTS = {
    "prefix": "RES",
    "business_key": ["result_id"],
    "fk_rks": [
        {"source_col": "RES_Participation_Id", "target_col": "RES_PART_Rk"},
    ],
    "column_mapping": {
        "result_id":        "RES_Result_Id",
        "participation_id": "RES_Participation_Id",
        "position":         "RES_Position_No",
        "score":            "RES_Score_No",
        "time_seconds":     "RES_TimeSeconds_No",
        "notes":            "RES_Notes_Desc",
        "is_official":      "RES_IsOfficial_Flag",
        "recorded_at":      "RES_RecordedAt_Ts",
        "corrected_at":     "RES_CorrectedAt_Ts",
    },
}
