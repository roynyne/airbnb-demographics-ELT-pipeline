---------------------------
-- Create Schema: Bronze
---------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

---------------------------
-- Staging: Airbnb Listings
---------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_airbnb_listings (
  listing_id VARCHAR,
  scrape_id VARCHAR,
  scraped_date VARCHAR,
  host_id VARCHAR,
  host_name VARCHAR,
  host_since VARCHAR,
  host_is_superhost VARCHAR,
  host_neighbourhood VARCHAR,
  listing_neighbourhood VARCHAR,
  property_type VARCHAR,
  room_type VARCHAR,
  accommodates VARCHAR,
  price VARCHAR,
  has_availability VARCHAR,
  availability_30 VARCHAR,
  number_of_reviews VARCHAR,
  review_scores_rating VARCHAR,
  review_scores_accuracy VARCHAR,
  review_scores_cleanliness VARCHAR,
  review_scores_checkin VARCHAR,
  review_scores_communication VARCHAR,
  review_scores_value VARCHAR
);

---------------------------
-- Staging: LGA Codes
---------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_lga_codes (
  lga_code VARCHAR,
  lga_name VARCHAR
);

---------------------------
-- Staging: LGA Suburb Mapping
---------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_lga_suburbs (
  lga_name VARCHAR,
  suburb_name VARCHAR
);

---------------------------
-- Staging: Census G02 Data
---------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_census_g02 (
  lga_code_2016 VARCHAR,
  median_age_persons VARCHAR,
  median_mortgage_repay_monthly VARCHAR,
  median_tot_prsnl_inc_weekly VARCHAR,
  median_rent_weekly VARCHAR,
  median_tot_fam_inc_weekly VARCHAR,
  average_num_psns_per_bedroom VARCHAR,
  median_tot_hhd_inc_weekly VARCHAR,
  average_household_size VARCHAR
);

---------------------------
-- Staging: Census G01 Data
---------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_census_g01 (
  lga_code_2016 VARCHAR,
  Tot_P_M VARCHAR,
  Tot_P_F VARCHAR,
  Tot_P_P VARCHAR,
  Age_0_4_yr_M VARCHAR,
  Age_0_4_yr_F VARCHAR,
  Age_0_4_yr_P VARCHAR,
  Age_5_14_yr_M VARCHAR,
  Age_5_14_yr_F VARCHAR,
  Age_5_14_yr_P VARCHAR,
  Age_15_19_yr_M VARCHAR,
  Age_15_19_yr_F VARCHAR,
  Age_15_19_yr_P VARCHAR,
  Age_20_24_yr_M VARCHAR,
  Age_20_24_yr_F VARCHAR,
  Age_20_24_yr_P VARCHAR,
  Age_25_34_yr_M VARCHAR,
  Age_25_34_yr_F VARCHAR,
  Age_25_34_yr_P VARCHAR,
  Age_35_44_yr_M VARCHAR,
  Age_35_44_yr_F VARCHAR,
  Age_35_44_yr_P VARCHAR,
  Age_45_54_yr_M VARCHAR,
  Age_45_54_yr_F VARCHAR,
  Age_45_54_yr_P VARCHAR,
  Age_55_64_yr_M VARCHAR,
  Age_55_64_yr_F VARCHAR,
  Age_55_64_yr_P VARCHAR,
  Age_65_74_yr_M VARCHAR,
  Age_65_74_yr_F VARCHAR,
  Age_65_74_yr_P VARCHAR,
  Age_75_84_yr_M VARCHAR,
  Age_75_84_yr_F VARCHAR,
  Age_75_84_yr_P VARCHAR,
  Age_85ov_M VARCHAR,
  Age_85ov_F VARCHAR,
  Age_85ov_P VARCHAR,
  Counted_Census_Night_home_M VARCHAR,
  Counted_Census_Night_home_F VARCHAR,
  Counted_Census_Night_home_P VARCHAR,
  Count_Census_Nt_Ewhere_Aust_M VARCHAR,
  Count_Census_Nt_Ewhere_Aust_F VARCHAR,
  Count_Census_Nt_Ewhere_Aust_P VARCHAR,
  Indigenous_psns_Aboriginal_M VARCHAR,
  Indigenous_psns_Aboriginal_F VARCHAR,
  Indigenous_psns_Aboriginal_P VARCHAR,
  Indig_psns_Torres_Strait_Is_M VARCHAR,
  Indig_psns_Torres_Strait_Is_F VARCHAR,
  Indig_psns_Torres_Strait_Is_P VARCHAR,
  Indig_Bth_Abor_Torres_St_Is_M VARCHAR,
  Indig_Bth_Abor_Torres_St_Is_F VARCHAR,
  Indig_Bth_Abor_Torres_St_Is_P VARCHAR,
  Indigenous_P_Tot_M VARCHAR,
  Indigenous_P_Tot_F VARCHAR,
  Indigenous_P_Tot_P VARCHAR,
  Birthplace_Australia_M VARCHAR,
  Birthplace_Australia_F VARCHAR,
  Birthplace_Australia_P VARCHAR,
  Birthplace_Elsewhere_M VARCHAR,
  Birthplace_Elsewhere_F VARCHAR,
  Birthplace_Elsewhere_P VARCHAR,
  Lang_spoken_home_Eng_only_M VARCHAR,
  Lang_spoken_home_Eng_only_F VARCHAR,
  Lang_spoken_home_Eng_only_P VARCHAR,
  Lang_spoken_home_Oth_Lang_M VARCHAR,
  Lang_spoken_home_Oth_Lang_F VARCHAR,
  Lang_spoken_home_Oth_Lang_P VARCHAR,
  Australian_citizen_M VARCHAR,
  Australian_citizen_F VARCHAR,
  Australian_citizen_P VARCHAR,
  Age_psns_att_educ_inst_0_4_M VARCHAR,
  Age_psns_att_educ_inst_0_4_F VARCHAR,
  Age_psns_att_educ_inst_0_4_P VARCHAR,
  Age_psns_att_educ_inst_5_14_M VARCHAR,
  Age_psns_att_educ_inst_5_14_F VARCHAR,
  Age_psns_att_educ_inst_5_14_P VARCHAR,
  Age_psns_att_edu_inst_15_19_M VARCHAR,
  Age_psns_att_edu_inst_15_19_F VARCHAR,
  Age_psns_att_edu_inst_15_19_P VARCHAR,
  Age_psns_att_edu_inst_20_24_M VARCHAR,
  Age_psns_att_edu_inst_20_24_F VARCHAR,
  Age_psns_att_edu_inst_20_24_P VARCHAR,
  Age_psns_att_edu_inst_25_ov_M VARCHAR,
  Age_psns_att_edu_inst_25_ov_F VARCHAR,
  Age_psns_att_edu_inst_25_ov_P VARCHAR,
  High_yr_schl_comp_Yr_12_eq_M VARCHAR,
  High_yr_schl_comp_Yr_12_eq_F VARCHAR,
  High_yr_schl_comp_Yr_12_eq_P VARCHAR,
  High_yr_schl_comp_Yr_11_eq_M VARCHAR,
  High_yr_schl_comp_Yr_11_eq_F VARCHAR,
  High_yr_schl_comp_Yr_11_eq_P VARCHAR,
  High_yr_schl_comp_Yr_10_eq_M VARCHAR,
  High_yr_schl_comp_Yr_10_eq_F VARCHAR,
  High_yr_schl_comp_Yr_10_eq_P VARCHAR,
  High_yr_schl_comp_Yr_9_eq_M VARCHAR,
  High_yr_schl_comp_Yr_9_eq_F VARCHAR,
  High_yr_schl_comp_Yr_9_eq_P VARCHAR,
  High_yr_schl_comp_Yr_8_belw_M VARCHAR,
  High_yr_schl_comp_Yr_8_belw_F VARCHAR,
  High_yr_schl_comp_Yr_8_belw_P VARCHAR,
  High_yr_schl_comp_D_n_g_sch_M VARCHAR,
  High_yr_schl_comp_D_n_g_sch_F VARCHAR,
  High_yr_schl_comp_D_n_g_sch_P VARCHAR,
  Count_psns_occ_priv_dwgs_M VARCHAR,
  Count_psns_occ_priv_dwgs_F VARCHAR,
  Count_psns_occ_priv_dwgs_P VARCHAR,
  Count_Persons_other_dwgs_M VARCHAR,
  Count_Persons_other_dwgs_F VARCHAR,
  Count_Persons_other_dwgs_P VARCHAR
);



-- View data from raw_airbnb_listings
SELECT * FROM bronze.raw_airbnb_listings LIMIT 10;

-- View data from raw_census_g01
SELECT * FROM bronze.raw_census_g01 LIMIT 10;

-- View data from raw_census_g02
SELECT * FROM bronze.raw_census_g02 LIMIT 10;

-- View data from raw_lga_codes
SELECT * FROM bronze.raw_lga_codes LIMIT 10;

-- View data from raw_lga_suburbs
SELECT * FROM bronze.raw_lga_suburbs LIMIT 10;