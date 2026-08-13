CREATE OR REPLACE VIEW sport_lakehouse.gold.members_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Current-member demographics, relocation, and marketing metrics"
  source: sport_lakehouse.silver.members
  filter: __END_AT IS NULL
  dimensions:
    - name: Gender
      expr: MEM_gender_Code
    - name: Nationality
      expr: MEM_nationality_Code
    - name: Country of Birth
      expr: MEM_country_of_birth_Code
    - name: Has Relocated
      expr: MEM_has_relocated_Flag
    - name: Relocation Year
      expr: MEM_relocation_year_No
    - name: Accepts Marketing
      expr: MEM_accepts_marketing_Flag
    - name: Birth Year
      expr: YEAR(MEM_birth_Dt)
    - name: Join Year
      expr: YEAR(MEM_created_Dts)
  measures:
    - name: Member Count
      expr: COUNT(MEM_Rk)
    - name: Marketing Opt-In Count
      expr: COUNT_IF(MEM_accepts_marketing_Flag = TRUE)
    - name: Marketing Opt-In Rate
      expr: COUNT_IF(MEM_accepts_marketing_Flag = TRUE) / COUNT(MEM_Rk)
    - name: Relocated Member Count
      expr: COUNT_IF(MEM_has_relocated_Flag = TRUE)
$$
