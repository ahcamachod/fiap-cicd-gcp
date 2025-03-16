CREATE OR REPLACE VIEW `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.view_datelimiter` AS
select date('2022-01-01') as StartDate, date_sub(current_date, interval -6 month) as EndDate;