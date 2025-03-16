CREATE OR REPLACE VIEW `{PROJECT_ID}.fiap_cicd_{ENV}_agnv.view_datas` AS
select date('2022-01-01') as DataInicio, date_sub(current_date, interval -6 month) as DataFinal;
