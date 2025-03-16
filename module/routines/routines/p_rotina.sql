CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.fiap_cicd_{ENV}_agnv.p_rotina`()
BEGIN 

BEGIN TRANSACTION;

insert into   `{PROJECT_ID}.fiap_cicd_{ENV}_agnv.tbl_data`
SELECT * FROM `{PROJECT_ID}.fiap_cicd_{ENV}_agnv.view_data`;

COMMIT TRANSACTION;

END;
