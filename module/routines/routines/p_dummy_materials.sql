CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.p_dummy_materials`()
BEGIN 

DECLARE Process_Name	STRING Default 'Update dummy materials';
DECLARE Seq_Module		STRING Default Process_name;
DECLARE Seq_Message   STRING Default 'begin';
DECLARE Seq_Rows		  int64 Default 0;

BEGIN TRANSACTION;

Call `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.p_shared_process_status_ins_verbose`(Process_Name,Seq_Module,Seq_Message, @@row_count
      ,@@query_label,@@dataset_project_id,@@dataset_id,@@project_id,@@script.bytes_processed,@@script.bytes_billed	
      ,@@script.num_child_jobs,@@script.job_id);

insert into   `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.tbl_material_dim`
(SK_MaterialID,
Material_UniqueID,
UPC,
OPERATIONAL_SIGNATURE_CODE,
OPERATIONAL_SIGNATURE_LABEL,
OPERATIONAL_BRAND_CODE,
OPERATIONAL_BRAND_LABEL,
OPERATIONAL_SUB_BRAND_CODE,
OPERATIONAL_SUBBRAND_LABEL,
OPERATIONAL_AXE_CODE,
OPERATIONAL_AXE_LABEL,
OPERATIONAL_SUB_AXE_CODE,
OPERATIONAL_SUB_AXE_LABEL,
OPERATIONAL_METIER_CODE,
OPERATIONAL_METIER_LABEL,
Category,
SUB_CATEGORY,
Material_Group_1,
Material_Group_4,
operational_division,
source_system_material
)
WITH Operational_division as  (
select distinct  OPERATIONAL_SIGNATURE_CODE, operational_division from  `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.tbl_material_dim`

where country_code = 'USA' AND operational_division IN ('CPD','PPD','LLD', 'LDB') AND OPERATIONAL_SIGNATURE_CODE IS NOT NULL
)

select 
farm_fingerprint(md5(CONCAT('USA_',upc_key) )) as SK_MaterialID,
CONCAT('USA_',upc_key) AS Material_UniqueID,
upc_Code	as 	UPC	,
Signature_Code	as 	OPERATIONAL_SIGNATURE_CODE	,
Signature_Description	as 	OPERATIONAL_SIGNATURE_LABEL	,
Brand_Code	as 	OPERATIONAL_BRAND_CODE	,
Brand_Description	as 	OPERATIONAL_BRAND_LABEL	,
Sub_Brand_Code	as 	OPERATIONAL_SUB_BRAND_CODE	,
Sub_Brand_Description	as 	OPERATIONAL_SUBBRAND_LABEL	,
Axe_Code	as 	OPERATIONAL_AXE_CODE	,
Axe_Description	as 	OPERATIONAL_AXE_LABEL	,
Sub_Axe_Code	as 	OPERATIONAL_SUB_AXE_CODE	,
Sub_Axe_Description	as 	OPERATIONAL_SUB_AXE_LABEL	,
Class_Code	as 	OPERATIONAL_METIER_CODE	,
Class_Description	as 	OPERATIONAL_METIER_LABEL	,
Category	as 	Category	,
SubCategory	as 	SUB_CATEGORY	,

Material_Group_1_Description	as 	Material_Group_1	,

Material_Group_4_Description	as 	Material_Group_4	,
b.operational_division ,
'Sellout Dummy UPCs' as source_system_material


from 
--amer-a07-us-amer-pd.amer_sisohub_bqdset_pd_curated.tbl_lookup_dummy_upc a 
`{PUBLISH}.btdp_ds_c1_3a0_amerso_eu_{ITG}.lookup_dummy_upc_v1` a -- changed by Bryan on 20250307

join Operational_division b
on a.Signature_Code = b.OPERATIONAL_SIGNATURE_CODE;


Call `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.p_shared_process_status_ins_verbose`(Process_Name,Seq_Module,'insert', @@row_count
      ,@@query_label,@@dataset_project_id,@@dataset_id,@@project_id,@@script.bytes_processed,@@script.bytes_billed	
      ,@@script.num_child_jobs,@@script.job_id);

Call `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.p_shared_process_status_ins_verbose`(Process_Name,Seq_Module,'end', @@row_count
      ,@@query_label,@@dataset_project_id,@@dataset_id,@@project_id,@@script.bytes_processed,@@script.bytes_billed	
      ,@@script.num_child_jobs,@@script.job_id);

COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN

ROLLBACK TRANSACTION;

  Call `{PROJECT_ID}.amer_sisohub_bqdset_{ENV}_cdm_commerce.p_shared_process_status_ins_verbose`(
            'Update dummy materials',  
            cast(@@error.message as string),
            'error', -99,
            cast( @@error.statement_text || '|' || @@error.formatted_stack_trace as string),
             @@dataset_project_id,@@dataset_id,@@project_id,@@script.bytes_processed,@@script.bytes_billed,@@script.num_child_jobs,@@script.job_id);   

END;