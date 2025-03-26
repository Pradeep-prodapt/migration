CREATE OR REPLACE PACKAGE BODY ZAYOMSS.PKG_MIG_EWO_OBJ
AS
   ----------------------------------------------------------------------------

   --
   --  Usage: Loads data from ZAYOMSS to target
   --
   --  Modification Log
   --  Modifier          		Date        Description
   ----------------------------------------------------------------------------------------------------------
   --  Pradeep kumar D  	01/13/2025   	Initial Creation
   --  Pradeep kumar D    	01/13/2025   	Added procedures to handle order and order details
  
   -----------------------------------------------------------------------------------------------------------
--CURSOR TO GET DATA FROM STAGING MAIN TABLE
   CURSOR get_ewo_stg 
   IS
        SELECT *
          FROM ZAYOMSS.STG_EWO_MAIN a
         WHERE   NOT EXISTS
                      (SELECT 1 FROM ZAYOMSS.aref_so_ewo
                        WHERE stg_order_id = a.stg_ewo_order_id) 
      ORDER BY stg_ewo_order_id;
	  
--CURSOR TO GET DATA FROM STAGING DETAIL TABLE
   CURSOR get_ewo_stg_detail
   IS
        SELECT CASE
                  WHEN gen_object_case = 'CIRCUIT' THEN 1
                  WHEN gen_object_case = 'EQUIPMENT' THEN 2
                  WHEN gen_object_case = 'NOTES' THEN 3
                  WHEN gen_object_case = 'ATTACHMENT' THEN 4
                  WHEN gen_object_case = 'SUPP_HISTORY' THEN 5
                  ELSE 99
               END
                  AS goc_seq,
               d.*
          FROM ZAYOMSS.STG_EWO_DETAIL d
         WHERE     stg_ewo_order_id = gv_order_id
                AND NVL (stg_leg_ckt_ecckt_type, 'XYZ') <> 'CLM'
      ORDER BY goc_seq;

  -- CURSOR get_user
  -- IS
  --    SELECT *
  --      FROM ZAYOMSS.STG_EWO_USER_DATA
  --     WHERE     stg_ewo_document_number = gv_order_id;
             --AND gen_source = gv_source
             --AND stg_ewo_value_type = 'UDD';

   grec_ewo          get_ewo_stg%ROWTYPE;
   grec_ewo_detail   get_ewo_stg_detail%ROWTYPE;
   --grec_ewo_user     get_user%ROWTYPE;

   FUNCTION func_location_id (arg_clli_code VARCHAR2)
      RETURN NUMBER
   IS
      lv_location_id_func   NUMBER;
   BEGIN
      IF arg_clli_code IS NULL
      THEN
         lv_location_id_func := 0;
      ELSE
         BEGIN
            SELECT MAX (location_id)
              INTO lv_location_id_func
              FROM asap.network_location
             WHERE clli_code = arg_clli_code;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               lv_location_id_func := 0;
         END;

         IF lv_location_id_func = ''
         THEN
            lv_location_id_func := 0;
         END IF;
      END IF;

      RETURN lv_location_id_func;
   END;


   FUNCTION func_circuit_design_id (arg_ecckt VARCHAR2)
      RETURN NUMBER
   IS
      lv_circuit_design_id   NUMBER;
   BEGIN
      BEGIN
         SELECT circuit_design_id
           INTO lv_circuit_design_id
           FROM asap.circuit
          WHERE exchange_carrier_circuit_id = arg_ecckt;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_circuit_design_id := 0;
         WHEN TOO_MANY_ROWS
         THEN
            lv_circuit_design_id := 0;
      END;

      IF lv_circuit_design_id = ''
      THEN
         lv_circuit_design_id := 0;
      END IF;

      RETURN lv_circuit_design_id;
   END;

   PROCEDURE insert_performance_dashboard (
      arg_program_function     IN ZAYOMSS.conv_performance_dashboard.program_function%TYPE,
      --arg_program_thread_id    IN ZAYOMSS.conv_performance_dashboard.program_thread_id%TYPE,
      arg_program_start_time   IN ZAYOMSS.conv_performance_dashboard.program_start_time%TYPE,
      arg_total_count          IN ZAYOMSS.conv_performance_dashboard.program_records_processed%TYPE,
      arg_error_count          IN ZAYOMSS.conv_performance_dashboard.error_record_cnt%TYPE,
      arg_target_count         IN ZAYOMSS.conv_performance_dashboard.target_record_cnt%TYPE)
   IS
      --error description
      lv_err_desc   ZAYOMSS.conv_conversion_error_log.error_description%TYPE := NULL;
   BEGIN
      INSERT INTO ZAYOMSS.conv_performance_dashboard (program_name,
                                                      program_function,
                                                      --program_thread_id,
                                                      program_run_date,
                                                      program_start_time,
                                                      program_status_time,
                                                      program_records_processed,
                                                      error_record_cnt,
                                                      target_record_cnt)
           VALUES (gv_package_name,                                          --program_name,
                   arg_program_function,                                 --program_function,
                   --arg_program_thread_id,                               --program_thread_id,
                   TO_DATE (gv_run_dt, 'YYYYMMDD'),                      --program_run_date,
                   arg_program_start_time,                             --program_start_time,
                   SYSDATE,                                            --program_status_time
                   arg_total_count,                             --program_records_processed,
                   arg_error_count,                                      --error_record_cnt,
                   arg_target_count                                     --target_record_cnt)
                                   );
   EXCEPTION
      WHEN OTHERS
      THEN
         --lv_err_desc:= SQLERRM;
         lv_err_desc :=
               'FATAL: unable to insert log for '
            || 'program_name='
            || gv_package_name
            || 'program_function='
            || arg_program_function
            || 'error_message='
            || SQLERRM;
         RAISE gv_fatal_exception;
   END;                                                                     --end insert_log


   PROCEDURE insert_log (
      arg_program_function      IN ZAYOMSS.conv_conversion_error_log.program_function%TYPE,
      arg_class_of_err          IN ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE,
      arg_err_loc_id                  --         arg_err_desc            ~ error description
                    IN             ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE,
      arg_src_data_key_lookup   IN ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE,
      arg_err_desc              IN ZAYOMSS.conv_conversion_error_log.error_description%TYPE,
      arg_resolution_required   IN ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE)
   IS
      --error description
      lv_err_desc              ZAYOMSS.conv_conversion_error_log.error_description%TYPE := NULL;
      lv_src_data_key_lookup   ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
   BEGIN
      IF arg_src_data_key_lookup IS NULL
      THEN
         lv_src_data_key_lookup := 'NO LOOKUP';
      ELSE
         lv_src_data_key_lookup := arg_src_data_key_lookup;
      END IF;

      INSERT INTO ZAYOMSS.conv_conversion_error_log (program_name,
                                                     program_function,
                                                     program_run_date,
                                                     class_of_error,
                                                     error_locator_id,
                                                     src_data_key_lookup,
                                                     error_description,
                                                     resolution_required)
           VALUES (gv_package_name,
                   arg_program_function,
                   TO_DATE (TO_CHAR (gv_run_dt), 'YYYYMMDD'),
                   arg_class_of_err,
                   arg_err_loc_id,
                   lv_src_data_key_lookup,
                   arg_err_desc,
                   arg_resolution_required);
   EXCEPTION
      WHEN OTHERS
      THEN
         --lv_err_desc:= SQLERRM;
         lv_err_desc :=
               'FATAL: unable to insert log for '
            || 'program_name='
            || gv_package_name
            || 'program_function='
            || arg_program_function
            || 'error_message='
            || arg_err_desc;
         RAISE gv_fatal_exception;
   END;                                                                     --end insert_log

   ------------------------------------------------------------------
   --
   -- Usage: Insert value into the  ZAYOMSS.conv_stat_summary_log table
   --
   -- Input: arg_legacy.conv_stat_summary_log ~ summary information
   --
   -- Output: NONE
   --
   -- Errors: Raises fatal exception if the entry cannot be inserted
   --
   ---------------------------------------------------------------------------

   PROCEDURE insert_conv_stat_summary_log (
      arg_conv_stat_summary_log   IN ZAYOMSS.conv_stat_summary_log%ROWTYPE)
   IS
      --error description
      lv_err_desc   ZAYOMSS.conv_conversion_error_log.error_description%TYPE := NULL;
   BEGIN
      INSERT INTO ZAYOMSS.conv_stat_summary_log (program_name,
                                                 program_function,
                                                 program_run_date,
                                                 program_start_time,
                                                 program_end_time,
                                                 source_record_cnt,
                                                 error_record_cnt,
                                                 target_record_cnt)
           VALUES (arg_conv_stat_summary_log.program_name,
                   arg_conv_stat_summary_log.program_function,
                   arg_conv_stat_summary_log.program_run_date,
                   arg_conv_stat_summary_log.program_start_time,
                   arg_conv_stat_summary_log.program_end_time,
                   arg_conv_stat_summary_log.source_record_cnt,
                   arg_conv_stat_summary_log.error_record_cnt,
                   arg_conv_stat_summary_log.target_record_cnt);
   EXCEPTION
      WHEN OTHERS
      THEN
         lv_err_desc :=
               'FATAL: unable to insert stat summary for '
            || 'program name='
            || gv_package_name
            || 'program function='
            || arg_conv_stat_summary_log.program_function
            || 'error='
            || SQLERRM;
         RAISE gv_fatal_exception;
   END;                                                   --end insert_conv_stat_summary_log

   ----------------------------------------------------------------------------
   --  Usage: To initialize the global variables
   --
   --  input: None
   --
   --  output: NONE
   --
   --  Error: If any errors wil be inserted into conv_conversion_error_log table
   -----------------------------------------------------------------------------

   PROCEDURE initialize_variables
   AS
      lv_procedure_name    ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                              := 'INITIALIZE_VARIABLES';
      lv_err_desc          ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      var_ca_id            NUMBER;
      lv_check_uom_exist   NUMBER;
      lv_uom_name          VARCHAR2 (30);
      e                    EXCEPTION;
   BEGIN
      BEGIN
         SELECT value_text
           INTO gv_lmuid
           FROM ZAYOMSS.conv_global_variable
          WHERE label_name = 'LMUID';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_desc := SQLERRM;
            insert_log (
               lv_procedure_name,
               'ERROR:No DATA FOUND',
               -21002,
               'Error getting global variable LMUID',
               lv_err_desc,
               'add entry for run_date in conv_global_variable then run the program again');
            RAISE e;
         WHEN OTHERS
         THEN
            lv_err_desc := SQLERRM;
            insert_log (
               lv_procedure_name,
               'ERROR:NO DATA FOUND',
               -21005,
               'Error getting global variable LMUID',
               lv_err_desc,
               'add entry for run_date in conv_global_variable THEN run the program again');
            RAISE e;
      END;

       BEGIN
         SELECT plan_id
           INTO gv_plan_id
           FROM asap.provisioning_plan
          WHERE plan_name = 'A2TASKPLAN' AND ROWNUM = 1;			
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_desc := SQLERRM;
            -- Run date has to be changed
            insert_log (lv_procedure_name,
                        'ERROR:NO DATA FOUND',
                        -21012,
                        'Required Provisioning Plan not found in target database',
                        lv_err_desc,
                        'Create missing Provisioning Plan in MSS');
            RAISE e;
      END;

      -- Fetching a value for global variable RUN_DATE
      BEGIN
         SELECT value_number
           INTO gv_run_dt
           FROM ZAYOMSS.conv_global_variable
          WHERE label_name = 'RUN_DATE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_desc := SQLERRM;
            gv_run_dt := TO_NUMBER (TO_CHAR (SYSDATE, 'YYYYMMDD'));
            -- Run date has to be changed
            insert_log (
               lv_procedure_name,
               'ERROR:NO DATA FOUND',
               -21003,
               'Error getting Global variable GV_RUN_DT',
               lv_err_desc,
               'add entry for run_date in conv_global_variable then run the program again');
            RAISE e;
      END;

      -- Fetching a value for global variable release number
      BEGIN
         SELECT '99999' value_number
           INTO gv_rel_num
           FROM ZAYOMSS.conv_global_variable
          WHERE label_name = 'CONV_REL_NUM';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_desc := SQLERRM;
            insert_log (
               lv_procedure_name,
               'ERROR:NO DATA FOUND',
               -21004,
               'Error getting the global variable gv_rel_num',
               lv_err_desc,
               'add entry for release number in conv_global_variable then run the program again');
            RAISE e;
      END;
   EXCEPTION
      WHEN e
      THEN
         RAISE gv_fatal_exception;
      WHEN OTHERS
      THEN
         lv_err_desc := lv_err_desc || SQLERRM;
         insert_log (lv_procedure_name,
                     'FATAL ERROR',
                     -29999,
                     NULL,
                     lv_err_desc,
                     'Need to check code');
         --COMMIT;
         RAISE gv_fatal_exception;
   END initialize_variables;

   --------------------------------------------------------------------------------------------------------------------
   --
   -- Usage: Initialize the summary log with NULL/0 values
   --
   -- Input: arg_conv_stat_summary_log ~ the summary record
   --
   -- Output: arg_conv_stat_summary_log
   --
   --
   -- Error: NONE
   --
   --
   -----------------------------------------------------------------------------------------------------------------------

   PROCEDURE init_conv_stat_summary_log (
      arg_conv_stat_summary_log   IN OUT ZAYOMSS.conv_stat_summary_log%ROWTYPE,
      arg_procedure_name          IN     VARCHAR2)
   IS
   BEGIN
      -- Procedure to initialize with initial statistics before the conversion program is run
      arg_conv_stat_summary_log.program_name := gv_package_name;
      arg_conv_stat_summary_log.program_function := arg_procedure_name;
      arg_conv_stat_summary_log.program_run_date := TO_DATE (gv_run_dt, 'YYYYMMDD');
      arg_conv_stat_summary_log.program_start_time := SYSDATE;
      arg_conv_stat_summary_log.program_end_time := SYSDATE;
      arg_conv_stat_summary_log.source_record_cnt := 0;
      arg_conv_stat_summary_log.error_record_cnt := 0;
      arg_conv_stat_summary_log.target_record_cnt := 0;
   END;                                                     --end init_conv_stat_summary_log
--------------------PROCEDURE TO LOAD EWO SR_RELATION------------------------------

   PROCEDURE load_ewo_sr_rel
   IS
      lv_serv_item_id             NUMBER (9);
      lv_count                    NUMBER (9) := 0;
      lv_procedure_name           ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                     := 'LOAD_EWO_SR_REL';
      rec_conv_stat_summary_log   ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      lv_err_desc                 ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err             ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator            ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup               ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd          ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      lv_sr_rel_seq               NUMBER (9);
      lv_document_number          NUMBER (9);
      lv_document_number_rel      NUMBER (9);
      lv_total_recs               NUMBER (9) := 0;
      lv_target_recs              NUMBER (9) := 0;
      lv_error_recs               NUMBER (9) := 0;
      lv_primary_key              VARCHAR2 (100);
      exit_record                 EXCEPTION;

      CURSOR cur_rel
      IS
         SELECT *
           FROM ZAYOMSS.STG_EWO_DETAIL
          WHERE gen_object_case = 'SR_RELATION';
   BEGIN
      initialize_variables;
      init_conv_stat_summary_log (rec_conv_stat_summary_log, lv_procedure_name);
      insert_conv_stat_summary_log (rec_conv_stat_summary_log);

      FOR rec_rel IN cur_rel
      LOOP
         BEGIN
            gv_lm_date := NVL (rec_rel.stg_last_modified_date, SYSDATE);
            gv_lmuid := NVL (rec_rel.stg_last_modified_userid, gv_lmuid);

            lv_total_recs := lv_total_recs + 1;

            lv_primary_key :=
                  'STG_EWO_ORDER_ID = '
               || rec_rel.stg_ewo_order_id;
               --|| ', SOURCE = '
               --|| rec_rel.gen_source
               --|| ', GEN_SEQ='
               --|| rec_rel.gen_seq;

            BEGIN
               SELECT mss_document_number
                 INTO lv_document_number
                 FROM ZAYOMSS.aref_so_ewo
                WHERE     stg_order_id = rec_rel.stg_ewo_order_id
                      --AND stg_source = rec_rel.gen_source
                      AND ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  lv_document_number := NULL;
                  lv_err_desc := 'EWO Order not found in MSS';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:No Data',                           -- arg_class_of_err
                              -22020,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create EWO Order in MSS'           -- arg_resolution_required
                                                       );
                  RAISE exit_record;
            END;

            BEGIN
               SELECT mss_document_number
                 INTO lv_document_number_rel
                 FROM ZAYOMSS.aref_so_ewo
                WHERE     stg_order_id = rec_rel.stg_ewo_order_parent_id
                      --AND stg_source = rec_rel.gen_source
                      AND ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  lv_document_number_rel := NULL;
                  lv_err_desc := 'EWO Related Order not found in MSS';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERORR:No Data',                           -- arg_class_of_err
                              -22022,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create Related EWO Order in MSS'   -- arg_resolution_required
                                                               );
                  RAISE exit_record;
            END;

            SELECT NVL (MAX (sr_relation_seq), 0) + 1
              INTO lv_sr_rel_seq
              FROM asap.sr_relationship
             WHERE     document_number = lv_document_number
                   AND document_number_related = lv_document_number_rel;

            INSERT INTO asap.sr_relationship (document_number,
                                              document_number_related,
                                              sr_relation_seq,
                                              sr_relation_type_cd,
                                              active_ind,
                                              last_modified_userid,
                                              last_modified_date,
                                              create_userid,
                                              create_date)
                 VALUES (lv_document_number,                              --DOCUMENT_NUMBER,
                         lv_document_number_rel,                  --DOCUMENT_NUMBER_RELATED,
                         lv_sr_rel_seq,                                   --SR_RELATION_SEQ,
                         rec_rel.stg_ewo_parent_rel_type_cd,          --SR_RELATION_TYPE_CD,
                         rec_rel.stg_ewo_parent_active_ind,                    --ACTIVE_IND,
                         gv_lmuid,                                   --LAST_MODIFIED_USERID,
                         gv_lm_date,                                   --LAST_MODIFIED_DATE,
                         gv_lmuid,                                          --CREATE_USERID,
                         gv_lm_date                                            --CREATE_DATE
                                   );

            lv_target_recs := lv_target_recs + 1;
         EXCEPTION
            WHEN exit_record
            THEN
               lv_error_recs := lv_error_recs + 1;
         END;

         --COMMIT;
      END LOOP;

      UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = lv_total_recs,
             error_record_cnt = lv_error_recs,
             target_record_cnt = lv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      --COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'FATAL ERROR',
                     -29999,
                     lv_primary_key,
                     lv_err_desc,
                     'Need to check code');

         gv_fatal := 'Y';
         gv_fail_ind := 'Y';
         --COMMIT;
   END;
--------------------------------
  /* PROCEDURE load_ewo_user_data
   IS
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                       := 'LOAD_EWO_USER_DATA';
      
	  v_budget_id                   asap.ewo_user_data.budget_id%TYPE := NULL;
      v_cut_type                    asap.ewo_user_data.cut_type%TYPE := NULL;
      v_ewo_input                   asap.ewo_user_data.ewo_input%TYPE := NULL;
      v_ewo_order_type              asap.ewo_user_data.ewo_order_type%TYPE := NULL;
      v_ewr                         asap.ewo_user_data.ewr%TYPE := NULL;
      v_fp_ticket                   asap.ewo_user_data.fp_ticket%TYPE := NULL;
      v_from_dslam                  asap.ewo_user_data.from_dslam%TYPE := NULL;
      v_install_mts_dt_ticket_num   asap.ewo_user_data.install_mts_dt_ticket_num%TYPE
                                       := NULL;
      v_installation_package        asap.ewo_user_data.installation_package%TYPE := NULL;
      v_jobtrac_project_id          asap.ewo_user_data.jobtrac_project_id%TYPE := NULL;
      v_maintenance                 asap.ewo_user_data.maintenance%TYPE := NULL;
      v_mop_mts_dt_ticket_num       asap.ewo_user_data.mop_mts_dt_ticket_num%TYPE := NULL;
      v_project_description         asap.ewo_user_data.project_description%TYPE := NULL;
      v_teo_                        asap.ewo_user_data.teo_%TYPE := NULL;
      v_to_dslam                    asap.ewo_user_data.to_dslam%TYPE := NULL;
      v_approval_code               asap.ewo_user_data.approval_code%TYPE := NULL;
      v_engineering_work_request    asap.ewo_user_data.engineering_work_request%TYPE
                                       := NULL;
      v_product_description         asap.ewo_user_data.product_description%TYPE := NULL;
      lv_primary_key                VARCHAR2 (100);
      lv_count                      NUMBER := 0;
      lv_err_desc                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err               ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator              ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup                 ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd            ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      error_record                  EXCEPTION;
      exit_order                    EXCEPTION;
      v_user_data_flag              VARCHAR2 (1) := 'N';
	  
	  
   BEGIN
      --      prev_val := grec_ewo_user.stg_EWO_order_id;

      v_user_data_flag := 'N';

      FOR get_rec_user IN get_user
      LOOP
         BEGIN
            v_user_data_flag := 'Y';
            grec_ewo_user := get_rec_user;
            gv_total_recs_ud := gv_total_recs_ud + 1;
            gv_lm_date := NVL (grec_ewo.stg_last_modified_date, SYSDATE);
            gv_lmuid := NVL (grec_ewo.stg_last_modified_userid, gv_lmuid);

            lv_primary_key :=
                  'STG_ORDER_ID = '
               || gv_order_id
               || ': Name = ';
               --|| grec_ewo_user.stg_ewo_value_name;

            --DBMS_OUTPUT.PUT_LINE ( grec_ewo_user.stg_ewo_value_name );
--Not in ZAYOM6
/*
            IF grec_ewo_user.stg_ewo_value_name = 'BUDGET_ID'
            THEN
               v_budget_id := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'CUT_TYPE'
            THEN
               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.user_data_category_values
                WHERE     user_data_category IN ('CUT TYPE')
                      AND display_value = grec_ewo_user.stg_ewo_value_value
                      AND user_data_category_value_id =
                             TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               v_cut_type := TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               IF lv_count = 0
               THEN
                  v_ewo_input := NULL;
                  lv_err_desc :=
                     'DISPLAY_VALUE not defined in target database- record not loaded';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:No Data',                           -- arg_class_of_err
                              -20023,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create Display Value in user_data_category_values in MSS' -- arg_resolution_required
                                                                                        );
                  RAISE error_record;
               END IF;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'EWO_INPUT'
            THEN
               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.user_data_category_values
                WHERE     user_data_category IN ('EWO_INPUT')
                      AND display_value = grec_ewo_user.stg_ewo_value_value
                      AND user_data_category_value_id =
                             TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               v_ewo_input := TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               IF lv_count = 0
               THEN
                  v_ewo_input := NULL;
                  lv_err_desc :=
                     'DISPLAY_VALUE not defined in target database- record not loaded';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:No Data',                           -- arg_class_of_err
                              -20024,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create Display Value in user_data_category_values in MSS' -- arg_resolution_required
                                                                                        );
                  RAISE error_record;
               END IF;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'EWO_ORDER_TYPE'
            THEN
               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.user_data_category_values
                WHERE     user_data_category IN ('EWO ORDER TYPES')
                      AND display_value = grec_ewo_user.stg_ewo_value_value
                      AND user_data_category_value_id =
                             TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);


               v_ewo_order_type := TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               IF lv_count = 0 OR grec_ewo_user.stg_ewo_value_instance_id IS NULL
               THEN
                  v_ewo_order_type := NULL;
                  lv_err_desc :=
                     'DISPLAY_VALUE not defined in target database- record not loaded';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:No Data',                           -- arg_class_of_err
                              -20026,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create Display Value in user_data_category_values in MSS' -- arg_resolution_required
                                                                                        );
                  RAISE exit_order;
               END IF;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'EWR'
            THEN
               v_ewr := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'FP_TICKET'
            THEN
               v_fp_ticket := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'FROM_DSLAM'
            THEN
               v_from_dslam := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'INSTALL_MTS_DT_TICKET_NUM'
            THEN
               v_install_mts_dt_ticket_num := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'INSTALLATION_PACKAGE'
            THEN
               v_installation_package := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'JOBTRAC_PROJECT_ID'
            THEN
               v_jobtrac_project_id := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'MAINTENANCE'
            THEN
               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.user_data_category_values
                WHERE     user_data_category IN ('MAINTENANCE')
                      AND display_value = grec_ewo_user.stg_ewo_value_value
                      AND user_data_category_value_id =
                             TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               v_maintenance := TO_NUMBER (grec_ewo_user.stg_ewo_value_instance_id);

               IF lv_count = 0
               THEN
                  v_maintenance := NULL;
                  lv_err_desc :=
                     'DISPLAY_VALUE not defined in target database- record not loaded';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:No Data',                           -- arg_class_of_err
                              -20028,                                      -- arg_err_loc_id
                              lv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Create Display Value in user_data_category_values in MSS' -- arg_resolution_required
                                                                                        );
                  RAISE error_record;
               END IF;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'MOP_MTS_DT_TICKET_NUM'
            THEN
               v_mop_mts_dt_ticket_num := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'PROJECT_DESCRIPTION'
            THEN
               v_project_description := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'TEO_'
            THEN
               v_teo_ := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'TO_DSLAM'
            THEN
               v_to_dslam := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'APPROVAL_CODE'
            THEN
               v_approval_code := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'ENGINEERING_WORK_REQUEST'
            THEN
               v_engineering_work_request := grec_ewo_user.stg_ewo_value_value;
            ELSIF grec_ewo_user.stg_ewo_value_name = 'PRODUCT_DESCRIPTION'
            THEN
               v_product_description := grec_ewo_user.stg_ewo_value_value;
            END IF;

            gv_target_recs_ud := gv_target_recs_ud + 1;
         EXCEPTION
            WHEN error_record
            THEN
               gv_error_recs_ud := gv_error_recs_ud + 1;
         END;
      END LOOP;

      IF v_user_data_flag = 'Y'
      THEN
         --IF v_ewo_order_type IS NULL
		 IF gv_document_number IS NULL
		               		 
         THEN
            lv_err_desc :=
               'EWO ORDER TYPE is required for EWO USER_DATA and is not found in ZAYOMSS- record not loaded';
            insert_log (lv_procedure_name,                           -- arg_program_function
                        'ERROR:No Data',                                 -- arg_class_of_err
                        -20027,                                            -- arg_err_loc_id
                        'STG_ORDER_ID = ' || gv_order_id,         -- arg_src_data_key_lookup
                        lv_err_desc,                                         -- arg_err_desc
                        'Load valid value for EWO Order Type in ZAYOMSS' -- arg_resolution_required
                                                                        );
            RAISE exit_order;
         END IF;

         INSERT INTO asap.ewo_user_data (document_number,
                                         last_modified_date,
                                         last_modified_userid
                                         --budget_id,
                                         --cut_type,
                                         --ewo_input,
                                         --ewo_order_type,
                                         --ewr,
                                         --fp_ticket,
                                         --from_dslam,
                                         --install_mts_dt_ticket_num,
                                         --installation_package,
                                         --jobtrac_project_id,
                                         --maintenance,
                                         --mop_mts_dt_ticket_num,
                                         --project_description,
                                         --teo_,
                                         --to_dslam,
                                         --approval_code,
                                         --engineering_work_request,
                                         --product_description
										 )
              VALUES (gv_document_number,                                 --DOCUMENT_NUMBER,
                      gv_lm_date,                                      --LAST_MODIFIED_DATE,
                      gv_lmuid                                     --LAST_MODIFIED_USERID,
                      --v_budget_id,                                              --BUDGET_ID,
                      --v_cut_type,                                                --CUT_TYPE,
                      --v_ewo_input,                                              --EWO_INPUT,
                      --v_ewo_order_type,                                    --EWO_ORDER_TYPE,
                      --v_ewr,                                                          --EWR,
                      --v_fp_ticket,                                              --FP_TICKET,
                      --v_from_dslam,                                            --FROM_DSLAM,
                      --v_install_mts_dt_ticket_num,              --INSTALL_MTS_DT_TICKET_NUM,
                      --v_installation_package,                        --INSTALLATION_PACKAGE,
                      --v_jobtrac_project_id,                            --JOBTRAC_PROJECT_ID,
                      --v_maintenance,                                          --MAINTENANCE,
                      --v_mop_mts_dt_ticket_num,                      --MOP_MTS_DT_TICKET_NUM,
                      --v_project_description,                          --PROJECT_DESCRIPTION,
                      --v_teo_,                                                        --TEO_,
                      --v_to_dslam,                                                --TO_DSLAM,
                      --v_approval_code,                                      --APPROVAL_CODE,
                      --v_engineering_work_request,                --ENGINEERING_WORK_REQUEST,
                      --v_product_description                            --PRODUCT_DESCRIPTION
                                           );
      --END IF;

      --COMMIT;
   EXCEPTION
      WHEN exit_order
      THEN
         gv_error_recs_ud := gv_error_recs_ud + 1;
      WHEN OTHERS
      THEN
         ROLLBACK;
         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'FATAL ERROR',
                     -29999,
                     lv_primary_key,
                     lv_err_desc,
                     'Need to check code');

         gv_fatal := 'Y';
         gv_fail_ind := 'Y';
         --COMMIT;
   END;*/
---------------PROCEDURE TO LOAD EWO EQUIPMENT-------------------
   PROCEDURE load_ewo_equipment (arg_equipment_id NUMBER DEFAULT NULL)
   IS
      lv_serv_item_id             NUMBER (9);
      lv_count                    NUMBER (9) := 0;
      lv_count1                   NUMBER (9) := 0;
      lv_procedure_name           ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                     := 'LOAD_EWO_EQUIPMENT';
      rec_conv_stat_summary_log   ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      exit_record                 EXCEPTION;
      lv_err_desc                 ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err             ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator            ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup               ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd          ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      lv_serv_loc                 VARCHAR (15);
      lv_srl_cnt                  NUMBER (9);
      lv_actl_exists              NUMBER := 0;
      lv_location_id              NUMBER (9);
      lv_old_serv_item_id         NUMBER (9);
      lv_equipment_id             NUMBER (9);
   BEGIN
      lv_equipment_id := arg_equipment_id;

 --CHECK SEQUENCE FOR SERV_ITEM_ID
      SELECT asap.sq_serv_item.NEXTVAL
        INTO lv_serv_item_id
        FROM DUAL;
--INSERT SEQUENCE FOR SERV_ITEM_ID INTO SERV_ITEM TABLE
		INSERT INTO asap.serv_item 
								(serv_item_id,
                                  status,
                                  from_effective_date,
                                  to_eff_dt,
                                  qty,
                                  telecom_srvc_priority,
                                  item_alias,
                                  trunk_seg,
                                  spec_grp_id,
                                  circuit_design_id,
                                  trunk_group_design_id,
                                  last_modified_date,
                                  last_modified_userid,
                                  serv_item_type_cd,
                                  serv_item_desc,
                                  src_verified_ind,
                                  create_userid,
                                  create_date,
                                  item_alias_suf,
                                  disc_reason_cd,
                                  term_nbr,
                                  active_ind,
                                  cust_acct_id,
                                  int_ext_cd,
                                  ownership_cd,
                                  assignment_control_cd,
                                  cur_trbl_ind,
                                  prev_circuit_design_id,
                                  donor_cust_acct_id,
                                  prev_trunk_design_id,
                                  id_xref,
                                  ui_location_code)
           VALUES (lv_serv_item_id,                                         -- serv_item_id,
                   NVL (grec_ewo_detail.stg_si_status, '6'),                      -- status,
                   grec_ewo_detail.stg_si_from_eff_dt,               -- from_effective_date,
                   grec_ewo_detail.stg_si_to_eff_dt,                           -- to_eff_dt,
                   NULL,                                                             -- qty,
                   NULL,                                           -- telecom_srvc_priority,
                   NULL,                                                      -- item_alias,
                   NULL,                                                       -- trunk_seg,
                   NULL,                                                     -- spec_grp_id,
                   NULL,                                               -- circuit_design_id,
                   NULL,                                           -- trunk_group_design_id,
                   gv_lm_date,                                        -- last_modified_date,
                   gv_lmuid,                                        -- last_modified_userid,
                   'EQUIPMENT',                                        -- serv_item_type_cd,
                   grec_ewo_detail.stg_serv_item_desc,                    -- serv_item_desc,
                   'Y',                                                 -- src_verified_ind,
                   gv_lmuid,                                               -- create_userid,
                   gv_lm_date,                                               -- create_date,
                   NULL,                                                  -- item_alias_suf,
                   NULL,                                                  -- disc_reason_cd,
                   NULL,                                                        -- term_nbr,
                   'Y',                                                       -- active_ind,
                   NULL,                                                    -- cust_acct_id,
                   'I',                                                       -- int_ext_cd,
                   'COMP',                                                  -- ownership_cd,
                   NULL,                                           -- assignment_control_cd,
                   'N',                                                     -- cur_trbl_ind,
                   NULL,                                          -- prev_circuit_design_id,
                   NULL,                                              -- donor_cust_acct_id,
                   NULL,                                            -- prev_trunk_design_id,
                   NULL,                                                         -- id_xref,
                   NULL                                                  -- ui_location_code
                       );

--INSERT SEQUENCE FOR SERV_ITEM_ID INTO SERV_REQ_SI TABLE
      INSERT INTO asap.serv_req_si (document_number,
                                    serv_item_id,
                                    item_alias,
                                    spec_grp_id,
                                    activity_cd,
                                    qty,
                                    status,
                                    trunk_seg,
                                    additional_info,
                                    last_modified_date,
                                    last_modified_userid,
                                    reported_issue_nbr,
                                    item_alias_suf,
                                    reference_number,
                                    srsi_group_id,
                                    prior_status,
                                    activity_eu_cd)
           VALUES (gv_document_number,                                   -- document_number,
                   lv_serv_item_id,                                         -- serv_item_id,
                   NULL,                                                      -- item_alias,
                   NULL,                                                     -- spec_grp_id,
                   grec_ewo_detail.stg_si_activity_ind,                      -- activity_cd,
                   NULL,                                                             -- qty,
                   NVL (grec_ewo_detail.stg_si_status, '6'),                      -- status,
                   NULL,                                                       -- trunk_seg,
                   NULL,                                                 -- additional_info,
                   gv_lm_date,                                        -- last_modified_date,
                   gv_lmuid,                                        -- last_modified_userid,
                   NULL,                                              -- reported_issue_nbr,
                   NULL,                                                  -- item_alias_suf,
                   NULL,                                                -- reference_number,
                   NULL,                                                   -- srsi_group_id,
                   NULL,                                                    -- prior_status,
                   NULL                                                    -- activity_eu_cd
                       );

      --      INSERT INTO ZAYOMSS.aref_ewo_ckt_si
      --           VALUES (gv_source,                                                      --SOURCE,
      --                   gv_order_id,                                          --STG_EWO_ORDER_ID,
      --                   grec_ewo_detail.stg_leg_exch_carrier_ckt_id,                 --STG_ECCKT,
      --                   NULL,                                            --MSS_CIRCUIT_DESIGN_ID,
      --                   NULL,                                         --mss_trunk_group_design_id
      --                   lv_serv_item_id,                                      --MSS_SERV_ITEM_ID,
      --                   gv_document_number,                                --MSS_DOCUMENT_NUMBER,
      --                   'EQUIPMENT'                                           --SERV_ITEM_TYPE_CD
      --                              );

      IF lv_equipment_id IS NOT NULL
      THEN
         INSERT INTO asap.si_equipment (serv_item_id,
                                        equipment_id,
                                        last_modified_date,
                                        last_modified_userid)
              VALUES (lv_serv_item_id,                                       --SERV_ITEM_ID,
                      lv_equipment_id,                                       --EQUIPMENT_ID,
                      gv_lm_date,                                      --LAST_MODIFIED_DATE,
                      gv_lmuid                                        --LAST_MODIFIED_USERID
                              );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'ERROR',
                     -29999,
                     'STG_EWO_ORDER_ID = ' || gv_order_id,-- || ', SOURCE = ' || gv_source,
                     lv_err_desc,
                     'Need to check code');

         gv_fail_ind := 'Y';
         --COMMIT;
   END;
   
-------------PROCEDURE TO LOAD EWO RELATED CIRCUIT------------
   PROCEDURE load_ewo_circuit (arg_circuit_design_id NUMBER DEFAULT NULL)
   IS
      lv_serv_item_id                 NUMBER (9);
      lv_count                        NUMBER (9) := 0;
      lv_count1                       NUMBER (9) := 0;
      lv_procedure_name               ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                         := 'LOAD_EWO_CIRCUIT';
      rec_conv_stat_summary_log       ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      exit_record                     EXCEPTION;
      lv_err_desc                     ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err                 ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator                ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup                   ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd              ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      lv_serv_loc                     VARCHAR (15);
      lv_srl_cnt                      NUMBER (9);
      lv_order_existslv_actl_exists   NUMBER := 0;
      lv_actl_exists                  NUMBER := 0;
      lv_order_exists                 NUMBER := 0;
      lv_location_id                  NUMBER (9);
      lv_old_serv_item_id             NUMBER (9);
      lv_circuit_design_id            NUMBER (9);
      lv_activity_cd                  asap.serv_req_si.activity_cd%TYPE;
   BEGIN
      lv_circuit_design_id := arg_circuit_design_id;

      BEGIN
         SELECT serv_item_id
           INTO lv_old_serv_item_id
           FROM asap.serv_item
          WHERE circuit_design_id = lv_circuit_design_id;

         BEGIN
            SELECT 1
              INTO lv_order_exists
              FROM asap.serv_item si, asap.serv_req_si srsi, asap.serv_req sr
             WHERE     srsi.document_number = sr.document_number
                   AND srsi.serv_item_id = si.serv_item_id
                   AND si.circuit_design_id = lv_circuit_design_id
                   AND ROWNUM = 1;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_order_exists := 0;
         END;
      EXCEPTION
         WHEN OTHERS
         THEN
            lv_old_serv_item_id := NULL;
      END;

      BEGIN
         IF lv_old_serv_item_id IS NOT NULL AND lv_order_exists = 0
         THEN
			EXECUTE IMMEDIATE 'DELETE FROM asap.serv_req_si WHERE serv_item_id = :1' USING lv_old_serv_item_id;
			EXECUTE IMMEDIATE 'DELETE FROM asap.serv_item WHERE serv_item_id = :1' USING lv_old_serv_item_id;
            
			--DELETE FROM asap.serv_req_si WHERE serv_item_id = lv_old_serv_item_id;
	  
            --DELETE FROM asap.serv_item WHERE serv_item_id = lv_old_serv_item_id;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            lv_circuit_design_id := NULL;
            lv_err_desc := 'Circuit already found associated to another order in MSS';
            insert_log (
               lv_procedure_name,                                    -- arg_program_function
               'WARNING:Duplicate Data',                                 -- arg_class_of_err
               -22038,                                                     -- arg_err_loc_id
                  'STG_EWO_ORDER_ID ='
               || grec_ewo.stg_ewo_order_id,                                 -- arg_src_data_key_lookup
               lv_err_desc,                                                  -- arg_err_desc
               'TO associate circuit on a new order remove any prior association from an Order in MSS' -- arg_resolution_required
                                                                                                      );
      END;
	  
      IF grec_ewo_detail.stg_si_activity_ind IS NULL
      THEN
         SELECT DECODE (grec_ewo_detail.stg_leg_ckt_status, '8', 'D', 'N')
           INTO lv_activity_cd
           FROM DUAL;
      ELSE
         lv_activity_cd := grec_ewo_detail.stg_si_activity_ind;
      END IF;
	  
      IF lv_old_serv_item_id IS NOT NULL AND lv_order_exists > 0
      THEN
         lv_serv_item_id := lv_old_serv_item_id;
	  
         BEGIN
            UPDATE asap.serv_item
               SET status =
                      NVL (
                         NVL (grec_ewo_detail.stg_si_status,
                              grec_ewo_detail.stg_leg_ckt_status),
                         '6'),
                   from_effective_date = grec_ewo_detail.stg_si_from_eff_dt,
                   to_eff_dt = grec_ewo_detail.stg_si_to_eff_dt,
                   qty = NULL,
                   telecom_srvc_priority = NULL,
                   item_alias = NULL,
                   trunk_seg = NULL,
                   spec_grp_id = NULL,
                   circuit_design_id = lv_circuit_design_id,
                   trunk_group_design_id = NULL,
                   last_modified_date = gv_lm_date,
                   last_modified_userid = gv_lmuid,
                   serv_item_type_cd = 'CIRCUIT',
                   serv_item_desc = grec_ewo_detail.stg_serv_item_desc,
                   src_verified_ind = 'Y',
                   create_userid = gv_lmuid,
                   create_date = gv_lm_date,
                   item_alias_suf = NULL,
                   disc_reason_cd = NULL,
                   term_nbr = NULL,
                   active_ind = 'Y',
                   cust_acct_id = NULL,
                   int_ext_cd = 'I',
                   ownership_cd = 'COMP',
                   assignment_control_cd = NULL,
                   cur_trbl_ind = 'N',
                   prev_circuit_design_id = NULL,
                   donor_cust_acct_id = NULL,
                   prev_trunk_design_id = NULL,
                   id_xref = NULL,
                   ui_location_code = NULL
             WHERE serv_item_id = lv_serv_item_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               NULL;
         END;
		 
		------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.serv_item table
        --
        -- Target table : asap.serv_item
        -- function_name : LOAD_EWO_CIRCUIT
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------
      ELSE
         SELECT asap.sq_serv_item.NEXTVAL
           INTO lv_serv_item_id
           FROM DUAL;

         BEGIN
            INSERT INTO asap.serv_item 
										(serv_item_id,
                                        status,
                                        from_effective_date,
                                        to_eff_dt,
                                        qty,
                                        telecom_srvc_priority,
                                        item_alias,
                                        trunk_seg,
                                        spec_grp_id,
                                        circuit_design_id,
                                        trunk_group_design_id,
                                        last_modified_date,
                                        last_modified_userid,
                                        serv_item_type_cd,
                                        serv_item_desc,
                                        src_verified_ind,
                                        create_userid,
                                        create_date,
                                        item_alias_suf,
                                        disc_reason_cd,
                                        term_nbr,
                                        active_ind,
                                        cust_acct_id,
                                        int_ext_cd,
                                        ownership_cd,
                                        assignment_control_cd,
                                        cur_trbl_ind,
                                        prev_circuit_design_id,
                                        donor_cust_acct_id,
                                        prev_trunk_design_id,
                                        id_xref,
                                        ui_location_code)
                    VALUES (
                              lv_serv_item_id,                              -- serv_item_id,
                              NVL (
                                 NVL (grec_ewo_detail.stg_si_status,
                                      grec_ewo_detail.stg_leg_ckt_status),
                                 '6'),                                            -- status,
                              grec_ewo_detail.stg_si_from_eff_dt,    -- from_effective_date,
                              grec_ewo_detail.stg_si_to_eff_dt,                -- to_eff_dt,
                              NULL,                                                  -- qty,
                              NULL,                                -- telecom_srvc_priority,
                              NULL,                                           -- item_alias,
                              NULL,                                            -- trunk_seg,
                              NULL,                                          -- spec_grp_id,
                              lv_circuit_design_id,                    -- circuit_design_id,
                              NULL,                                -- trunk_group_design_id,
                              gv_lm_date,                             -- last_modified_date,
                              gv_lmuid,                             -- last_modified_userid,
                              'CIRCUIT',                               -- serv_item_type_cd,
                              grec_ewo_detail.stg_serv_item_desc,         -- serv_item_desc,
                              'Y',                                      -- src_verified_ind,
                              gv_lmuid,                                    -- create_userid,
                              gv_lm_date,                                    -- create_date,
                              NULL,                                       -- item_alias_suf,
                              NULL,                                       -- disc_reason_cd,
                              NULL,                                             -- term_nbr,
                              'Y',                                            -- active_ind,
                              NULL,                                         -- cust_acct_id,
                              'I',                                            -- int_ext_cd,
                              'COMP',                                       -- ownership_cd,
                              NULL,                                -- assignment_control_cd,
                              'N',                                          -- cur_trbl_ind,
                              NULL,                               -- prev_circuit_design_id,
                              NULL,                                   -- donor_cust_acct_id,
                              NULL,                                 -- prev_trunk_design_id,
                              NULL,                                              -- id_xref,
                              NULL                                       -- ui_location_code
                                  );
         EXCEPTION
            WHEN OTHERS
            THEN
               --DBMS_LOCK.sleep (10);

               BEGIN
                  SELECT serv_item_id
                    INTO lv_serv_item_id
                    FROM asap.serv_item
                   WHERE circuit_design_id = lv_circuit_design_id AND ROWNUM = 1;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     lv_serv_item_id := NULL;
               END;

               IF lv_serv_item_id IS NULL
               THEN
                  SELECT asap.sq_serv_item.NEXTVAL
                    INTO lv_serv_item_id
                    FROM DUAL;

                  INSERT INTO asap.serv_item 
											(serv_item_id,
                                              status,
                                              from_effective_date,
                                              to_eff_dt,
                                              qty,
                                              telecom_srvc_priority,
                                              item_alias,
                                              trunk_seg,
                                              spec_grp_id,
                                              circuit_design_id,
                                              trunk_group_design_id,
                                              last_modified_date,
                                              last_modified_userid,
                                              serv_item_type_cd,
                                              serv_item_desc,
                                              src_verified_ind,
                                              create_userid,
                                              create_date,
                                              item_alias_suf,
                                              disc_reason_cd,
                                              term_nbr,
                                              active_ind,
                                              cust_acct_id,
                                              int_ext_cd,
                                              ownership_cd,
                                              assignment_control_cd,
                                              cur_trbl_ind,
                                              prev_circuit_design_id,
                                              donor_cust_acct_id,
                                              prev_trunk_design_id,
                                              id_xref,
                                              ui_location_code)
                          VALUES (
                                    lv_serv_item_id,                        -- serv_item_id,
                                    NVL (
                                       NVL (grec_ewo_detail.stg_si_status,
                                            grec_ewo_detail.stg_leg_ckt_status),
                                       '6'),                                      -- status,
                                    grec_ewo_detail.stg_si_from_eff_dt, -- from_effective_date,
                                    grec_ewo_detail.stg_si_to_eff_dt,          -- to_eff_dt,
                                    NULL,                                            -- qty,
                                    NULL,                          -- telecom_srvc_priority,
                                    NULL,                                     -- item_alias,
                                    NULL,                                      -- trunk_seg,
                                    NULL,                                    -- spec_grp_id,
                                    lv_circuit_design_id,              -- circuit_design_id,
                                    NULL,                          -- trunk_group_design_id,
                                    gv_lm_date,                       -- last_modified_date,
                                    gv_lmuid,                       -- last_modified_userid,
                                    'CIRCUIT',                         -- serv_item_type_cd,
                                    grec_ewo_detail.stg_serv_item_desc,   -- serv_item_desc,
                                    'Y',                                -- src_verified_ind,
                                    gv_lmuid,                              -- create_userid,
                                    gv_lm_date,                              -- create_date,
                                    NULL,                                 -- item_alias_suf,
                                    NULL,                                 -- disc_reason_cd,
                                    NULL,                                       -- term_nbr,
                                    'Y',                                      -- active_ind,
                                    NULL,                                   -- cust_acct_id,
                                    'I',                                      -- int_ext_cd,
                                    'COMP',                                 -- ownership_cd,
                                    NULL,                          -- assignment_control_cd,
                                    'N',                                    -- cur_trbl_ind,
                                    NULL,                         -- prev_circuit_design_id,
                                    NULL,                             -- donor_cust_acct_id,
                                    NULL,                           -- prev_trunk_design_id,
                                    NULL,                                        -- id_xref,
                                    NULL                                 -- ui_location_code
                                        );
               END IF;
         END;
      END IF;
	  
	    ---------------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.serv_req_si table
        --
        -- Target table : asap.serv_req_si
        -- function_name : LOAD_EWO_CIRCUIT
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------

      INSERT INTO asap.serv_req_si (document_number,
                                    serv_item_id,
                                    item_alias,
                                    spec_grp_id,
                                    activity_cd,
                                    qty,
                                    status,
                                    trunk_seg,
                                    additional_info,
                                    last_modified_date,
                                    last_modified_userid,
                                    reported_issue_nbr,
                                    item_alias_suf,
                                    reference_number,
                                    srsi_group_id,
                                    prior_status,
                                    activity_eu_cd)
              VALUES (
                        gv_document_number,                              -- document_number,
                        lv_serv_item_id,                                    -- serv_item_id,
                        NULL,                                                 -- item_alias,
                        NULL,                                                -- spec_grp_id,
                        lv_activity_cd,                                      -- activity_cd,
                        NULL,                                                        -- qty,
                        NVL (
                           NVL (grec_ewo_detail.stg_si_status,
                                grec_ewo_detail.stg_leg_ckt_status),
                           '6'),                                                  -- status,
                        NULL,                                                  -- trunk_seg,
                        NULL,                                            -- additional_info,
                        gv_lm_date,                                   -- last_modified_date,
                        gv_lmuid,                                   -- last_modified_userid,
                        NULL,                                         -- reported_issue_nbr,
                        NULL,                                             -- item_alias_suf,
                        NULL,                                           -- reference_number,
                        NULL,                                              -- srsi_group_id,
                        NULL,                                               -- prior_status,
                        NULL                                               -- activity_eu_cd
                            );


		---------------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.service_request_circuit table
        --
        -- Target table : asap.service_request_circuit
        -- function_name : LOAD_EWO_CIRCUIT
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------

      IF lv_circuit_design_id IS NOT NULL
      THEN
         INSERT INTO asap.service_request_circuit (document_number,
                                                   circuit_design_id,
                                                   facility_assignment_indicator,
                                                   cabs_extract_date,
                                                   completion_date,
                                                   secondary_location,
                                                   asr_form_type,
                                                   cabs_extract_ind,
                                                   last_modified_userid,
                                                   last_modified_date,
                                                   foc_design_id,
                                                   document_number_2,
                                                   msl_reference_number,
                                                   document_number_3,
                                                   document_number_4,
                                                   document_number_5,
                                                   reference_number,
                                                   from_effective_date,
                                                   to_effective_date,
                                                   order_number,
                                                   facility_order_number,
                                                   complete_with_related_order_nm,
                                                   circuit_quantity,
                                                   routing_indicator,
                                                   machine_interface_code,
                                                   discrete_telephone_number,
                                                   foc_reference_number,
                                                   admin_ckr_ind,
                                                   msl_bridge_ind,
                                                   ar_reference_number,
                                                   ar_document_number,
                                                   req_plan_id,
                                                   circuit_status,
                                                   circuit_activity_ind,
                                                   document_number_vc,
                                                   virtual_conn_nbr,
                                                   circuit_design_id_tiedown,
                                                   channel_to,
                                                   channel_from,
                                                   misc_1,
                                                   misc_2,
                                                   misc_3,
                                                   misc_4,
                                                   misc_5,
                                                   misc_6)
                 VALUES (
                           gv_document_number,                           -- document_number,
                           lv_circuit_design_id,                       -- circuit_design_id,
                           'CFA',                          -- facility_assignment_indicator,
                           NULL,                                       -- cabs_extract_date,
                           NVL (grec_ewo_detail.stg_si_from_eff_dt,
                                grec_ewo.stg_desired_due_date),          -- completion_date,
                           NULL,                                      -- secondary_location,
                           'FAC',                                          -- asr_form_type,
                           'Y',                                         -- cabs_extract_ind,
                           gv_lmuid,                                -- last_modified_userid,
                           gv_lm_date,                                -- last_modified_date,
                           NULL,                                           -- foc_design_id,
                           NULL,                                       -- document_number_2,
                           NULL,                                    -- msl_reference_number,
                           NULL,                                       -- document_number_3,
                           NULL,                                       -- document_number_4,
                           NULL,                                       -- document_number_5,
                           NULL,                                        -- reference_number,
                           NVL (grec_ewo_detail.stg_si_from_eff_dt,
                                grec_ewo.stg_desired_due_date),      -- from_effective_date,
                           grec_ewo_detail.stg_si_to_eff_dt,           -- to_effective_date,
                           CASE
                              WHEN LENGTH (grec_ewo.stg_order_number) > 17 THEN NULL
                              ELSE grec_ewo.stg_order_number
                           END,                                             -- order_number,
                           NULL,                                   -- facility_order_number,
                           NULL,                          -- complete_with_related_order_nm,
                           NULL,                                        -- circuit_quantity,
                           NULL,                                       -- routing_indicator,
                           NULL,                                  -- machine_interface_code,
                           NULL,                               -- discrete_telephone_number,
                           NULL,                                    -- foc_reference_number,
                           NULL,                                           -- admin_ckr_ind,
                           NULL,                                          -- msl_bridge_ind,
                           NULL,                                     -- ar_reference_number,
                           NULL,                                      -- ar_document_number,
                           NULL,                                             -- req_plan_id,
                           grec_ewo_detail.stg_leg_ckt_status,            -- circuit_status,
                           grec_ewo_detail.stg_circuit_activity_ind, -- circuit_activity_ind,
                           NULL,                                      -- document_number_vc,
                           NULL,                                        -- virtual_conn_nbr,
                           NULL,                               -- circuit_design_id_tiedown,
                           NULL,                                              -- channel_to,
                           NULL,                                            -- channel_from,
                           NULL,                                                  -- misc_1,
                           NULL,                                                  -- misc_2,
                           NULL,                                                  -- misc_3,
                           NULL,                                                  -- misc_4,
                           NULL,                                                  -- misc_5,
                           NULL                                                    -- misc_6
                               );


         UPDATE asap.port_address
            SET document_number = gv_document_number
          WHERE circuit_design_id = lv_circuit_design_id;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         lv_err_desc := SQLERRM;

         IF lv_err_desc LIKE '%FK_INSTALLEDSI_CIRCUIT%'
         THEN
            insert_log (lv_procedure_name,
                        'ERROR',
                        -29997,
                        'STG_EWO_ORDER_ID = ' || gv_order_id,-- || ', SOURCE = ' || gv_source,
                        'Deadlock Circuit. Unable to associate circuit on the order',
                        'Circuit needs to to be added manaully');

            gv_fail_ind := 'Y';
         ELSE
            insert_log (lv_procedure_name,
                        'ERROR',
                        -29999,
                        'STG_EWO_ORDER_ID = ' || gv_order_id,-- || ', SOURCE = ' || gv_source,
                        REPLACE (lv_err_desc, 'ORA-', NULL),
                        'Circuit needs to to be added manaully');

            gv_fail_ind := 'Y';
         END IF;

         --COMMIT;
   END;
----------PROCEDURE TO LOAD EWO DETAIL--------
   PROCEDURE load_ewo_detail
   IS
      rec_conv_stat_summary_log   ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      lv_procedure_name           ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                     := 'LOAD_EWO_DETAIL';
      lv_proc_name                ZAYOMSS.conv_conversion_error_log.program_function%TYPE;
      lv_insert                   NUMBER (9) := 0;
      lv_count                    NUMBER (9) := 0;
      lv_count1                   NUMBER (9) := 0;
      lv_count2                   NUMBER (9) := 0;
      lv_count3                   NUMBER (9) := 0;
      lv_commit                   NUMBER (9) := 0;
      lv_total_recs               NUMBER (9) := 0;
      lv_target_recs              NUMBER (9) := 0;
      lv_error_recs               NUMBER (9) := 0;
      lv_location_id              NUMBER (9);
      lv_circuit_design_id        NUMBER (9);
      lv_trunk_group_design_id    NUMBER (9);
      lv_serv_item_id             NUMBER (9);
      lv_seq_num                  NUMBER (9);
      lv_equipment_id             NUMBER (9);
      lv_notes_seq                NUMBER (9);
      lv_notes_id                 NUMBER (9);
      lv_ms_attachment_link_id    NUMBER (9);
      exit_record                 EXCEPTION;
      stop_load                   EXCEPTION;

      lv_err_desc                 ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err             ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator            ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup               ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd          ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
   BEGIN
      gv_ckt_seq := 0;

      FOR get_rec_ewo_detail IN get_ewo_stg_detail
      LOOP
         BEGIN
            gv_total_recs_dtl := gv_total_recs_dtl + 1;
            grec_ewo_detail := get_rec_ewo_detail;
            gv_primary_key :=
                  'STG_EWO_ORDER_ID = '
               || gv_order_id
               || ','
               || grec_ewo_detail.gen_object_case;

            gv_lm_date := NVL (grec_ewo_detail.stg_last_modified_date, SYSDATE);
            gv_lmuid := NVL (grec_ewo_detail.stg_last_modified_userid, gv_lmuid);

            lv_insert := 0;

            IF     (   grec_ewo_detail.gen_object_case IN ('EQUIPMENT') 
				OR (    grec_ewo_detail.gen_object_case IN ('CIRCUIT')))
                       -- AND grec_ewo_detail.stg_trunk_group_ecckt IS NOT NULL))
               AND grec_ewo_detail.stg_si_activity_ind IS NULL
            THEN
               lv_err_desc :=
                  'STG_SI_ACTIVITY_IND is NULL, it is required to create serv item for CIRCUIT and EQUIPMENT objects - record not loaded';
               insert_log (
                  lv_procedure_name,                                 -- arg_program_function
                  'ERROR:No Data',                                       -- arg_class_of_err
                  -22040,                                                  -- arg_err_loc_id
                  gv_primary_key || ':' || grec_ewo_detail.stg_leg_exch_carrier_ckt_id, -- arg_src_data_key_lookup
                  lv_err_desc,                                               -- arg_err_desc
                  'Stage valid data in STG_SI_ACTIVITY_IND'       -- arg_resolution_required
                                                           );
               RAISE exit_record;
            END IF;

           IF grec_ewo_detail.gen_object_case = 'CIRCUIT'
            THEN
               lv_circuit_design_id := NULL;

               BEGIN
                  SELECT mss_circuit_design_id
                    INTO lv_circuit_design_id
                    FROM ZAYOMSS.aref_circuit
                   WHERE     legacy_circuit_id =
                                TO_CHAR (grec_ewo_detail.stg_leg_circuit_design_id);
                        
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     lv_circuit_design_id := NULL;

                     lv_err_desc :=
                        'Circuit not found in target database- record not loaded';
                     insert_log (
                        lv_procedure_name,                           -- arg_program_function
                        'ERROR:No Data',                                 -- arg_class_of_err
                        -22046,                                            -- arg_err_loc_id
                           gv_primary_key
                        || ':'
                        || grec_ewo_detail.stg_leg_exch_carrier_ckt_id, -- arg_src_data_key_lookup
                        lv_err_desc,                                         -- arg_err_desc
                        'Create circuit in MSS'                   -- arg_resolution_required
                                               );
                     RAISE exit_record;
                  
				  WHEN TOO_MANY_ROWS
                  THEN
                     lv_circuit_design_id := NULL;
                     lv_err_desc :=
                        'Duplicate circuit found in target database- record not loaded';
                     insert_log (
                        lv_procedure_name,                           -- arg_program_function
                        'ERROR:Duplicate Data',                          -- arg_class_of_err
                        -22048,                                            -- arg_err_loc_id
                           gv_primary_key
                        || ':'
                        || grec_ewo_detail.stg_leg_exch_carrier_ckt_id, -- arg_src_data_key_lookup
                        lv_err_desc,                                         -- arg_err_desc
                        'Remove duplicate data from MSS'          -- arg_resolution_required
                                                        );
                     RAISE exit_record;
               END;

               IF lv_circuit_design_id IS NOT NULL
               THEN
                  gv_ckt_seq := gv_ckt_seq + 1;
                  load_ewo_circuit (lv_circuit_design_id);
               END IF;

               IF gv_fatal = 'Y'
               THEN
                  RAISE stop_load;
               END IF;

               IF gv_fail_ind = 'Y'
               THEN
                  RAISE exit_record;
               END IF;
            ELSIF grec_ewo_detail.gen_object_case = 'EQUIPMENT'
            THEN
               lv_equipment_id := NULL;

               BEGIN
                      SELECT mss_equipment_id
                       INTO lv_equipment_id
                       FROM ZAYOMSS.aref_mig_equipment
                      WHERE     pnd_equipment_id = grec_ewo_detail.stg_leg_equipment_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     lv_equipment_id := NULL;

                     lv_err_desc :=
                        'EQUIPMENT not found in target database- record not loaded';
                     insert_log (
                        lv_procedure_name,                           -- arg_program_function
                        'ERROR:No Data',                                 -- arg_class_of_err
                        -22052,                                            -- arg_err_loc_id
                           gv_primary_key
                        || ': LEGACY EQ_ID'
                        || grec_ewo_detail.stg_leg_equipment_id,  -- arg_src_data_key_lookup
                        lv_err_desc,                                         -- arg_err_desc
                        'Create EQUIPMENT in MSS'                 -- arg_resolution_required
                                                 );
                     RAISE exit_record;
                  WHEN TOO_MANY_ROWS
                  THEN
                     lv_equipment_id := NULL;
                     lv_err_desc :=
                        'Duplicate EQUIPMENT found in target database- record not loaded';
                     insert_log (
                        lv_procedure_name,                           -- arg_program_function
                        'ERROR:Duplicate Data',                          -- arg_class_of_err
                        -22054,                                            -- arg_err_loc_id
                           gv_primary_key
                        || ': LEGACY EQ_ID'
                        || grec_ewo_detail.stg_leg_equipment_id,  -- arg_src_data_key_lookup
                        lv_err_desc,                                         -- arg_err_desc
                        'Remove duplicate data from MSS'          -- arg_resolution_required
                                                        );
                     RAISE exit_record;
               END;

               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.si_equipment
                WHERE equipment_id = lv_equipment_id;

               IF lv_count = 0 AND lv_equipment_id IS NOT NULL
               THEN
                  gv_ckt_seq := gv_ckt_seq + 1;
                  load_ewo_equipment (lv_equipment_id);
               ELSE
                  lv_err_desc :=
                     'EQUIPMENT already found associated to an order in target database- record not loaded';
                  insert_log (
                     lv_procedure_name,                              -- arg_program_function
                     'ERROR:Duplicate Data',                             -- arg_class_of_err
                     -22056,                                               -- arg_err_loc_id
                        gv_primary_key
                     || ': LEGACY EQ_ID'
                     || grec_ewo_detail.stg_leg_equipment_id,     -- arg_src_data_key_lookup
                     lv_err_desc,                                            -- arg_err_desc
                     'Only allow new records to be staged'        -- arg_resolution_required
                                                          );
                  RAISE exit_record;
               END IF;

               IF gv_fatal = 'Y'
               THEN
                  RAISE stop_load;
               END IF;

               IF gv_fail_ind = 'Y'
               THEN
                  RAISE exit_record;
               END IF;
            ELSIF grec_ewo_detail.gen_object_case = 'NOTES'
            THEN
               SELECT NVL (MAX (notes_sequence), 0) + 1
                 INTO lv_notes_seq
                 FROM asap.notes
                WHERE document_number = gv_document_number;
				
		---------------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.notes table
        --
        -- Target table : asap.notes,aref_notes
        -- function_name : LOAD_EWO_DETAIL
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------


               SELECT asap.sq_notes_id.NEXTVAL
                 INTO lv_notes_id
                 FROM DUAL;

               INSERT INTO asap.notes (notes_id,
                                       document_number,
                                       notes_sequence,
                                       note_text,
                                       user_id,
                                       date_entered,
                                       last_modified_userid,
                                       last_modified_date,
                                       circuit_design_id,
                                       document_number_src,
                                       system_gen_ind,
                                       circuit_note_ind,
                                       exchange_carrier_circuit_id,
                                       location_id,
                                       location_id_2)
                    VALUES (asap.sq_notes_id.NEXTVAL,
                            gv_document_number,
                            lv_notes_seq,
                            grec_ewo_detail.stg_note_text,
                            grec_ewo_detail.stg_note_userid,
                            grec_ewo_detail.stg_note_date_entered,
                            gv_lmuid,
                            gv_lm_date,
                            NULL,
                            NULL,
                            grec_ewo_detail.stg_system_gen_ind,
                            grec_ewo_detail.stg_circuit_note_ind,
                            NULL,
                            NULL,
                            NULL);

               INSERT INTO ZAYOMSS.aref_notes
                       VALUES (
                                 null, --grec_ewo_detail.gen_source,
                                 grec_ewo_detail.stg_notes_id,
                                 lv_notes_id);
								 
		 ---------------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.ms_attachment_link table
        --
        -- Target table : asap.ms_attachment_link
        -- function_name : LOAD_EWO_DETAIL
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------
            ELSIF grec_ewo_detail.gen_object_case = 'ATTACHMENT'
            THEN
               SELECT asap.sq_ms_attachment_link.NEXTVAL
                 INTO lv_ms_attachment_link_id
                 FROM DUAL;

               INSERT INTO asap.ms_attachment_link (ms_attachment_link_id,
                                                    ms_table_nm,
                                                    ms_table_key_id,
                                                    ms_table_key_value,
                                                    url,
                                                    url_desc,
                                                    creation_date,
                                                    last_modified_userid,
                                                    last_modified_date,
                                                    transform_id,
                                                    ms_attachment_nm,
                                                    ms_attachment)
                    VALUES (lv_ms_attachment_link_id,               --MS_ATTACHMENT_LINK_ID,
                            grec_ewo_detail.stg_ms_table_nm,                  --MS_TABLE_NM,
                            gv_document_number,                           --MS_TABLE_KEY_ID,
                            grec_ewo_detail.stg_ms_table_key_value,    --MS_TABLE_KEY_VALUE,
                            grec_ewo_detail.stg_url,                                  --URL,
                            grec_ewo_detail.stg_url_desc,                        --URL_DESC,
                            NVL (grec_ewo_detail.stg_creation_date, gv_lm_date), --CREATION_DATE,
                            gv_lmuid,                                --LAST_MODIFIED_USERID,
                            gv_lm_date,                                --LAST_MODIFIED_DATE,
                            grec_ewo_detail.stg_transform_id,                --TRANSFORM_ID,
                            grec_ewo_detail.stg_ms_attachment_nm,        --MS_ATTACHMENT_NM,
                            NULL                                             --MS_ATTACHMENT
                                );
								
		 ---------------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.sr_supp_history table
        --
        -- Target table : asap.sr_supp_history
        -- function_name : LOAD_EWO_DETAIL
        -- Stage table : STG_EWO_DETAIL
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------
            ELSIF grec_ewo_detail.gen_object_case = 'SUPP_HISTORY'
            THEN
               SELECT COUNT (*)
                 INTO lv_count
                 FROM asap.sr_supp_history
                WHERE     document_number = gv_document_number
                      AND version_identification =
                             grec_ewo_detail.stg_version_identification;

               IF lv_count > 0
               THEN
                  lv_err_desc :=
                     'Record already found for Supp History in target database- record not loaded';
                  insert_log (lv_procedure_name,                     -- arg_program_function
                              'ERROR:Duplicate Data',                    -- arg_class_of_err
                              -22056,                                      -- arg_err_loc_id
                              gv_primary_key,                     -- arg_src_data_key_lookup
                              lv_err_desc,                                   -- arg_err_desc
                              'Only allow new records to be staged' -- arg_resolution_required
                                                                   );
                  RAISE exit_record;
               END IF;

               INSERT INTO asap.sr_supp_history (document_number,
                                                 version_identification,
                                                 supplement_type,
                                                 supp_note,
                                                 ver_ident_scheme,
                                                 last_modified_userid,
                                                 last_modified_date,
                                                 supp_cancel_reason)
                    VALUES (gv_document_number,                           --document_number,
                            grec_ewo_detail.stg_version_identification, --version_identification,
                            grec_ewo_detail.stg_supplement_type,          --supplement_type,
                            grec_ewo_detail.stg_supp_note,                      --supp_note,
                            grec_ewo_detail.stg_ver_ident_scheme,        --ver_ident_scheme,
                            gv_lmuid,                                --LAST_MODIFIED_USERID,
                            gv_lm_date,                                --LAST_MODIFIED_DATE,
                            grec_ewo_detail.stg_supp_cancel_reason      --supp_cancel_reason
                                                                  );
            END IF;

            gv_target_recs_dtl := gv_target_recs_dtl + 1;
         EXCEPTION
            WHEN exit_record
            THEN
               gv_error_recs_dtl := gv_error_recs_dtl + 1;
         END;
      END LOOP;
      
      gv_fatal := 'N';
      gv_fail_ind := 'N';
   EXCEPTION
      WHEN stop_load
      THEN
         gv_error_recs_dtl := gv_error_recs_dtl + 1;
      WHEN OTHERS
      THEN
         ROLLBACK;
         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'ERROR',
                     -29999,
                     gv_primary_key,
                     lv_err_desc,
                     'Need to check code');

         gv_fail_ind := 'Y';
         gv_error_recs_dtl := gv_error_recs_dtl + 1;
         --COMMIT;
   END;
------------------------------------
   PROCEDURE load_ewo_order
   IS
      rec_conv_stat_summary_log   ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      lv_procedure_name           ZAYOMSS.conv_conversion_error_log.program_function%TYPE
                                     := 'LOAD_EWO_ORDER';
      lv_proc_name                ZAYOMSS.conv_conversion_error_log.program_function%TYPE;
      lv_insert                   NUMBER (9) := 0;
      lv_count                    NUMBER (9) := 0;
      lv_count1                   NUMBER (9) := 0;
      lv_count2                   NUMBER (9) := 0;
      lv_count3                   NUMBER (9) := 0;
      lv_commit                   NUMBER (9) := 0;
      lv_total_recs               NUMBER (9) := 0;
      lv_target_recs              NUMBER (9) := 0;
      lv_error_recs               NUMBER (9) := 0;
      debug_msg                   VARCHAR2 (200);
      arg_err_desc                VARCHAR2 (200);
      lv_org_id                   VARCHAR2 (5);
      lv_factl_location           NUMBER (9);
      lv_ac_sw_location           NUMBER (9);
      lv_secloc_location          NUMBER (9);
      lv_muxloc_location          NUMBER (9);
      lv_smuxloc_location         NUMBER (9);
      lv_actl_exists              NUMBER := 0;
      lv_serv_loc                 VARCHAR2 (15);
      lv_srl_cnt                  NUMBER := 0;
      lv_party_id                 asap.party.party_id%TYPE;
      lv_party_role_seq           NUMBER;
      lv_party_role_addr_seq      NUMBER;
      lv_party_addr_seq           NUMBER;
      lv_sr_party_role_id         NUMBER;
      lv_party_role_type_cd       VARCHAR2 (10);
      lv_serv_item_id             NUMBER;
      lv_cust_name_abbr           asap.access_cust.customer_name_abbreviation%TYPE;
      lv_location_id              asap.network_location.location_id%TYPE;
      lv_remark_seq               NUMBER (9) := 0;
      lv_remark                   VARCHAR2 (256);
      lv_svcreq_provplan          asap.svcreq_provplan.req_plan_id%TYPE;
      lv_err_desc                 ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err             ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator            ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup               ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd          ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      error_record                EXCEPTION;
      lv_char_cnt                 NUMBER;
   BEGIN
      IF grec_ewo.stg_org_name IS NOT NULL
      THEN
         BEGIN
            SELECT organization_id
              INTO lv_org_id
              FROM asap.organization
             WHERE organization_name = 'ELECTRIC LIGHTWAVE, INC.C'
			 AND ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               lv_org_id := NULL;
               lv_err_desc := 'ORGANIZATION_NAME does not exist in target database';

               lv_class_of_err := 'WARNING:NO Data';
               lv_error_locator := -22061;
               lv_src_lookup :=
                     'ORDER_ID ='
                  || grec_ewo.stg_ewo_order_id
                  || ','
                  || 'ORG_NAME = '
                  || grec_ewo.stg_org_name;
               lv_resolution_reqd := 'Create ORGANIZATION_NAME in MSS';
               RAISE error_record;
         END;
      ELSE
         lv_org_id := NULL;
      END IF;
	     ------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.serv_req ,asap.access_service_request, asap.remark tables
        --
        -- Target table : asap.serv_req, asap.access_service_request, asap.remark
        -- function_name : LOAD_EWO_ORDER
        -- Stage table : STG_EWO_MAIN
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------

      SELECT asap.sq_serv_req.NEXTVAL
        INTO gv_document_number
        FROM DUAL;

      INSERT INTO asap.serv_req (document_number,
                                 order_compl_dt,
                                 type_of_sr,
                                 last_modified_userid,
                                 last_modified_date,
                                 req_header_id,
                                 service_req_seq,
                                 first_ecckt_id,
                                 order_number,
                                 cust_acct_id,
                                 interim_bill_ind,
                                 bill_act_dt,
                                 ccna,
                                 pon,
                                 project_identification,
                                 version_identification,
                                 desired_due_date,
                                 request_type,
                                 request_type_status,
                                 activity_ind,
                                 supplement_type,
                                 related_pon,
                                 acna,
                                 order_entered_contact_name,
                                 prov_order_id,
                                 service_request_seq,
                                 sent_recv_cd,
                                 responsible_party,
                                 service_request_status,
                                 case_number,
                                 neg_rate_ind,
                                 organization_id,
                                 ccna_name,
                                 acna_name,
                                 source_code,
                                 web_confirmation_id,
                                 gmt_dt_tm_received,
                                 serv_req_sent_tri,
                                 expert_mode_sf_type_nm,
                                 expert_model_sf_stuc_format_nm,
                                 template_ind,
                                 template_short_desc,
                                 external_order_nbr,
                                 expedite_tri,
                                 supp_cancel_reason_cd,
                                 old_ccna,
                                 project_name,
                                 project_description,
                                 date_created,
                                 project_status,
                                 is_auto_ewo,
                                 npa,
                                 nxx,
                                 sr_audit_ind)
           VALUES (gv_document_number,                                    --DOCUMENT_NUMBER,
                   grec_ewo.stg_order_compl_dt,                            --ORDER_COMPL_DT,
                   'EWO',                                                      --TYPE_OF_SR,
                   gv_lmuid,                                         --LAST_MODIFIED_USERID,
                   gv_lm_date,                                         --LAST_MODIFIED_DATE,
                   NULL,                                                    --REQ_HEADER_ID,
                   NULL,                                                  --SERVICE_REQ_SEQ,
                   grec_ewo.stg_first_ecckt_id,                            --FIRST_ECCKT_ID,
                   grec_ewo.stg_order_number,                                --ORDER_NUMBER,
                   NULL,                                                     --CUST_ACCT_ID,
                   'N',                                                  --INTERIM_BILL_IND,
                   NULL,                                                      --BILL_ACT_DT,
                   NULL,                                                             --CCNA,
                   grec_ewo.stg_pon,                                                  --PON,
                   grec_ewo.stg_project_identification,            --PROJECT_IDENTIFICATION,
                   NULL,                                           --VERSION_IDENTIFICATION,
                   grec_ewo.stg_desired_due_date,                        --DESIRED_DUE_DATE,
                   NULL,                                                     --REQUEST_TYPE,
                   NULL,                                              --REQUEST_TYPE_STATUS,
                   grec_ewo.stg_activity_ind,                                --ACTIVITY_IND,
                   grec_ewo.stg_supplement_type,                          --SUPPLEMENT_TYPE,
                   NULL,                                                      --RELATED_PON,
                   NULL,                                                             --ACNA,
                   NULL,                                       --ORDER_ENTERED_CONTACT_NAME,
                   NULL,                                                    --PROV_ORDER_ID,
                   NULL,                                              --SERVICE_REQUEST_SEQ,
                   NULL,                                                     --SENT_RECV_CD,
                   grec_ewo.stg_responsible_party,                      --RESPONSIBLE_PARTY,
                   grec_ewo.stg_service_request_status,            --SERVICE_REQUEST_STATUS,
                   NULL,                                                      --CASE_NUMBER,
                   NULL,                                                     --NEG_RATE_IND,
                   lv_org_id, --grec_ewo.stg_organization_id,                --ORGANIZATION_ID,
                   NULL,                                                        --CCNA_NAME,
                   NULL,                                                        --ACNA_NAME,
                   NULL,                                                      --SOURCE_CODE,
                   NULL,                                              --WEB_CONFIRMATION_ID,
                   grec_ewo.stg_gmt_dt_tm_received,                    --GMT_DT_TM_RECEIVED,
                   NULL,                                                --SERV_REQ_SENT_TRI,
                   NULL,                                           --EXPERT_MODE_SF_TYPE_NM,
                   NULL,                                   --EXPERT_MODEL_SF_STUC_FORMAT_NM,
                   NULL,                                                     --TEMPLATE_IND,
                   NULL,                                              --TEMPLATE_SHORT_DESC,
                   NULL,                                               --EXTERNAL_ORDER_NBR,
                   NULL,                                                     --EXPEDITE_TRI,
                   NULL,                                            --SUPP_CANCEL_REASON_CD,
                   NULL,                                                         --OLD_CCNA,
                   NULL,                                                     --PROJECT_NAME,
                   NULL,                                              --PROJECT_DESCRIPTION,
                   NULL,                                                     --DATE_CREATED,
                   NULL,                                                   --PROJECT_STATUS,
                   NULL,                                                      --IS_AUTO_EWO,
                   NULL,                                                              --NPA,
                   NULL,                                                              --NXX,
                   NULL                                                       --SR_AUDIT_IND
                       );

      INSERT INTO asap.access_service_request (document_number,
                                               order_type,
                                               special_routing_code_type,
                                               version_identification,
                                               pon,
                                               asr_number,
                                               ckr,
                                               special_action_quality_ac_ind,
                                               ltp_elements_ordered,
                                               ltp_elements_using_sa_fac,
                                               ltp_ef_level,
                                               ltp_transport_level,
                                               telecom_service_priority,
                                               frame_due_time,
                                               test_order_cabs_extract_date,
                                               test_order_cabs_extract_ind,
                                               desired_due_date,
                                               project_plant_test_date,
                                               project_facility_plant_test_da,
                                               basic_serving_arrangement,
                                               tq_request_1,
                                               tq_request_2,
                                               universal_service_order,
                                               total_nbr_due_date_changes,
                                               quote_authorized_indicator,
                                               service_request_sequence_nbr,
                                               number_of_requests,
                                               lata_usage_percentage,
                                               service_and_product_enhanc_cod,
                                               percent_interstate_use,
                                               request_type,
                                               request_type_status,
                                               activity_indicator,
                                               supplement_type,
                                               access_billable_ind,
                                               additional_forms_aci,
                                               additional_forms_tsr,
                                               additional_forms_eod,
                                               expedite_indicator,
                                               additional_engineering_ind,
                                               additional_labor_ind,
                                               quantity_first,
                                               quantity_second,
                                               billing_account_number,
                                               ec_change_request_ind,
                                               bic_telephone_nbr,
                                               ec_identifier,
                                               quantity_unit,
                                               ec_related_order_number,
                                               coordinated_conversion,
                                               suppress_circuit_ind,
                                               variable_term_id,
                                               date_received,
                                               associated_lec_order_id,
                                               billing_comments,
                                               serv_ord_sys_number,
                                               agency_authorization_status,
                                               date_of_agency_authorization,
                                               extended_billing_plan,
                                               duplicated_media,
                                               access_service_group,
                                               two_six_code,
                                               date_time_sent,
                                               cancel_date,
                                               related_pon,
                                               plant_test_tel_number,
                                               tax_exemption_ind,
                                               converted_asr_indicator,
                                               connecting_facility_assignment,
                                               connecting_fac_assignment_use,
                                               secondary_connect_fac,
                                               sec_connecting_fac_assign_use,
                                               days_confirm_dlr_ind,
                                               high_cap_bill_account_number,
                                               transmit_tlv,
                                               secondary_transmit_tlv,
                                               receive_tlv,
                                               secondary_receive_tlv,
                                               non_revenue_ind,
                                               lease_arrangement_ind,
                                               plant_test_date,
                                               ic_circuit_reference,
                                               service_request_status,
                                               last_modified_userid,
                                               last_modified_date,
                                               organization_id,
                                               location_id,
                                               npa,
                                               nxx,
                                               location_id_2,
                                               access_provider_serv_ctr_code,
                                               access_prov_serv_ctr_code2,
                                               access_carrier_number,
                                               access_carrier_number_2,
                                               project_identification,
                                               network_channel_service_code,
                                               network_channel_option_code,
                                               network_channel_service_code_2,
                                               network_channel_option_code_2,
                                               operating_company_number,
                                               network_channel_interface_code,
                                               agency_of_federal_government,
                                               document_number_2,
                                               letter_of_auth_req_ind,
                                               expected_measured_loss_receive,
                                               expected_measured_loss_transmi,
                                               obf_asr_version_number,
                                               subscriber_auth_id,
                                               trans_doc_status_code,
                                               access_cust_ckt_ref_t1,
                                               desired_fac_dlr_date,
                                               desired_dlr_date,
                                               desired_foc_date,
                                               facility_bill_arg,
                                               facility_bill_arg_ef,
                                               facility_bill_arg_dt,
                                               facility_bill_arg_mux,
                                               apot_indicator,
                                               additional_point_of_term,
                                               parent_document_number,
                                               parent_child_ind,
                                               fni,
                                               ac_fni,
                                               channel_pair_timeslot,
                                               location_id_primary_adm,
                                               location_id_secondary_adm,
                                               rush_ind,
                                               supp_level,
                                               company_code,
                                               case_number,
                                               response_type_requested,
                                               percent_local_usage,
                                               variable_term_agmt_vc,
                                               cross_connect_eq_assign,
                                               additional_forms_nai,
                                               unbundled_network_element,
                                               wireless_svc_type,
                                               qty_network_assign_info,
                                               location_id_psl,
                                               pri_svc_loc_cd,
                                               wireless_screening_tel_nbr,
                                               clarification_req_ind,
                                               pot_bay_type,
                                               iw_billing_account_number,
                                               call_before_dispatch,
                                               qty_svc_addr,
                                               serv_item_id,
                                               lease_arrangement_dt,
                                               lease_arrangement_nm,
                                               lata_number,
                                               two_six_cd2,
                                               two_six_cd3,
                                               two_six_cd4,
                                               special_action_quality_ap_ind,
                                               industry_validation_ind,
                                               trunk_activity_identifier,
                                               call_before_dispatch_1,
                                               time_zone_code,
                                               fiber_network_type_code,
                                               evc_ind,
                                               rel_fni,
                                               wireless_site_ind,
                                               promotion_nbr,
                                               promo_subs_date,
                                               fed_universal_serv_fee,
                                               additional_forms_vcat,
                                               link_aggregation_group,
                                               coordinated_change_ind,
                                               service_reservation_number,
                                               secondary_connect_fac_use,
                                               location_id_jpr,
                                               early_date_acceptance,
                                               network_access_groom,
                                               switched_ethernet_indicator,
                                               end_user_indicator,
                                               pvc_ind,
                                               nbr_of_perm_virtual_conn,
                                               wireless_site_ind_priloc)
           VALUES (gv_document_number,                                    --DOCUMENT_NUMBER,
                   'EWO',                                                      --ORDER_TYPE,
                   NULL,                                        --SPECIAL_ROUTING_CODE_TYPE,
                   NULL,                                           --VERSION_IDENTIFICATION,
                   NULL,                                                              --PON,
                   NULL,                                                       --ASR_NUMBER,
                   NULL,                                                              --CKR,
                   NULL,                                    --SPECIAL_ACTION_QUALITY_AC_IND,
                   NULL,                                             --LTP_ELEMENTS_ORDERED,
                   NULL,                                        --LTP_ELEMENTS_USING_SA_FAC,
                   NULL,                                                     --LTP_EF_LEVEL,
                   NULL,                                              --LTP_TRANSPORT_LEVEL,
                   NULL,                                         --TELECOM_SERVICE_PRIORITY,
                   NULL,                                                   --FRAME_DUE_TIME,
                   NULL,                                     --TEST_ORDER_CABS_EXTRACT_DATE,
                   'N',                                       --TEST_ORDER_CABS_EXTRACT_IND,
                   grec_ewo.stg_desired_due_date,                        --DESIRED_DUE_DATE,
                   NULL,                                          --PROJECT_PLANT_TEST_DATE,
                   NULL,                                   --PROJECT_FACILITY_PLANT_TEST_DA,
                   NULL,                                        --BASIC_SERVING_ARRANGEMENT,
                   NULL,                                                     --TQ_REQUEST_1,
                   NULL,                                                     --TQ_REQUEST_2,
                   NULL,                                          --UNIVERSAL_SERVICE_ORDER,
                   NULL,                                       --TOTAL_NBR_DUE_DATE_CHANGES,
                   NULL,                                       --QUOTE_AUTHORIZED_INDICATOR,
                   NULL,                                     --SERVICE_REQUEST_SEQUENCE_NBR,
                   NULL,                                               --NUMBER_OF_REQUESTS,
                   NULL,                                            --LATA_USAGE_PERCENTAGE,
                   NULL,                                   --SERVICE_AND_PRODUCT_ENHANC_COD,
                   NULL,                                           --PERCENT_INTERSTATE_USE,
                   NULL,                                                     --REQUEST_TYPE,
                   NULL,                                              --REQUEST_TYPE_STATUS,
                   NVL (grec_ewo.stg_activity_ind, 'N'),               --ACTIVITY_INDICATOR,
                   NULL,                                                  --SUPPLEMENT_TYPE,
                   NULL,                                              --ACCESS_BILLABLE_IND,
                   NULL,                                             --ADDITIONAL_FORMS_ACI,
                   NULL,                                             --ADDITIONAL_FORMS_TSR,
                   NULL,                                             --ADDITIONAL_FORMS_EOD,
                   NULL,                                               --EXPEDITE_INDICATOR,
                   NULL,                                       --ADDITIONAL_ENGINEERING_IND,
                   NULL,                                             --ADDITIONAL_LABOR_IND,
                   NULL,                                                   --QUANTITY_FIRST,
                   NULL,                                                  --QUANTITY_SECOND,
                   NULL,                                           --BILLING_ACCOUNT_NUMBER,
                   NULL,                                            --EC_CHANGE_REQUEST_IND,
                   NULL,                                                --BIC_TELEPHONE_NBR,
                   NULL,                                                    --EC_IDENTIFIER,
                   NULL,                                                    --QUANTITY_UNIT,
                   NULL,                                          --EC_RELATED_ORDER_NUMBER,
                   NULL,                                           --COORDINATED_CONVERSION,
                   NULL,                                             --SUPPRESS_CIRCUIT_IND,
                   NULL,                                                 --VARIABLE_TERM_ID,
                   NULL,                                                    --DATE_RECEIVED,
                   NULL,                                          --ASSOCIATED_LEC_ORDER_ID,
                   NULL,                                                 --BILLING_COMMENTS,
                   NULL,                                              --SERV_ORD_SYS_NUMBER,
                   NULL,                                      --AGENCY_AUTHORIZATION_STATUS,
                   NULL,                                     --DATE_OF_AGENCY_AUTHORIZATION,
                   NULL,                                            --EXTENDED_BILLING_PLAN,
                   NULL,                                                 --DUPLICATED_MEDIA,
                   NULL,                                             --ACCESS_SERVICE_GROUP,
                   NULL,                                                     --TWO_SIX_CODE,
                   NULL,                                                   --DATE_TIME_SENT,
                   NULL,                                                      --CANCEL_DATE,
                   NULL,                                                      --RELATED_PON,
                   NULL,                                            --PLANT_TEST_TEL_NUMBER,
                   NULL,                                                --TAX_EXEMPTION_IND,
                   NULL,                                          --CONVERTED_ASR_INDICATOR,
                   NULL,                                   --CONNECTING_FACILITY_ASSIGNMENT,
                   NULL,                                    --CONNECTING_FAC_ASSIGNMENT_USE,
                   NULL,                                            --SECONDARY_CONNECT_FAC,
                   NULL,                                    --SEC_CONNECTING_FAC_ASSIGN_USE,
                   NULL,                                             --DAYS_CONFIRM_DLR_IND,
                   NULL,                                     --HIGH_CAP_BILL_ACCOUNT_NUMBER,
                   NULL,                                                     --TRANSMIT_TLV,
                   NULL,                                           --SECONDARY_TRANSMIT_TLV,
                   NULL,                                                      --RECEIVE_TLV,
                   NULL,                                            --SECONDARY_RECEIVE_TLV,
                   'N',                                                   --NON_REVENUE_IND,
                   NULL,                                            --LEASE_ARRANGEMENT_IND,
                   NULL,                                                  --PLANT_TEST_DATE,
                   NULL,                                             --IC_CIRCUIT_REFERENCE,
                   grec_ewo.stg_service_request_status,            --SERVICE_REQUEST_STATUS,
                   gv_lmuid,                                         --LAST_MODIFIED_USERID,
                   gv_lm_date,                                         --LAST_MODIFIED_DATE,
                   lv_org_id,                                             --ORGANIZATION_ID,
                   NULL,                                                      --LOCATION_ID,
                   NULL,                                                              --NPA,
                   NULL,                                                              --NXX,
                   NULL,                                                    --LOCATION_ID_2,
                   NULL,                                    --ACCESS_PROVIDER_SERV_CTR_CODE,
                   NULL,                                       --ACCESS_PROV_SERV_CTR_CODE2,
                   NULL,                                            --ACCESS_CARRIER_NUMBER,
                   NULL,                                          --ACCESS_CARRIER_NUMBER_2,
                   NULL,                                           --PROJECT_IDENTIFICATION,
                   NULL,                                     --NETWORK_CHANNEL_SERVICE_CODE,
                   NULL,                                      --NETWORK_CHANNEL_OPTION_CODE,
                   NULL,                                   --NETWORK_CHANNEL_SERVICE_CODE_2,
                   NULL,                                    --NETWORK_CHANNEL_OPTION_CODE_2,
                   NULL,                                         --OPERATING_COMPANY_NUMBER,
                   NULL,                                   --NETWORK_CHANNEL_INTERFACE_CODE,
                   NULL,                                     --AGENCY_OF_FEDERAL_GOVERNMENT,
                   NULL,                                                --DOCUMENT_NUMBER_2,
                   NULL,                                           --LETTER_OF_AUTH_REQ_IND,
                   NULL,                                   --EXPECTED_MEASURED_LOSS_RECEIVE,
                   NULL,                                   --EXPECTED_MEASURED_LOSS_TRANSMI,
                   '0',                                            --OBF_ASR_VERSION_NUMBER,
                   NULL,                                               --SUBSCRIBER_AUTH_ID,
                   NULL,                                            --TRANS_DOC_STATUS_CODE,
                   NULL,                                           --ACCESS_CUST_CKT_REF_T1,
                   NULL,                                             --DESIRED_FAC_DLR_DATE,
                   NULL,                                                 --DESIRED_DLR_DATE,
                   NULL,                                                 --DESIRED_FOC_DATE,
                   NULL,                                                --FACILITY_BILL_ARG,
                   NULL,                                             --FACILITY_BILL_ARG_EF,
                   NULL,                                             --FACILITY_BILL_ARG_DT,
                   NULL,                                            --FACILITY_BILL_ARG_MUX,
                   NULL,                                                   --APOT_INDICATOR,
                   NULL,                                         --ADDITIONAL_POINT_OF_TERM,
                   NULL,                                           --PARENT_DOCUMENT_NUMBER,
                   NULL,                                                 --PARENT_CHILD_IND,
                   NULL,                                                              --FNI,
                   NULL,                                                           --AC_FNI,
                   NULL,                                            --CHANNEL_PAIR_TIMESLOT,
                   NULL,                                          --LOCATION_ID_PRIMARY_ADM,
                   NULL,                                        --LOCATION_ID_SECONDARY_ADM,
                   NULL,                                                         --RUSH_IND,
                   NULL,                                                       --SUPP_LEVEL,
                   NULL,                                                     --COMPANY_CODE,
                   NULL,                                                      --CASE_NUMBER,
                   NULL,                                          --RESPONSE_TYPE_REQUESTED,
                   NULL,                                              --PERCENT_LOCAL_USAGE,
                   NULL,                                            --VARIABLE_TERM_AGMT_VC,
                   NULL,                                          --CROSS_CONNECT_EQ_ASSIGN,
                   NULL,                                             --ADDITIONAL_FORMS_NAI,
                   NULL,                                        --UNBUNDLED_NETWORK_ELEMENT,
                   NULL,                                                --WIRELESS_SVC_TYPE,
                   NULL,                                          --QTY_NETWORK_ASSIGN_INFO,
                   NULL,                                                  --LOCATION_ID_PSL,
                   NULL,                                                   --PRI_SVC_LOC_CD,
                   NULL,                                       --WIRELESS_SCREENING_TEL_NBR,
                   NULL,                                            --CLARIFICATION_REQ_IND,
                   NULL,                                                     --POT_BAY_TYPE,
                   NULL,                                        --IW_BILLING_ACCOUNT_NUMBER,
                   NULL,                                             --CALL_BEFORE_DISPATCH,
                   NULL,                                                     --QTY_SVC_ADDR,
                   NULL,                                                     --SERV_ITEM_ID,
                   NULL,                                             --LEASE_ARRANGEMENT_DT,
                   NULL,                                             --LEASE_ARRANGEMENT_NM,
                   NULL,                                                      --LATA_NUMBER,
                   NULL,                                                      --TWO_SIX_CD2,
                   NULL,                                                      --TWO_SIX_CD3,
                   NULL,                                                      --TWO_SIX_CD4,
                   NULL,                                    --SPECIAL_ACTION_QUALITY_AP_IND,
                   NULL,                                          --INDUSTRY_VALIDATION_IND,
                   NULL,                                        --TRUNK_ACTIVITY_IDENTIFIER,
                   NULL,                                           --CALL_BEFORE_DISPATCH_1,
                   NULL,                                                   --TIME_ZONE_CODE,
                   NULL,                                          --FIBER_NETWORK_TYPE_CODE,
                   NULL,                                                          --EVC_IND,
                   NULL,                                                          --REL_FNI,
                   NULL,                                                --WIRELESS_SITE_IND,
                   NULL,                                                    --PROMOTION_NBR,
                   NULL,                                                  --PROMO_SUBS_DATE,
                   NULL,                                           --FED_UNIVERSAL_SERV_FEE,
                   'N',                                             --ADDITIONAL_FORMS_VCAT,
                   NULL,                                           --LINK_AGGREGATION_GROUP,
                   NULL,                                           --COORDINATED_CHANGE_IND,
                   NULL,                                       --SERVICE_RESERVATION_NUMBER,
                   NULL,                                        --SECONDARY_CONNECT_FAC_USE,
                   NULL,                                                  --LOCATION_ID_JPR,
                   NULL,                                            --EARLY_DATE_ACCEPTANCE,
                   NULL,                                             --NETWORK_ACCESS_GROOM,
                   NULL,                                      --SWITCHED_ETHERNET_INDICATOR,
                   NULL,                                               --END_USER_INDICATOR,
                   NULL,                                                          --PVC_IND,
                   NULL,                                         --NBR_OF_PERM_VIRTUAL_CONN,
                   NULL                                           --WIRELESS_SITE_IND_PRILOC
                       );

      IF grec_ewo.stg_remark IS NOT NULL
      THEN
         SELECT NVL (MAX (sequence_number), 0) + 1
           INTO lv_remark_seq
           FROM asap.remark
          WHERE document_number = gv_document_number;

         SELECT   TO_NUMBER (SUBSTR (DUMP (grec_ewo.stg_remark),11,
                             INSTR (DUMP (grec_ewo.stg_remark),':',1,1)- 11))- 256
           INTO lv_char_cnt
           FROM DUAL;

         IF lv_char_cnt > 0
         THEN
            BEGIN
               lv_remark := SUBSTR (grec_ewo.stg_remark, 1, 256 - lv_char_cnt);
            EXCEPTION
               WHEN OTHERS
               THEN
                  lv_remark := SUBSTR (grec_ewo.stg_remark, 1, 240);
            END;
         ELSE
            lv_remark := SUBSTR (grec_ewo.stg_remark, 1, 256);
         END IF;

         INSERT INTO asap.remark
              VALUES ('EWO',                                                       --form_id
                      lv_remark_seq,                                       --sequence_number
                      gv_document_number,                                  --document_number
                      lv_remark,                                                    --REMARK
                      NULL,                                               --reference_number
                      SUBSTR (gv_lmuid, 1, 8),                       --LAST_MODIFIED_USERID,
                      gv_lm_date                                       --LAST_MODIFIED_DATE,
                                );
      END IF;

	     ------------------------------------------------------------------
        --
        -- Usage: Insert value into the  asap.sq_svcreq_provplan, asap.task , aref_so_ewo tables
        --
        -- Target table : asap.svcreq_provplan,asap.task, aref_so_ewo
        -- function_name : LOAD_EWO_ORDER
        -- Stage table : STG_EWO_MAIN
        --
        -- Errors: Raises fatal exception if the entry cannot be inserted
        --
        ---------------------------------------------------------------------------



      --Assign Provisioning Plan--
      SELECT asap.sq_svcreq_provplan.NEXTVAL
        INTO lv_svcreq_provplan
        FROM DUAL;

      INSERT INTO asap.svcreq_provplan (req_plan_id,
                                        document_number,
                                        plan_id,
                                        last_modified_userid,
                                        last_modified_date,
                                        display_sequence)
           VALUES (lv_svcreq_provplan,
                   gv_document_number,
                   gv_plan_id,
                   'ZAYOM6',
                   SYSDATE,
                   NULL);

      INSERT INTO asap.task (document_number,
                             task_number,
                             task_status,
                             task_priority,
                             scheduled_completion_date,
                             actual_completion_date,
                             estimated_completion_date,
                             last_modified_userid,
                             last_modified_date,
                             task_type,
                             circuit_design_id,
                             work_queue_id,
                             work_queue_priority,
                             assigned_from_work_queue,
                             assigned_from_date,
                             task_status_date,
                             queue_status,
                             revised_completion_date,
                             actual_release_date,
                             billing_status,
                             scheduled_release_date,
                             sort_priority,
                             required_ind,
                             system_gen_ind,
                             req_plan_id,
                             task_open_ind,
                             job_id,
                             auto_comp_ind,
                             first_jeopardy_id,
                             sequence,
                             reject_status,
                             task_prompt,
                             assign_dt_cd,
                             serv_item_id,
                             completion_days,
                             completion_hours,
                             completion_minutes,
                             potentially_late_days,
                             potentially_late_hours,
                             potentially_late_minutes,
                             close_of_business_ind,
                             late_prompt_ind,
                             system_task_ind,
                             task_label,
                             execution_point,
                             disposition_days,
                             disposition_lock_ind,
                             prev_work_queue_id,
                             task_opened_ind,
                             late_extension_ind,
                             potential_late_extension_ind,
                             task_label_2,
                             task_label_3,
                             task_label_4,
                             task_label_5,
                             task_label_6,
                             task_label_7,
                             task_label_8,
                             task_label_9,
                             task_label_10,
                             task_label_11,
                             task_label_12,
                             task_label_13,
                             task_label_14,
                             task_label_15,
                             task_label_16,
                             task_label_17,
                             task_label_18,
                             task_label_19,
                             task_label_20,
                             expedited_completion_days,
                             expedited_completion_hours,
                             expedited_completion_minutes)
           VALUES (gv_document_number,
					asap.sq_task.NEXTVAL,
                   'Complete',
                   5,
                   SYSDATE,
                   SYSDATE,
                   SYSDATE,
                   'ZAYOM6',
                   SYSDATE,
                   'DESIGN',
                   NULL,
                   'CAP_PLAN',
                   99999,
                   'AUTO',
                   SYSDATE,
                   SYSDATE,
                   NULL,
                   SYSDATE,
                   SYSDATE,
                   NULL,
                   SYSDATE,
                   NULL,
                   'Y',
                   'P',
                   lv_svcreq_provplan,
                   'N',
                   NULL,
                   'N',
                   NULL,
                   1,
                   NULL,
                   'N',
                   'F',
                   NULL,
                   1,
                   0,
                   0,
                   0,
                   0,
                   0,
                   'N',
                   'N',
                   'N',
                   NULL,
                   NULL,
                   NULL,
                   'N',
                   NULL,
                   'Y',
                   'N',
                   'N',
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL);
				  
				  INSERT INTO asap.task (document_number,
                             task_number,
                             task_status,
                             task_priority,
                             scheduled_completion_date,
                             actual_completion_date,
                             estimated_completion_date,
                             last_modified_userid,
                             last_modified_date,
                             task_type,
                             circuit_design_id,
                             work_queue_id,
                             work_queue_priority,
                             assigned_from_work_queue,
                             assigned_from_date,
                             task_status_date,
                             queue_status,
                             revised_completion_date,
                             actual_release_date,
                             billing_status,
                             scheduled_release_date,
                             sort_priority,
                             required_ind,
                             system_gen_ind,
                             req_plan_id,
                             task_open_ind,
                             job_id,
                             auto_comp_ind,
                             first_jeopardy_id,
                             sequence,
                             reject_status,
                             task_prompt,
                             assign_dt_cd,
                             serv_item_id,
                             completion_days,
                             completion_hours,
                             completion_minutes,
                             potentially_late_days,
                             potentially_late_hours,
                             potentially_late_minutes,
                             close_of_business_ind,
                             late_prompt_ind,
                             system_task_ind,
                             task_label,
                             execution_point,
                             disposition_days,
                             disposition_lock_ind,
                             prev_work_queue_id,
                             task_opened_ind,
                             late_extension_ind,
                             potential_late_extension_ind,
                             task_label_2,
                             task_label_3,
                             task_label_4,
                             task_label_5,
                             task_label_6,
                             task_label_7,
                             task_label_8,
                             task_label_9,
                             task_label_10,
                             task_label_11,
                             task_label_12,
                             task_label_13,
                             task_label_14,
                             task_label_15,
                             task_label_16,
                             task_label_17,
                             task_label_18,
                             task_label_19,
                             task_label_20,
                             expedited_completion_days,
                             expedited_completion_hours,
                             expedited_completion_minutes)
           VALUES (gv_document_number,
					asap.sq_task.NEXTVAL,
                   'Complete',
                   5,
                   SYSDATE,
                   SYSDATE,
                   SYSDATE,
                   'ZAYOM6',
                   SYSDATE,
                   'DD',
                   NULL,
                   'CAP_PLAN',
                   99999,
                   'AUTO',
                   SYSDATE,
                   SYSDATE,
                   NULL,
                   SYSDATE,
                   SYSDATE,
                   NULL,
                   SYSDATE,
                   NULL,
                   'Y',
                   'P',
                   lv_svcreq_provplan,
                   'N',
                   NULL,
                   'N',
                   NULL,
                   1,
                   NULL,
                   'N',
                   'F',
                   NULL,
                   1,
                   0,
                   0,
                   0,
                   0,
                   0,
                   'N',
                   'N',
                   'N',
                   NULL,
                   NULL,
                   NULL,
                   'N',
                   NULL,
                   'Y',
                   'N',
                   'N',
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL);

      INSERT INTO ZAYOMSS.aref_so_ewo (stg_source,
                                       stg_order_id,
                                       stg_order_number,
                                       mss_document_number)
           VALUES ('ZAYOM6',                                                       --source
                   gv_order_id,                                           --stg_ewo_order_id
                   grec_ewo.stg_order_number,                            --stg_order_number,
                   gv_document_number                                 --mss_document_number,
                                     );
   EXCEPTION
      WHEN error_record
      THEN
         ROLLBACK;

         insert_log (lv_procedure_name,
                     lv_class_of_err,
                     lv_error_locator,
                     lv_src_lookup,
                     lv_err_desc,                                            -- arg_err_desc
                     lv_resolution_reqd);
         gv_fail_ind := 'Y';
         --COMMIT;
      WHEN OTHERS
      THEN
         ROLLBACK;
         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'FATAL ERROR',
                     -29999,
                     'stg_ewo_order_id = ' || gv_order_id,-- || ', SOURCE = ' || gv_source,
                     lv_err_desc,
                     'Need to check code');

         gv_fatal := 'Y';
         gv_fail_ind := 'Y';
         --COMMIT;
   END;
--------------PROCEDURE TO LOAD EWO MAIN---------------

   PROCEDURE load_ewo_main
   IS
      lv_gen_obj                    VARCHAR2 (25);
      lv_count1                     NUMBER (9);
      lv_procedure_name             VARCHAR2 (30) := 'LOAD_EWO_MAIN';
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      exit_record                   EXCEPTION;
      stop_load                     EXCEPTION;
      lv_err_desc                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_class_of_err               ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
      lv_error_locator              ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
      lv_src_lookup                 ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
      lv_resolution_reqd            ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
      lv_total_recs                 NUMBER (9) := 0;
      lv_target_recs                NUMBER (9) := 0;
      lv_error_recs                 NUMBER (9) := 0;

      lv_total_recs_dtl             NUMBER (9) := 0;
      lv_target_recs_dtl            NUMBER (9) := 0;
      lv_error_recs_dtl             NUMBER (9) := 0;

      lv_count                      NUMBER;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;
   BEGIN
      gv_total_recs_dtl := 0;
      gv_target_recs_dtl := 0;
      gv_error_recs_dtl := 0;

      gv_total_recs_subdtl := 0;
      gv_target_recs_subdtl := 0;
      gv_error_recs_subdtl := 0;

      --gv_total_recs_ud := 0;
      --gv_target_recs_ud := 0;
      --gv_error_recs_ud := 0;

      initialize_variables;
      init_conv_stat_summary_log (rec_conv_stat_summary_log,lv_procedure_name);-- || '_' || arg_process_id);
      insert_conv_stat_summary_log (rec_conv_stat_summary_log);

      init_conv_stat_summary_log (rec_conv_stat_summary_log,'LOAD_EWO_DETAIL');-- || '_' || arg_process_id);
      insert_conv_stat_summary_log (rec_conv_stat_summary_log);

      --init_conv_stat_summary_log (rec_conv_stat_summary_log,'LOAD_EWO_USER_DATA' || '_' || arg_process_id);
      --insert_conv_stat_summary_log (rec_conv_stat_summary_log);


		-- Main loop starts
      FOR get_rec_ewo IN get_ewo_stg 
      LOOP
         BEGIN
            gv_fatal := 'N';
            gv_fail_ind := 'N';
            lv_total_recs := lv_total_recs + 1;
            grec_ewo := get_rec_ewo;
            gv_first_ckt_si_id := NULL;
            --gv_first_trk_si_id := NULL;
            --gv_trunk_group_design_id := NULL;
            gv_ckt_seq := 0;
            gv_order_id := grec_ewo.stg_ewo_order_id;
            gv_lm_date := NVL (grec_ewo.stg_last_modified_date, SYSDATE);
            gv_lmuid := NVL (grec_ewo.stg_last_modified_userid, gv_lmuid);

            SELECT COUNT (*)
              INTO lv_count
              FROM ZAYOMSS.aref_so_ewo
             WHERE stg_order_id = gv_order_id; 

            IF lv_count > 0
            THEN
               lv_err_desc := 'Duplicate EWO order in ZAYOM6';
               insert_log (
                  lv_procedure_name,
                  'ERROR:Duplicate Order',
                  -22071,
                  'stg_ewo_order_id = ' || gv_order_id ,
                  lv_err_desc,
                  'Only allow new records in staging');
               RAISE exit_record;
            END IF;
			
            load_ewo_order;

            IF gv_fatal = 'Y'
            THEN
               RAISE exit_record;
            END IF;

            IF gv_fail_ind = 'Y'
            THEN
               RAISE exit_record;
            END IF;
			
            load_ewo_detail;

            IF gv_fatal = 'Y'
            THEN
               RAISE exit_record;
            END IF;
			
            IF gv_fail_ind = 'Y'
            THEN
               RAISE exit_record;
            END IF;

            --load_ewo_user_data;

            lv_target_recs := lv_target_recs + 1;
            --COMMIT;
         EXCEPTION
            WHEN exit_record
            THEN
               ROLLBACK;
               lv_error_recs := lv_error_recs + 1;
         END;

         lv_elapsed_minutes :=
            ROUND (TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

         --report progress every 15 minutes
         IF lv_elapsed_minutes > 15
         THEN
            --reset to new report time
            lv_program_last_report_time := SYSDATE;
            --– insert log entry into performance dashboard

            insert_performance_dashboard (lv_procedure_name,
                                          --arg_process_id,
                                          lv_program_start_time,
                                          lv_total_recs,
                                          lv_error_recs,
                                          lv_target_recs);

            --COMMIT;
         END IF;
      END LOOP;

      UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = lv_total_recs,
             error_record_cnt = lv_error_recs,
             target_record_cnt = lv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name-- || '_' || arg_process_id
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');


      UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = gv_total_recs_dtl,
             error_record_cnt = gv_error_recs_dtl,
             target_record_cnt = gv_target_recs_dtl
       WHERE     program_name = gv_package_name
             AND program_function = 'LOAD_EWO_DETAIL' --|| '_' || arg_process_id
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      /*UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = gv_total_recs_ud,
             error_record_cnt = gv_error_recs_ud,
             target_record_cnt = gv_target_recs_ud
       WHERE     program_name = gv_package_name
             AND program_function = 'LOAD_EWO_USER_DATA' || '_' || arg_process_id
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');*/

      --COMMIT;
   EXCEPTION
      WHEN stop_load
      THEN
         ROLLBACK;

         UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = lv_total_recs,
                error_record_cnt = lv_error_recs,
                target_record_cnt = lv_target_recs
          WHERE     program_name = gv_package_name
                AND program_function = lv_procedure_name --|| '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');


         UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = gv_total_recs_dtl,
                error_record_cnt = gv_error_recs_dtl,
                target_record_cnt = gv_target_recs_dtl
          WHERE     program_name = gv_package_name
                AND program_function = 'LOAD_EWO_DETAIL' --|| '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

         /*UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = gv_total_recs_ud,
                error_record_cnt = gv_error_recs_ud,
                target_record_cnt = gv_target_recs_ud
          WHERE     program_name = gv_package_name
                AND program_function = 'LOAD_EWO_USER_DATA' || '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');*/

         --COMMIT;
      WHEN gv_fatal_exception
      THEN
         NULL;
      WHEN OTHERS
      THEN
         ROLLBACK;
         lv_error_recs := lv_error_recs + 1;

         lv_err_desc := SQLERRM;

         insert_log (lv_procedure_name,
                     'FATAL ERROR',
                     -29999,
                     'stg_ewo_order_id = ' || gv_order_id,-- || ', SOURCE = ' || gv_source,
                     lv_err_desc,
                     'Need to check code');

         UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = lv_total_recs,
                error_record_cnt = lv_error_recs,
                target_record_cnt = lv_target_recs
          WHERE     program_name = gv_package_name
                AND program_function = lv_procedure_name --|| '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');


         UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = gv_total_recs_dtl,
                error_record_cnt = gv_error_recs_dtl,
                target_record_cnt = gv_target_recs_dtl
          WHERE     program_name = gv_package_name
                AND program_function = 'LOAD_EWO_DETAIL' --|| '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

       /*  UPDATE ZAYOMSS.conv_stat_summary_log
            SET program_end_time = SYSDATE,
                source_record_cnt = gv_total_recs_ud,
                error_record_cnt = gv_error_recs_ud,
                target_record_cnt = gv_target_recs_ud
          WHERE     program_name = gv_package_name
                AND program_function = 'LOAD_EWO_USER_DATA' || '_' || arg_process_id
                AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');*/


         --COMMIT;
   END;
END;