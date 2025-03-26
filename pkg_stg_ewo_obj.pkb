CREATE OR REPLACE PACKAGE BODY ZAYOMSS.PKG_STG_EWO_OBJ
AS
   ----------------------------------------------------------------------------
	--  $Id: pkg_stg_ewo_obj.pkb $
	--  $Revision: $
	--  $Date: $
	--
	--  Created by		:  Pradeep kumar D 
	--
	--  Creation date	: 24-DEC-2024
	
	--  Usage: This package is used to load data into EWO staging tables
	--
	--  Modification Log
	--  Modifier          Date        		Description
   ----------------------------------------------------------------------------------------------------------
   --  Pradeep kumar D    24/12/2024        Creation of Stg script to load EWO data in stg tables
   -----------------------------------------------------------------------------------------------------------
PROCEDURE insert_log (
      arg_program_function      IN zayomss.conv_conversion_error_log.program_function%TYPE,
      arg_class_of_err          IN zayomss.conv_conversion_error_log.class_of_error%TYPE,
      arg_err_loc_id            IN zayomss.conv_conversion_error_log.error_locator_id%TYPE,
      arg_src_data_key_lookup   IN zayomss.conv_conversion_error_log.src_data_key_lookup%TYPE,
      arg_err_desc              IN zayomss.conv_conversion_error_log.error_description%TYPE,
      arg_resolution_required   IN zayomss.conv_conversion_error_log.resolution_required%TYPE)
   IS
      -- declare variables
      --error description
      lv_err_desc              zayomss.conv_conversion_error_log.error_description%TYPE
                                  := NULL;
      lv_src_data_key_lookup   zayomss.conv_conversion_error_log.src_data_key_lookup%TYPE;
   BEGIN
      IF arg_src_data_key_lookup IS NULL
      THEN
         lv_src_data_key_lookup := 'NO LOOKUP';
      ELSE
         lv_src_data_key_lookup := arg_src_data_key_lookup;
      END IF;


      INSERT INTO zayomss.conv_conversion_error_log (program_name,
                                                     program_function,
                                                     program_run_date,
                                                     class_of_error,
                                                     error_locator_id,
                                                     src_data_key_lookup,
                                                     error_description,
                                                     resolution_required)
           VALUES (gv_package_name,                            -- program_name
                   arg_program_function,                   		-- program_function
                   TO_DATE (TO_CHAR (gv_run_dt), 'YYYYMMDD'),	-- program_run_date
                   arg_class_of_err,                         	-- class_of_error
                   arg_err_loc_id,                         		-- error_locator_id
                   arg_src_data_key_lookup,             		-- src_data_key_lookup
                   arg_err_desc,                          		-- error_description
                   arg_resolution_required);            		-- resolution_required
   EXCEPTION
      WHEN OTHERS
      THEN
         lv_err_desc :=
               'FATAL: unable to insert log for '
            || 'program_name='
            || gv_package_name
            || 'program_function='
            || arg_program_function
            || 'error_message='
            || arg_err_desc;
         raise_application_error (gv_fatal_exception_num,          		--error num
                                                         lv_err_desc); 	--error description
   END;                                                       			--end insert_log

   ------------------------------------------------------------------
   --
   -- Usage: Insert value into the  zayomss.conv_stat_summary_log table
   --
   -- Input: arg_zayomss.conv_stat_summary_log ~ summary information
   --
   -- Output: NONE
   --
   -- Errors: Raises fatal exception if the entry cannot be inserted
   --
   ---------------------------------------------------------------------------
   PROCEDURE insert_conv_stat_summary_log (
      arg_conv_stat_summary_log IN zayomss.conv_stat_summary_log%ROWTYPE)
   IS
      --declare variables
      --error description
      lv_err_desc   zayomss.conv_conversion_error_log.error_description%TYPE
                       := NULL;
   BEGIN
      INSERT INTO zayomss.conv_stat_summary_log (program_name,
                                                 program_function,
                                                 program_run_date,
                                                 program_start_time,
                                                 program_end_time,
                                                 source_record_cnt,
                                                 error_record_cnt,
                                                 target_record_cnt)
           VALUES (arg_conv_stat_summary_log.program_name,     			-- program_name
                   arg_conv_stat_summary_log.program_function, 			-- program_function
                   arg_conv_stat_summary_log.program_run_date, 			-- program_run_date
                   arg_conv_stat_summary_log.program_start_time, 		-- program_start_time
                   arg_conv_stat_summary_log.program_end_time, 			-- program_end_time
                   arg_conv_stat_summary_log.source_record_cnt, 		-- source_record_cnt
                   arg_conv_stat_summary_log.error_record_cnt, 			-- error_record_cnt
                   arg_conv_stat_summary_log.target_record_cnt); 		-- target_record_cnt
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

         raise_application_error (gv_fatal_exception_num,          			--error num
                                                         lv_err_desc); 		--error description
   END;                                     								--end insert_conv_stat_summary_log

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
      --declare variables
      lv_procedure_name   zayomss.conv_conversion_error_log.program_function%TYPE
         := 'initialize_variables';
      lv_err_mesg         zayomss.conv_conversion_error_log.error_description%TYPE;
   BEGIN
      -- to get the last modified user id
      BEGIN
         SELECT value_text
           INTO gv_lmuid
           FROM zayomss.conv_global_variable
          WHERE label_name = 'LMUID';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_mesg := 'FATAL: NO_DATA_FOUND for global variable';

            insert_log (
               lv_procedure_name,                      				-- arg_program_function
               'missing global variable: LMUID',        			-- arg_class_of_err
               -29999,                                       		-- arg_err_loc_id
               'Error getting global variable LMUID', 				-- arg_src_data_key_lookup
               lv_err_mesg,                                    		-- arg_err_desc
                  'add entry for lmuid in '
               ||                                  					-- arg_resolution_required
                 'conv_global_variable then run the program again');

            RAISE gv_fatal_exception;
      END;                                               			-- end begin gv_lmuid



      -- to get the global run date
      BEGIN
         SELECT value_number
           INTO gv_run_dt
           FROM zayomss.conv_global_variable
          WHERE label_name = 'RUN_DATE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_mesg := 'FATAL: NO_DATA_FOUND for global variable';
            insert_log (
               lv_procedure_name,                      				-- arg_program_function
               'missing global variable: run_date',        			-- arg_class_of_err
               -29999,                                       		-- arg_err_loc_id
               'Error getting Global variable gv_run_date', 		-- arg_src_data_key_lookup
               lv_err_mesg,                                    		-- arg_err_desc
                  'add entry for run_date in '
               ||                                   				-- arg_resolution_required
                 'conv_global_variable then run the program again');
            RAISE gv_fatal_exception;
      END;                                                 			-- end of gv_run_dt



      -- to get the global release number
      BEGIN
         SELECT value_text
           INTO gv_rel_num
           FROM zayomss.conv_global_variable
          WHERE label_name = 'CONV_REL_NUM';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_err_mesg := 'FATAL: NO_DATA_FOUND for release variable';
            insert_log (
               lv_procedure_name,                      					-- arg_program_function
               'missing global variable: conv_rel_num',    				-- arg_class_of_err
               -29999,                                       			-- arg_err_loc_id
               'Error getting the global variable gv_rel_num', 			-- arg_src_data_key_lookup
               lv_err_mesg,                                    			-- arg_err_desc
                  'add entry for conv_rel_num in '
               ||                                   					-- arg_resolution_required
                 'conv_global_variable then run the program again');
            RAISE gv_fatal_exception;
      END;                                                				-- end of gv_rel_num
   END initialize_variables;

   -----------------------------------------------------------------------------
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
   -----------------------------------------------------------------------------
   PROCEDURE init_conv_stat_summary_log (
      arg_conv_stat_summary_log   IN OUT zayomss.conv_stat_summary_log%ROWTYPE,
      arg_procedure_name          IN     zayomss.conv_stat_summary_log.program_function%TYPE)
   IS
   BEGIN
      arg_conv_stat_summary_log.program_name := gv_package_name;
      arg_conv_stat_summary_log.program_function := arg_procedure_name;
      arg_conv_stat_summary_log.program_run_date :=
         TO_DATE (gv_run_dt, 'YYYYMMDD');
      arg_conv_stat_summary_log.program_start_time := SYSDATE;
      arg_conv_stat_summary_log.program_end_time := SYSDATE;
      arg_conv_stat_summary_log.source_record_cnt := 0;
      arg_conv_stat_summary_log.error_record_cnt := 0;
      arg_conv_stat_summary_log.target_record_cnt := 0;
   END;                                       --end init_conv_stat_summary_log

   -----------------------------------------------------------------------------
   --
   -- Usage: Log insert into the zayomss.insert_performance_dashboard table every 15 minutes
   --
   -- Input:  arg_program_function    ~ calling function
   --         arg_program_thread_id   ~ Parallel process thread id
   --         arg_program_start_time  ~ Program start time
   --         arg_total_count           ~ Total count selected from source
   --         arg_error_count         ~ Total error count
   --         arg_target_count           ~ Total target count
   --
   -- Output:  NONE
   --
   --
   -- Error raises gv_FATAL_EXCEPTION if a record cannot be inserted
   --
   --
   -----------------------------------------------------------------------------

   PROCEDURE insert_performance_dashboard (
      arg_program_function     IN zayomss.conv_performance_dashboard.program_function%TYPE,
      arg_program_thread_id    IN zayomss.conv_performance_dashboard.program_thread_id%TYPE,
      arg_program_start_time   IN zayomss.conv_performance_dashboard.program_start_time%TYPE,
      arg_total_count          IN zayomss.conv_performance_dashboard.program_records_processed%TYPE,
      arg_error_count          IN zayomss.conv_performance_dashboard.error_record_cnt%TYPE,
      arg_target_count         IN zayomss.conv_performance_dashboard.target_record_cnt%TYPE)
   IS
      --error description
      lv_err_desc   zayomss.conv_conversion_error_log.error_description%TYPE
                       := NULL;
   BEGIN
      INSERT
        INTO zayomss.conv_performance_dashboard (program_name,
                                                 program_function,
                                                 program_thread_id,
                                                 program_run_date,
                                                 program_start_time,
                                                 program_status_time,
                                                 program_records_processed,
                                                 error_record_cnt,
                                                 target_record_cnt)
      VALUES (gv_package_name,                                 	--program_name,
              arg_program_function,                        		--program_function,
              null,	--arg_program_thread_id,                    --program_thread_id,
              TO_DATE (gv_run_dt, 'YYYYMMDD'),             		--program_run_date,
              arg_program_start_time,                    		--program_start_time,
              SYSDATE,                                   		--program_status_time
              arg_total_count,                    				--program_records_processed,
              arg_error_count,                             		--error_record_cnt,
              arg_target_count                            		--target_record_cnt)
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
         raise_application_error (gv_fatal_exception_num,          		--error num
                                                         lv_err_desc); 	--error description
   END;                                                       			--end insert_log

   ----------------------------------------------------------------------------
    --  Usage: Main program to call the sub routines for consolidation of EWO
	--
   --  input: None
   --
   --
   --  output: NONE
   --
   --  Error: FATAL_EXCEPTION will be thrown if the
   -----------------------------------------------------------------------------
   
PROCEDURE stg_load_ewomain 
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
	  --INITIALIZE LOCAL VARIABLES
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE:= 'STG_LOAD_EWOMAIN';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;
		
	--CURSOR TO RETRIVE EWO SEED ORDERS
      CURSOR cur_stg_ewo_main 
	  IS
			SELECT 	sr.desired_due_date,
							sr.document_number,
							sr.last_modified_date,
							sr.last_modified_userid,
							sr.order_number,
							sr.project_identification,
							sr.responsible_party,
							sr.service_request_status,
							sr.order_compl_dt,
							(SELECT org.organization_name FROM asap.organization@zayodev_dblink org 
							WHERE org.organization_id = sr.organization_id) org_name,
							(SELECT rm.remark FROM asap.remark@zayodev_dblink rm WHERE 
							rm.document_number = sr.document_number AND rm.form_id = 'EWO' AND ROWNUM <= 1) remark,
							sr.activity_ind,
							sr.pon,
							sr.gmt_dt_tm_received,
							sr.supplement_type,
							sr.first_ecckt_id
						FROM 
							asap.serv_req@zayodev_dblink sr
						WHERE 
							
							 type_of_Sr='EWO' 
							AND order_compl_dt IS NOT NULL
							AND service_request_status >= 801
							AND (supplement_type <> '1' or supplement_type IS NULL);
							
		
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM ZAYOMSS.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;
  
      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,lv_procedure_name);
         
		 --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;
  
      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;
  
      FOR indx IN cur_stg_ewo_main
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
           
			-- INSERT CURSOR ENTRIES INTO STAGING EWO MAIN TABLE
            INSERT INTO ZAYOMSS.STG_EWO_MAIN (stg_desired_due_date,
                                            stg_ewo_order_id,
                                            stg_last_modified_date,
                                            stg_last_modified_userid,
                                            stg_order_number,
                                            stg_project_identification,
                                            stg_responsible_party,
                                            stg_service_request_status,
                                            stg_order_compl_dt,
                                            stg_org_name,
                                            stg_remark,
                                            stg_activity_ind,
                                            stg_pon,
                                            stg_gmt_dt_tm_received,
                                            stg_supplement_type,
											stg_first_ecckt_id)
                 VALUES (indx.desired_due_date,
                         indx.document_number,
                         indx.last_modified_date,
                         indx.last_modified_userid,
                         indx.order_number,
                         indx.project_identification,
                         indx.responsible_party,
                         indx.service_request_status,
                         indx.order_compl_dt,
                         indx.org_name,
                         indx.remark,
                         indx.activity_ind,
                         indx.pon,
                         indx.gmt_dt_tm_received,
                         indx.supplement_type,
						 indx.first_ecckt_id);
  
  
            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
  
            gvcommitctr := gvcommitctr + 1;
  
            IF gvcommitctr >= 100
            THEN
               
                COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes := ROUND (TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', ORDER_NUMBER = '
                  || indx.order_number,             -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;

		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      COMMIT;
   END;
   --------------------PROCEDURE TO LOAD EWO RELATED CIRCUIT------------------------------------------------

 PROCEDURE stg_load_ewocir
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
	  --INITIALIZE LOCAL VARIABLES
	  lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE := 'STG_LOAD_EWOCIR';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_stg_serv_item_desc         VARCHAR2 (256);
	  lv_stg_serv_item_id           NUMBER(9);
      lv_stg_si_from_eff_dt         DATE;
      lv_stg_si_status              CHAR (1);
      lv_stg_si_to_eff_dt           DATE;
      lv_stg_si_activity_ind        CHAR (1);
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

	  --CURSOR TO GET THE CIRCUIT DETAILS USING EWO ORDER ID.
      CURSOR cur_stg_ewo_cir
      IS
	  SELECT 		cr.circuit_design_id,
							cr.ecckt_type,
							cr.exchange_carrier_circuit_id,
							cr.status AS cir_status,
							cr.TYPE,
							src.last_modified_userid,
							src.last_modified_date,
							src.document_number,
							src.circuit_activity_ind
					FROM asap.circuit@zayodev_dblink cr,ZAYOMSS.STG_EWO_MAIN sse,
                        asap.service_request_circuit@zayodev_dblink src
                    WHERE     cr.circuit_design_id = src.circuit_design_id
                        AND src.document_number =  sse.stg_ewo_order_id;
						
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM ZAYOMSS.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;

      FOR indx IN cur_stg_ewo_cir
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
            
			BEGIN
						--get serv_item_status and activity_ind from serv_req_si
							SELECT 	si.serv_item_desc,
									si.serv_item_id,
									si.from_effective_date,
									si.status,
									si.to_eff_dt,
									sri.activity_cd
								INTO 	lv_stg_serv_item_desc,
										lv_stg_serv_item_id,
										lv_stg_si_from_eff_dt,
										lv_stg_si_status,
										lv_stg_si_to_eff_dt,
										lv_stg_si_activity_ind
								FROM asap.serv_item@zayodev_dblink si, asap.serv_req_si@zayodev_dblink sri
							WHERE     si.serv_item_id = sri.serv_item_id
									AND sri.document_number = indx.document_number
									AND si.circuit_design_id = indx.circuit_design_id;
						
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;
			-- INSERT CIRCUIT CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
            INSERT INTO ZAYOMSS.STG_EWO_DETAIL (gen_object_case,
                                              stg_leg_circuit_design_id,
                                              stg_leg_ckt_ecckt_type,
                                              stg_leg_exch_carrier_ckt_id,
                                              stg_leg_ckt_status,
                                              stg_leg_ckt_type,
                                              stg_last_modified_userid,
                                              stg_last_modified_date,
                                              stg_ewo_order_id,
                                              stg_circuit_activity_ind,
                                              stg_serv_item_desc,
                                              stg_si_from_eff_dt,
                                              stg_si_status,
                                              stg_si_to_eff_dt,
                                              stg_si_activity_ind,
											  stg_serv_item_id)
            VALUES ('CIRCUIT',
                    indx.circuit_design_id,
                    indx.ecckt_type,
                    indx.exchange_carrier_circuit_id,
                    --indx.trunk_ecckt,
                    indx.cir_status,
                    indx.TYPE,
                    indx.last_modified_userid,
                    indx.last_modified_date,
                    indx.document_number,
                    indx.circuit_activity_ind,
                    lv_stg_serv_item_desc,
                    lv_stg_si_from_eff_dt,
                    lv_stg_si_status,
                    lv_stg_si_to_eff_dt,
                    lv_stg_si_activity_ind,
					lv_stg_serv_item_id);


            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;


            lv_elapsed_minutes := ROUND (TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,              					-- ARG_PROGRAM_FUNCTION
                  'ERROR:',                       					-- ARG_CLASS_OF_ERR
                  -20145,                         					-- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '					
                  || indx.document_number					
                  || ', CIRCUIT_DESIGN_ID = '					
                  || indx.circuit_design_id,      					-- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                    					-- ARG_ERR_DESC
                  'EXCEPTION'                     					-- ARG_RESOLUTION_REQUIRED
                             );					
               gv_error_recs := gv_error_recs + 1;					--THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;
		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      COMMIT;
   END;
-------------------------PROCEDURE TO LOAD EWO RELATED EQUIPMENTS-----------------------------------------------
   PROCEDURE stg_load_ewoequip 
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      --LOCAL VARIABLES
	  lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE := 'STG_LOAD_EWOEQUIP';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

		--CURSOR TO GET THE EQUIPMENT DETAILS USING EWO ORDER ID.
      CURSOR cur_stg_ewo_equip
      IS
         SELECT eq.equipment_id,
					eq.equipment_name,
					(SELECT clli_code
						FROM asap.network_location@zayodev_dblink
						WHERE location_id = eq.location_id) AS priloc,
					(SELECT clli_code
						FROM asap.network_location@zayodev_dblink
						WHERE location_id = eq.location_id_2) AS secloc,
					sie.last_modified_userid,
					sie.last_modified_date,
					sri.document_number,
					si.serv_item_id,
					si.serv_item_desc,
					si.from_effective_date,
					si.status,
					si.to_eff_dt,
					sri.activity_cd
				FROM  asap.serv_req_si@zayodev_dblink sri,
                      asap.si_equipment@zayodev_dblink sie,
                      asap.equipment@zayodev_dblink eq,
                      asap.serv_item@zayodev_dblink si,
					  ZAYOMSS.STG_EWO_MAIN sse
                WHERE     
                          sri.serv_item_id = sie.serv_item_id
                      AND sie.equipment_id = eq.equipment_id
                      AND sri.serv_item_id = si.serv_item_id
                      AND si.serv_item_id = sie.serv_item_id
					  AND sse.stg_ewo_order_id = sri.document_number;				--arg_document_number;
					 
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM ZAYOMSS.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;


      FOR indx IN cur_stg_ewo_equip
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
           
			-- INSERT EQUIPMENT CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
            INSERT INTO ZAYOMSS.STG_EWO_DETAIL (   gen_object_case,
                                                   stg_leg_equipment_id,
                                                   stg_equipment_name,
                                                   stg_priloc_location,
                                                   stg_secloc_location,
                                                   stg_last_modified_userid,
                                                   stg_last_modified_date,
                                                   stg_ewo_order_id,
                                                   stg_serv_item_desc,
                                                   stg_si_from_eff_dt,
                                                   stg_si_status,
                                                   stg_si_to_eff_dt,
                                                   stg_si_activity_ind,
												   stg_serv_item_id)
                 VALUES ('EQUIPMENT',
                         indx.equipment_id,
                         indx.equipment_name,
                         indx.priloc,
                         indx.secloc,
                         indx.last_modified_userid,
                         indx.last_modified_date,
                         indx.document_number,
                         indx.serv_item_desc,
                         indx.from_effective_date,
                         indx.status,
                         indx.to_eff_dt,
                         indx.activity_cd,
						 indx.serv_item_id);

            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes :=
               ROUND (
                  TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', EQUIPMENT_ID = '
                  || indx.equipment_id,             -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;

		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      COMMIT;
   END;
-------------------------PROCEDURE TO LOAD EWO RELATED NOTES-------------------------------------------
PROCEDURE stg_load_ewonotes 
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
	  --INITIALIZE LOCAL VARIABLES
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE:= 'STG_LOAD_EWONOTES';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

		--CURSOR TO GET THE NOTES DETAILS OF EWO SEEDED ORDER ID.
      CURSOR cur_stg_ewo_notes
      IS
         SELECT 		nt.notes_id,
						nt.note_text,						
                        nt.notes_sequence,
                        nt.last_modified_userid,
                        nt.last_modified_date,
                        nt.document_number,
                        nt.circuit_design_id,
                        nt.circuit_note_ind,
                        nt.exchange_carrier_circuit_id,
                        nt.date_entered,
                        nt.user_id,
                        nt.system_gen_ind
                   FROM asap.notes@zayodev_dblink nt,ZAYOMSS.STG_EWO_MAIN sse
                  WHERE   nt.document_number =sse.stg_ewo_order_id ;	-- arg_document_number;
				  
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM ZAYOMSS.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;

      FOR indx IN cur_stg_ewo_notes
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
           
			-- INSERT NOTES CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
            INSERT INTO ZAYOMSS.STG_EWO_DETAIL (gen_object_case,
                                              stg_notes_id,
                                              stg_note_text,
                                              stg_notes_sequence,
                                              stg_last_modified_userid,
                                              stg_last_modified_date,
                                              stg_ewo_order_id,
                                              stg_leg_circuit_design_id,
                                              stg_circuit_note_ind,
                                              stg_leg_exch_carrier_ckt_id,
                                              stg_note_date_entered,
                                              stg_note_userid,
                                              stg_system_gen_ind)
            VALUES ('NOTES',
                    indx.notes_id,
                    indx.note_text,
                    indx.notes_sequence,
                    indx.last_modified_userid,
                    indx.last_modified_date,
                    indx.document_number,
                    indx.circuit_design_id,
                    indx.circuit_note_ind,
                    indx.exchange_carrier_circuit_id,
                    indx.date_entered,
                    indx.user_id,
                    indx.system_gen_ind);

            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes :=
               ROUND (
                  TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', Notes_sequence = '
                  || indx.notes_sequence,           -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;

		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      COMMIT;
   END;

-------------------PROCEDURE TO LOAD EWO RELATED MS_ATTACHMENT_LINK---------------------
PROCEDURE stg_load_ms_link 
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
	  --INITIALIZE LOCAL VARIABLES
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE := 'STG_LOAD_MS_LINK';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

		--CURSOR TO GET THE MS_ATTACHMENT_LINK DETAILS OF EWO SEEDED ORDER ID.
      CURSOR cur_stg_ewo_link
      IS 
	   SELECT creation_date,
                        ms_attachment_nm,
                        ms_table_key_id document_number,
                        ms_table_key_value,
                        ms_table_nm,
                        transform_id,
                        url_desc,
                        url,
                        ms.last_modified_userid,
                        ms.last_modified_date
                   FROM ZAYOMSS.STG_MS_ATTACHMENT_LINK ms ,ZAYOMSS.STG_EWO_MAIN sse        -- FROM asap.Z_ms_attachment_link@zayodev_dblink ms               
                  WHERE     ms.ms_table_key_id = sse.stg_ewo_order_id; 					--arg_document_number;
					 
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM zayomss.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;


      FOR indx IN cur_stg_ewo_link
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
            
			-- INSERT MS_ATTACHMENT_LINK CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
            INSERT INTO ZAYOMSS.STG_EWO_DETAIL (gen_object_case,
                                                   stg_ewo_order_id,
                                                   stg_creation_date,
                                                   stg_ms_attachment_nm,
                                                   stg_ms_table_key_value,
                                                   stg_ms_table_nm,
                                                   stg_transform_id,
                                                   stg_url_desc,
                                                   stg_url,
                                                   stg_last_modified_userid,
                                                   stg_last_modified_date)
                 VALUES ('ATTACHMENT',
                         indx.document_number,
                         indx.creation_date,
                         indx.ms_attachment_nm,
                         indx.ms_table_key_value,
                         indx.ms_table_nm,
                         indx.transform_id,
                         indx.url_desc,
                         indx.url,
                         indx.last_modified_userid,
                         indx.last_modified_date);

            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes :=
               ROUND (
                  TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);

               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', URL_DESC = '
                  || indx.url_desc,                 -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );

               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;
		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

      COMMIT;
   END;
   -----------------PROCEDURE TO LOAD EWO USER DATA-------------------
  /* PROCEDURE stg_load_ewo_udf (p_process_id IN NUMBER,arg_document_number IN NUMBER)
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE
         := 'STG_LOAD_EWO_UDF';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      vcnt                          NUMBER := 0;
      vuserdatavalueid              NUMBER (10) := 0;
      vdisplayvalue                 VARCHAR2 (2000) := NULL;
      vuserdatacolumn               VARCHAR2 (2000) := NULL;
      gverrctr                      NUMBER := 0;
      v_user_data_category          asap.user_data_column.user_data_category%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;
   
   
      CURSOR cur_stg_ewo_udf
      IS
         SELECT *
				FROM asap.ewo_user_data@zayodev_dblink eud                        
          WHERE   eud.document_number = arg_document_number
               AND MOD (arg_document_number, 1) = p_process_id;
               
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM zayomss.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;
   
      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;
   
      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;
   
   
   
      FOR indx IN cur_stg_ewo_udf
      LOOP
         gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
         --vuserdatacolumn := indx.value_name;
   
         BEGIN
            IF indx.document_number IS NULL    --IF indx.value_value IS NULL
            THEN
               lv_err_mesg :=
                  'EWO USER DATA VALUE_NAME IS NULL SO NO NEED TO STAGE';
               insert_log (
                  lv_procedure_name,                   -- arg_program_function
                  'ERROR:NO DATA',                         -- arg_class_of_err
                  -20145,                                    -- arg_err_loc_id
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', VALUE_NAME = ',
                  --|| indx.value_name,                           --remove            --
                  -- arg_src_data_key_lookup
                  lv_err_mesg,                                 -- arg_err_desc
                  'Only allow valid records to be staged'-- arg_resolution_required
                  );
               gv_error_recs := gv_error_recs + 1;
               COMMIT;
          --  ELSIF indx.value_value IS NOT NULL			
          --  THEN
          --     BEGIN
          --        SELECT user_data_category
          --          INTO v_user_data_category
          --          FROM user_data_column
          --         WHERE     column_style = 'DROPDOWN'
          --               AND table_name = 'EWO_USER_DATA'
          --               AND column_name = indx.value_name;
		  --
          --        vcnt := 1;
          --     EXCEPTION
          --        WHEN OTHERS
          --        THEN
          --           vcnt := 0;
          --     END;
		  --
          --     IF vcnt > 0
          --     THEN
          --       
          --           BEGIN
          --              SELECT udc.user_data_category_value_id,
          --                     udc.display_value
          --                INTO vuserdatavalueid, vdisplayvalue
          --                FROM user_data_category_values@zayodev_dblink udcl,
          --                     user_data_category_values udc
          --               WHERE     udcl.display_value = udc.display_value
          --                     AND udcl.user_data_category_value_id =
          --                            indx.value_value
          --                     AND udc.user_data_category =
          --                            v_user_data_category;
          --           EXCEPTION
          --              WHEN OTHERS
          --              THEN
          --                 vuserdatavalueid := NULL;
          --                 vdisplayvalue := indx.document_number;	--displayvalue := indx.value_value;
          --           END;
          --        
          --        
          --     ELSE
          --        vuserdatavalueid := NULL;
          --        vdisplayvalue := indx.document_number;	--displayvalue := indx.value_value;
               END IF;
   
               --gv_genseq := gv_genseq + 1;    --THIS IS GENERATE THE SEQ VALUE
   
               INSERT
                 INTO ZAYOMSS.STG_EWO_USER_DATA (--gen_object_case,
                                                 stg_ewo_document_number,
                                                 stg_ewo_order_type
                                                 --stg_ewo_value_name,
                                                 --stg_ewo_value_instance_id,
                                                 --stg_ewo_value_value
												 )
               VALUES (--'EWO_USER_DATA',
                       indx.document_number,
                       'UDD'
                       --INDX.VALUE_NAME,
                       --vuserdatacolumn,
                       --vuserdatavalueid,
                       --vdisplayvalue
					   );
   
               gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
   
               gvcommitctr := gvcommitctr + 1;
   
               IF gvcommitctr >= 100
               THEN
                  COMMIT;
                  gvcommitctr := 0;
               END IF;
   
               lv_elapsed_minutes :=
                  ROUND (
                     TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);
   
               --REPORT PROGRESS EVERY 15 MINUTES
               IF lv_elapsed_minutes > 15
               THEN
                  --RESET TO NEW REPORT TIME
                  lv_program_last_report_time := SYSDATE;
                  --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD
   
                  insert_performance_dashboard (lv_procedure_name,
                                                p_process_id,
                                                lv_program_start_time,
                                                gv_total_recs,
                                                gv_error_recs,
                                                gv_target_recs);
                  COMMIT;
               END IF;
            --END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', VALUE_NAME = ',
                  --|| indx.value_name,               -- ARG_SRC_DATA_KEY_LOOKUP--remove
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;
   
   
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');
   
      COMMIT;
   END;*/
   ----------------PROCEDURE TO LOAD EWO RELATED SR_RELATIONSHIP-----------------
   
   PROCEDURE stg_load_eworel
   AS
      rec_conv_stat_summary_log     zayomss.conv_stat_summary_log%ROWTYPE;
	  --INITIALIZE LOCAL VARIABLES
      lv_procedure_name             zayomss.conv_conversion_error_log.program_function%TYPE := 'STG_LOAD_EWOREL';
      lv_err_mesg                   zayomss.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

		--CURSOR TO GET THE SR_RELATIONSHIP DETAILS OF EWO SEEDED ORDER ID.
      CURSOR cur_stg_ewo_rel
      IS
         SELECT srel.active_ind,
                        srel.create_date,
                        srel.create_userid,
                        srel.document_number,
                        srel.document_number_related,
                        srel.sr_relation_seq,
                        srel.sr_relation_type_cd,
                        srel.last_modified_userid,
                        srel.last_modified_date
                   FROM asap.sr_relationship@zayodev_dblink srel,zayomss.stg_ewo_main sse
                  WHERE     srel.document_number_related = sse.stg_ewo_order_id;						--arg_document_number;
					
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM zayomss.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;


      FOR indx IN cur_stg_ewo_rel
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
           

           -- INSERT SR_RELATIONSHIP CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
		   INSERT INTO ZAYOMSS.STG_EWO_DETAIL (gen_object_case,
                                              stg_ewo_parent_active_ind,
                                              stg_ewo_order_parent_id,
                                              stg_ewo_order_id,
                                              stg_ewo_parent_rel_type_cd,
                                              stg_last_modified_userid,
                                              stg_last_modified_date,
                                              stg_sr_relation_seq,
                                              stg_create_user_id,
                                              stg_creation_date)
            VALUES ('SR_RELATION',
                    indx.active_ind,
                    indx.document_number,
                    indx.document_number_related,
                    indx.sr_relation_type_cd,
                    indx.last_modified_userid,
                    indx.last_modified_date,
                    indx.sr_relation_seq,
                    indx.create_userid,
                    indx.create_date);

            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes :=
               ROUND (
                  TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', DOCUMENT_NUMBER_RELATED = '
                  || indx.document_number_related,  -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;
		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');


      COMMIT;
   END;
----------------PROCEDURE TO LOAD EWO RELATED SUPPLEMENTRY HISTORY-----------------
   PROCEDURE stg_load_ewo_history 
   AS
      rec_conv_stat_summary_log     ZAYOMSS.conv_stat_summary_log%ROWTYPE;
      lv_procedure_name             ZAYOMSS.conv_conversion_error_log.program_function%TYPE := 'STG_LOAD_EWO_HISTORY';
      lv_err_mesg                   ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
      lv_program_last_report_time   DATE := SYSDATE;
      lv_program_start_time         DATE := SYSDATE;
      lv_elapsed_minutes            NUMBER;

      --CURSOR TO GET THE SUPPLEMENTRY HISTORY DETAILS OF EWO SEEDED ORDER ID.
	  CURSOR cur_stg_ewo_his
      IS
	  SELECT srh.document_number,
                        srh.supp_cancel_reason,
                        srh.supp_note,
                        srh.supplement_type,
                        srh.ver_ident_scheme,
                        srh.version_identification,
                        srh.last_modified_userid,
                        srh.last_modified_date
                   FROM asap.sr_supp_history@zayodev_dblink srh,zayomss.stg_ewo_main sse
                  WHERE     srh.document_number = sse.stg_ewo_order_id;						--arg_document_number;
			      
   BEGIN
      SELECT COUNT (1)
        INTO gv_prc_cnt
        FROM ZAYOMSS.conv_stat_summary_log
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name;

      IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;

      gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;


      FOR indx IN cur_stg_ewo_his
      LOOP
         BEGIN
            gv_total_recs := gv_total_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS THAT WE ARE GOING TO PROCESS
            
			-- INSERT SUPPLEMENTRY HISTORY CURSOR ENTRIES INTO STAGING EWO_DETAIL TABLE
            INSERT INTO ZAYOMSS.STG_EWO_DETAIL (gen_object_case,
                                              stg_ewo_order_id,
                                              stg_supp_cancel_reason,
                                              stg_supp_note,
                                              stg_supplement_type,
                                              stg_ver_ident_scheme,
                                              stg_version_identification,
                                              stg_last_modified_userid,
                                              stg_last_modified_date)
            VALUES ('SUPP_HISTORY',
                    indx.document_number,
                    indx.supp_cancel_reason,
                    indx.supp_note,
                    indx.supplement_type,
                    indx.ver_ident_scheme,
                    indx.version_identification,
                    indx.last_modified_userid,
                    indx.last_modified_date);

            gv_target_recs := gv_target_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER OF RECORDS PROCESSED SUCESSFULLY.
            gvcommitctr := gvcommitctr + 1;

            IF gvcommitctr >= 100
            THEN
               COMMIT;
               gvcommitctr := 0;
            END IF;

            lv_elapsed_minutes :=
               ROUND (
                  TO_NUMBER (SYSDATE - lv_program_last_report_time) * 1440);

            --REPORT PROGRESS EVERY 15 MINUTES
            IF lv_elapsed_minutes > 15
            THEN
               --RESET TO NEW REPORT TIME
               lv_program_last_report_time := SYSDATE;
               --– INSERT LOG ENTRY INTO PERFORMANCE DASHBOARD

               insert_performance_dashboard (lv_procedure_name,
                                             null,--p_process_id,
                                             lv_program_start_time,
                                             gv_total_recs,
                                             gv_error_recs,
                                             gv_target_recs);
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_err_mesg := SUBSTR (SQLERRM, 1, 100);
               insert_log (
                  lv_procedure_name,                   -- ARG_PROGRAM_FUNCTION
                  'ERROR:',                                -- ARG_CLASS_OF_ERR
                  -20145,                                    -- ARG_ERR_LOC_ID
                     'DOCUMENT_NUMBER = '
                  || indx.document_number
                  || ', VERSION_IDENTIFICATION = '
                  || indx.version_identification,   -- ARG_SRC_DATA_KEY_LOOKUP
                  lv_err_mesg,                                 -- ARG_ERR_DESC
                  'EXCEPTION'                       -- ARG_RESOLUTION_REQUIRED
                             );
               gv_error_recs := gv_error_recs + 1; --THIS IS TO FIND THE TOTAL NUMBER ERROR RECORDS
         END;
      END LOOP;

		-- Updating the statistics of EWO procedure.
      UPDATE zayomss.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE     program_name = gv_package_name
             AND program_function = lv_procedure_name
             AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');


      COMMIT;
   END;
   ----------------PROCEDURE TO LOAD EWO MAIN--------------------------
   PROCEDURE STG_EWO_MAIN
   AS
 
      --declare variables
   rec_conv_stat_summary_log   ZAYOMSS.conv_stat_summary_log%ROWTYPE;
   lv_procedure_name           ZAYOMSS.conv_conversion_error_log.program_function%TYPE := 'STG_EWO_MAIN';       
   lv_err_mesg                 ZAYOMSS.conv_conversion_error_log.error_description%TYPE;
   lv_class_of_err             ZAYOMSS.conv_conversion_error_log.class_of_error%TYPE;
   lv_error_locator            ZAYOMSS.conv_conversion_error_log.error_locator_id%TYPE;
   lv_src_lookup               ZAYOMSS.conv_conversion_error_log.src_data_key_lookup%TYPE;
   lv_resolution_reqd          ZAYOMSS.conv_conversion_error_log.resolution_required%TYPE;
   lv_status                   VARCHAR2 (10);
   lv_proc_name                ZAYOMSS.conv_conversion_error_log.program_function%TYPE;
   lv_insert_or_update_flg     CHAR (1);
   lv_total_recs               NUMBER := 0;
   lv_target_recs              NUMBER := 0;
   lv_error_recs               NUMBER := 0;

	
   BEGIN
      --process to initialize the variables

      initialize_variables;
 SELECT COUNT (1)
     INTO gv_prc_cnt
     FROM ZAYOMSS.conv_stat_summary_log
    WHERE program_name = gv_package_name
      AND program_function = lv_procedure_name;
      

     IF gv_prc_cnt = 0
      THEN
         init_conv_stat_summary_log (rec_conv_stat_summary_log,
                                     lv_procedure_name);
         --PROCESS TO INSERT INTO CONV_STAT_SUMMARY_LOG TABLE
         insert_conv_stat_summary_log (rec_conv_stat_summary_log);
         COMMIT;
      END IF;
		
	  gv_total_recs := 0;
      gv_target_recs := 0;
      gv_error_recs := 0;
    
	  
	 	
      stg_load_ewomain ;   
	  lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED EWO_MAIN',NULL);
	 
	  stg_load_ewocir; 
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_ewocir',NULL);
	  
      stg_load_ewoequip; 
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_ewoequip',NULL);
		
      stg_load_ewonotes;
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_ewonotes',NULL);

      stg_load_ms_link; 
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_ms_link',NULL);

      --stg_load_ewo_udf (p_process_id,ewo_main.document_number);
      --lv_total_recs := lv_total_recs + gv_total_recs;
      --lv_target_recs := lv_target_recs + gv_target_recs;
      --lv_error_recs := lv_error_recs + gv_error_recs;

      stg_load_eworel; 
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_eworel',NULL);

      stg_load_ewo_history; 
      lv_total_recs := lv_total_recs + gv_total_recs;
      lv_target_recs := lv_target_recs + gv_target_recs;
      lv_error_recs := lv_error_recs + gv_error_recs;
	  --insert_log(lv_procedure_name,'DEBUG',-1,NULL,'COMPLETED stg_load_ewo_history',NULL);
	  
	  
	  --DBMS_OUTPUT.PUT_LINE('Total Records: ' || lv_total_recs);
      --DBMS_OUTPUT.PUT_LINE('Target Records: ' || lv_target_recs);
      --DBMS_OUTPUT.PUT_LINE('Error Records: ' || lv_error_recs);
	  	  	    
	  -- update record count on successful run
	UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + lv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + lv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + lv_target_recs
       WHERE program_name = gv_package_name
         AND program_function = lv_procedure_name
         AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');
    
	  COMMIT;
	
	--END LOOP;
EXCEPTION
   WHEN OTHERS THEN
      gv_error_recs := gv_error_recs + 1;
      lv_err_mesg := SQLERRM;
      insert_log (lv_procedure_name, 'FATAL ERROR', -29999, 'NA', lv_err_mesg, 'Need to check code');

         -- to update the conv stat summary log tables with the counts
       UPDATE ZAYOMSS.conv_stat_summary_log
         SET program_end_time = SYSDATE,
             source_record_cnt = NVL (source_record_cnt, 0) + gv_total_recs,
             error_record_cnt = NVL (error_record_cnt, 0) + gv_error_recs,
             target_record_cnt = NVL (target_record_cnt, 0) + gv_target_recs
       WHERE program_name = gv_package_name
         AND program_function = lv_procedure_name
         AND program_run_date = TO_DATE (gv_run_dt, 'YYYYMMDD');

         COMMIT;
   END stg_ewo_main;
END;