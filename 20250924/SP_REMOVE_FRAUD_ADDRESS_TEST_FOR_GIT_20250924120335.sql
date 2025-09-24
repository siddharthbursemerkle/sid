CREATE OR REPLACE PROCEDURE "SP_REMOVE_FRAUD_ADDRESS_TEST_FOR_GIT"()
RETURNS ARRAY
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
/********************************************************************************************
* PROCEDURE : SP_REMOVE_FRAUD_ADDRESS () 
*              
* EXAMPLE   : CALL SP_REMOVE_FRAUD_ADDRESS()
*
* Change Log:
* 
*   Date            Developer            	Jira          Change Description
*   ----------      --------------       ---------      ---------------------------------
*  2025-05-27 		Venkata Manchineni    ANFC-4192  	Added this Sp to SID remove address from the give tables ( We''re not setting this up to run automatically. We''ll run it manually whenever it''s needed. No Tidal job..)
*****************************************************************************************


var return_array = [];
var v_codestep = 0;
var v_job = ''SP_REMOVE_FRAUD_ADDRESS'';
var PROCESS_NAME = ''SP_REMOVE_FRAUD_ADDRESS'';
var V_INSERT_ROW_COUNT = 0;
var V_UPDATE_ROW_COUNT = 0;
var V_DELETE_ROW_COUNT = 0;
var PROCESS_ID = 0;
var PROCESS_LOG_ID = 0;



function getFormattedTimestamp() {
    var d = new Date();
    return d.getFullYear().toString() +
           ("0" + (d.getMonth() + 1)).slice(-2) +
           ("0" + d.getDate()).slice(-2) + "_" +
           ("0" + d.getHours()).slice(-2) +
           ("0" + d.getMinutes()).slice(-2) +
           ("0" + d.getSeconds()).slice(-2);
}

function get_process_log_id(process_name) {
    var log = [];

    var get_pid_sql = `SELECT PROCESS_ID FROM STG.PROCESS WHERE PROCESS_NAME = ''` + process_name + `''`;
    var rs = snowflake.execute({ sqlText: get_pid_sql });

    if (rs.next()) {
        PROCESS_ID = rs.getColumnValue(1);
        log.push("PROCESS_ID: " + PROCESS_ID);
    } else {
        throw "PROCESS_NAME not found in STG.PROCESS: " + process_name;
    }

    var log_sql = `CALL STG.ops_begin_process_log(''` + process_name + `'', '''', 0, ''0'')`;
    var rs_log = snowflake.execute({ sqlText: log_sql });

    if (rs_log.next()) {
        PROCESS_LOG_ID = rs_log.getColumnValue(1);
        log.push("PROCESS_LOG_ID: " + PROCESS_LOG_ID);
    } else {
        throw "Failed to start process log for: " + process_name;
    }

    return log;
}

try {
    v_codestep = 10;
    var v_curdt = new Date().toLocaleString();
    return_array.push(v_curdt + " STEP:" + v_codestep + " Starting SP_REMOVE_FRAUD_ADDRESS");

    // NEW: Get PROCESS_ID and PROCESS_LOG_ID
    v_codestep = 11;
    var log_result = get_process_log_id(PROCESS_NAME);
    for (var i = 0; i < log_result.length; i++) {
        return_array.push(new Date().toLocaleString() + " STEP:" + v_codestep + " " + log_result[i]);
    }
	var v_codestep = 20;

    var insert_sql = `
                      CREATE OR REPLACE TABLE  stg.dq_fraud_address_addr_key_2 AS 
                      SELECT *,CURRENT_DATE AS inserted_date FROM ( 
                          SELECT
                              a.ADDR_KEY,
                              ADDRESS_LINE_1,
                              ADDRESS_LINE_2,
                              b.CITY,
                              b.STATE_CD AS STATE
                          FROM MDB.D_ADDR a
                          JOIN STG.DQ_FRAUD_ADDRESS b 
                              ON a.ADDR_LINE_1_TEXT = b.ADDRESS_LINE_1
                              AND a.ADDR_LINE_2_TEXT = b.ADDRESS_LINE_2
                              AND a.CITY = b.CITY
                              AND a.STATE_PROVINCE_CD = b.STATE_CD
                          WHERE B.ACTIVE_FLAG = ''Y''
                          GROUP BY a.ADDR_KEY, ADDRESS_LINE_1, ADDRESS_LINE_2, b.CITY, b.STATE_CD
                      
                          UNION DISTINCT
                      
                          SELECT
                              a.ACTIVE_ADDR_KEY AS ADDR_KEY,
                              ADDRESS_LINE_1,
                              ADDRESS_LINE_2,
                              b.CITY,
                              b.STATE_CD AS STATE
                          FROM MDB.D_ADDR a
                          JOIN STG.DQ_FRAUD_ADDRESS b 
                              ON a.ADDR_LINE_1_TEXT = b.ADDRESS_LINE_1
                              AND a.ADDR_LINE_2_TEXT = b.ADDRESS_LINE_2
                              AND a.CITY = b.CITY
                              AND a.STATE_PROVINCE_CD = b.STATE_CD
                          WHERE B.ACTIVE_FLAG = ''Y''
                          GROUP BY a.ACTIVE_ADDR_KEY, ADDRESS_LINE_1, ADDRESS_LINE_2, b.CITY, b.STATE_CD
                      ) a
                      WHERE NOT EXISTS (
                          SELECT 1 
                          FROM stg.dq_fraud_address_addr_key b 
                          WHERE 
                              a.ADDR_KEY = b.ADDR_KEY 
                              AND a.ADDRESS_LINE_1 = b.ADDRESS_LINE_1 
                              AND a.ADDRESS_LINE_2 = b.ADDRESS_LINE_2 
                              AND a.CITY = b.CITY 
                              AND a.STATE = b.STATE
                      )
                      `;
    
    var insert_stmt = snowflake.createStatement({sqlText: insert_sql});
    insert_stmt.execute();
    
    return_array.push(v_curdt + " STEP:" + v_codestep + " INSERT executed into stg.dq_fraud_address_addr_key");


	    v_codestep = 160;
	    PROCESS_LOG_STATUS = ''Succeeded'';
        var v_msg = v_curdt + " STEP:" + v_codestep + " PROCESS_LOG_STATUS is :" + PROCESS_LOG_STATUS
        return_array.push(v_msg);                           
                              
	    PROCESS_LOG_STATUS_MESSAGE =  v_job + '' ''  + PROCESS_LOG_STATUS;                        
        var v_msg = v_curdt + " STEP:" + v_codestep + " PROCESS_LOG_STATUS_MESSAGE is :" + PROCESS_LOG_STATUS_MESSAGE
        return_array.push(v_msg);                             

        var v_msg = v_curdt + " STEP:" + v_codestep + " PROCESS_LOG_ID for STG.OPS_END_PROCESS_LOG is : " + PROCESS_LOG_ID
        return_array.push(v_msg);                               

		ds_v_sqlCommand = "CALL    STG.ops_end_process_log( ''" + PROCESS_LOG_ID + "'' ,0,0,0,0,0, ''" + V_INSERT_ROW_COUNT + "'' , ''" + V_UPDATE_ROW_COUNT + "'' , ''" + V_DELETE_ROW_COUNT + "'' , 0 , ''" + PROCESS_LOG_STATUS_MESSAGE + "'' , ''" + PROCESS_LOG_STATUS + "'' , 0) ;";

		var resultSet = snowflake.execute ( {sqlText: ds_v_sqlCommand} );
		if (resultSet.getRowCount() > 0)
		{
		resultSet.next(); 
	
		PROCESS_LOG_ID = resultSet.getColumnValue(1);
		
		}
		
		v_codestep = 170;
		var v_curdt = new Date().toLocaleString();
		var v_msg = v_curdt + " STEP:" + v_codestep + " Job completed successfully " + v_job;
		return_array.push(v_msg);



} catch (err) {
    return_array.push("ERROR at step " + v_codestep + ": " + err);
}

return return_array;
';