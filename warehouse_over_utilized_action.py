import json
import sys
import datetime
from snowflake.snowpark import Session
import pandas as pd
import datetime
from datetime import timedelta



#connection_parameters = {
#   "account": "AFA78268",
#   "user": "sayali",
#   "password": "Atgeir@03",
#   "warehouse": "COSTNOMICS_WH",
#   "role": "ACCOUNTADMIN",
#   "database": "DATAGEIR_COSTNOMICS_DEV",
#   "schema": "APP"}

#session = Session.builder.configs(connection_parameters).create()

def create_df(session, sql_query):
    df = session.sql(sql_query)
    return df

def get_wh_size(size):
    size_map = {
        'X-Small': 'Small',
        'Small': 'Medium',
        'Medium': 'Large',
        'Large': 'X-Large',
        'X-Large': '2X-Large',
        '2X-Large': '3X-Large',
        '3X-Large': '4X-Large',
        '4X-Large': '5X-Large',
        '5X-Large': '6X-Large',
        '6X-Large': '6X-Large'
    }
    return size_map.get(size, size)

def action_execute(session):

  try:
        previous_data_sql = """select WAREHOUSE_NAME,REASON,Total_Load_percentage,Total_spillage_percentage
                                        ,ACTION,ACTION_TAKEN,ACTION_TAKEN_TIMESTAMP  
                                        from APP.over_utilization_result WHERE ACTION_TAKEN=TRUE;"""
        previous_data = create_df(session, previous_data_sql).to_pandas()

        wh_details = """select warehouse_name, round((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY DAILY_SPILLAGE_PERCENT/100)),2) as ninety_per_value_spillage,
        round((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DAILY_SPILLAGE_PERCENT)),2) as seventyfive_per_value_spillage,
        round((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DAILY_SPILLAGE_PERCENT)),2) as  fifty_per_value_spillage, 
        round((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY DAILY_LOAD_PERCENT)),2) as ninety_per_value_load,
        round((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DAILY_LOAD_PERCENT)),2) as seventyfive_per_value_load,
        round((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DAILY_LOAD_PERCENT)),2) as fifty_per_value_load    
        from app.WH_OVER_UTILIZED_STATS  where DATE BETWEEN (DATEADD(day, -14, CURRENT_TIMESTAMP())) AND (DATE(CURRENT_TIMESTAMP())) 
        group by warehouse_name limit 2"""

        wh_data = create_df(session, wh_details).to_pandas()
        if not wh_data.empty:
            session.sql('TRUNCATE TABLE APP.over_utilization_result;').collect()
            warehouses = []
            for index, wh in wh_data.iterrows():
                wh_name = wh["WAREHOUSE_NAME"]
                print(wh_name)
                warehouses.append(wh_name)
                ninety_per_value_spillage = int(wh["NINETY_PER_VALUE_SPILLAGE"])
                seventyfive_per_value_spillage = int(wh["SEVENTYFIVE_PER_VALUE_SPILLAGE"])
                fifty_per_value_spillage = int(wh["FIFTY_PER_VALUE_SPILLAGE"])
                ninety_per_value_load = int(wh["NINETY_PER_VALUE_LOAD"])
                seventyfive_per_value_load = int(wh["SEVENTYFIVE_PER_VALUE_LOAD"])
                fifty_per_value_load = int(wh["FIFTY_PER_VALUE_LOAD"])

                wh_size_query = f"""SELECT warehouse_size FROM snowflake.account_usage.query_history 
                                    WHERE warehouse_name='{wh_name}' AND START_TIME=
                                    (SELECT MAX(START_TIME) FROM snowflake.account_usage.query_history WHERE 
                                    warehouse_name='{wh_name}' AND warehouse_size IS NOT NULL);"""
                wh_size_data = create_df(session, wh_size_query).to_pandas()

                if len(wh_size_data)==0:
                    continue
                size = wh_size_data["WAREHOUSE_SIZE"][0]
                any_wh_satisfies_condition = []

                if ninety_per_value_spillage >= 40 and ninety_per_value_load < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"90% of time warehouse is over-utilized because of high spillage percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load

                    new_size = get_wh_size(size)

                    if ((size == 'X-Small') or (size == 'Small')):
                        take_action = {
                            'Suggestion': f'Increase size of the warehouse to {new_size}.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_SIZE = {new_size};',
                            'Alter_Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse'
                        }
                    else:
                        warehouse_type = 'SNOWPARK-OPTIMIZED'
                        take_action = {
                            'Suggestion': f'Change the type of the warehouse to snowpark-optimized warehouse.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_TYPE = '+ warehouse_type +';',
                            'snowpark_optimized_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized'
                        }

                elif ninety_per_value_load >= 40 and ninety_per_value_spillage < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"90% of time warehouse is over-utilized because of high query load percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load
                    take_action = ""
                    wh_cluster_query = f"""SELECT name, size, MIN_CLUSTER_COUNT, MAX_CLUSTER_COUNT FROM app.warehouses
                                                                                            WHERE name='{wh_name}' AND UPDATED_ON=
                                                                                            (SELECT MAX(UPDATED_ON) FROM app.warehouses WHERE
                                                                                            name='{wh_name}' AND size IS NOT NULL);"""
                    wh_cluster_data = create_df(session, wh_cluster_query).to_pandas()
                    min_cluster_count = wh_cluster_data["MIN_CLUSTER_COUNT"][0]
                    max_cluster_count = wh_cluster_data["MAX_CLUSTER_COUNT"][0]
                    size = wh_cluster_data["SIZE"][0]
                    if min_cluster_count == max_cluster_count == 1:
                        take_action = {
                            'Suggestion': f'Create new warehouse (Please replace <WH_NAME> with the warehouse name that suits your organization policies).',
                            'WH_SQL': f'CREATE OR REPLACE WAREHOUSE <WH_NAME> WITH WAREHOUSE_SIZE = {size};',
                            'Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/create-warehouse'
                        }
                    elif min_cluster_count != max_cluster_count:
                        new_max_cluster_count = max_cluster_count + 1
                        take_action = {
                            'Suggestion': f'Alter warehouse to add additional cluster.',
                            'WH_SQL': f'ALTER WAREHOUSE {wh_name} SET MAX_CLUSTER_COUNT = {new_max_cluster_count};',
                            'MCW_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-multicluster'
                        }

                elif seventyfive_per_value_spillage >= 40 and seventyfive_per_value_load < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"75% of time warehouse is over-utilized because of high spillage percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load

                    new_size = get_wh_size(size)

                    if ((size == 'X-Small') or (size == 'Small')):
                        take_action = {
                            'Suggestion': f'Increase size of the warehouse to {new_size}.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_SIZE = {new_size};',
                            'Alter_Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse'
                        }
                    else:
                        warehouse_type = 'SNOWPARK-OPTIMIZED'
                        take_action = {
                            'Suggestion': f'Change the type of the warehouse to snowpark-optimized warehouse.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_TYPE = '+ warehouse_type +';',
                            'snowpark_optimized_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized'
                        }

                elif seventyfive_per_value_load >= 40 and seventyfive_per_value_spillage < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"75% of time warehouse is over-utilized because of high query load percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load
                    take_action = ""
                    wh_cluster_query = f"""SELECT name, size, MIN_CLUSTER_COUNT, MAX_CLUSTER_COUNT FROM app.warehouses
                                                                                            WHERE name='{wh_name}' AND UPDATED_ON=
                                                                                            (SELECT MAX(UPDATED_ON) FROM app.warehouses WHERE
                                                                                            name='{wh_name}' AND size IS NOT NULL);"""
                    wh_cluster_data = create_df(session, wh_cluster_query).to_pandas()
                    min_cluster_count = wh_cluster_data["MIN_CLUSTER_COUNT"][0]
                    max_cluster_count = wh_cluster_data["MAX_CLUSTER_COUNT"][0]
                    size = wh_cluster_data["SIZE"][0]
                    if min_cluster_count == max_cluster_count == 1:
                        take_action = {
                            'Suggestion': f'Create new warehouse (Please replace <WH_NAME> with the warehouse name that suits your organization policies).',
                            'WH_SQL': f'CREATE OR REPLACE WAREHOUSE <WH_NAME> WITH WAREHOUSE_SIZE = {size};',
                            'Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/create-warehouse'
                        }
                    elif min_cluster_count != max_cluster_count:
                        new_max_cluster_count = max_cluster_count + 1
                        take_action = {
                            'Suggestion': f'Alter warehouse to add additional cluster.',
                            'WH_SQL': f'ALTER WAREHOUSE {wh_name} SET MAX_CLUSTER_COUNT = {new_max_cluster_count};',
                            'MCW_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-multicluster'
                        }

                elif fifty_per_value_spillage >= 40 and fifty_per_value_load < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"50% of time warehouse is over-utilized because of high spillage percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load

                    new_size = get_wh_size(size)

                    if ((size == 'X-Small') or (size == 'Small')):
                        take_action = {
                            'Suggestion': f'Increase size of the warehouse to {new_size}.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_SIZE = {new_size};',
                            'Alter_Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse'
                        }
                    else:
                        warehouse_type = 'SNOWPARK-OPTIMIZED'
                        take_action = {
                            'Suggestion': f'Change the type of the warehouse to snowpark-optimized warehouse.',
                            'WH_SQL': f'ALTER WAREHOUSE IF EXISTS {wh_name} SET WAREHOUSE_TYPE = '+ warehouse_type +';',
                            'snowpark_optimized_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized'
                        }

                elif fifty_per_value_load >= 40 and fifty_per_value_spillage < 40:
                    any_wh_satisfies_condition.append(True)
                    reason = f"50% of time warehouse is over-utilized because of high query load percentage"
                    total_spillage_percentage = ninety_per_value_spillage
                    total_load_percentage = ninety_per_value_load
                    take_action = ""
                    wh_cluster_query = f"""SELECT name, size, MIN_CLUSTER_COUNT, MAX_CLUSTER_COUNT FROM app.warehouses
                                                                                            WHERE name='{wh_name}' AND UPDATED_ON=
                                                                                            (SELECT MAX(UPDATED_ON) FROM app.warehouses WHERE
                                                                                            name='{wh_name}' AND size IS NOT NULL);"""
                    wh_cluster_data = create_df(session, wh_cluster_query).to_pandas()
                    min_cluster_count = wh_cluster_data["MIN_CLUSTER_COUNT"][0]
                    max_cluster_count = wh_cluster_data["MAX_CLUSTER_COUNT"][0]
                    size = wh_cluster_data["SIZE"][0]
                    if min_cluster_count == max_cluster_count == 1:
                        take_action = {
                            'Suggestion': f'Create new warehouse (Please replace <WH_NAME> with the warehouse name that suits your organization policies).',
                            'WH_SQL': f'CREATE OR REPLACE WAREHOUSE <WH_NAME> WITH WAREHOUSE_SIZE = {size};',
                            'Wh_Reference': 'https://docs.snowflake.com/en/sql-reference/sql/create-warehouse'
                        }
                    elif min_cluster_count != max_cluster_count:
                        new_max_cluster_count = max_cluster_count + 1
                        take_action = {
                            'Suggestion': f'Alter warehouse to add additional cluster.',
                            'WH_SQL': f'ALTER WAREHOUSE {wh_name} SET MAX_CLUSTER_COUNT = {new_max_cluster_count};',
                            'MCW_Reference': 'https://docs.snowflake.com/en/user-guide/warehouses-multicluster'
                        }
                else:
                    any_wh_satisfies_condition.append(False)
                    continue

                if True not in any_wh_satisfies_condition:
                    sys.exit(0)
                # if wh name exist in previous data
                if wh_name in (previous_data['WAREHOUSE_NAME'].values):
                    print(wh_name)
                    timestamp = (
                        previous_data.loc[
                            previous_data['WAREHOUSE_NAME'] == wh_name, 'ACTION_TAKEN_TIMESTAMP']).item()
                    current_time = datetime.datetime.now()
                    delta = current_time - timestamp
                    if delta >= timedelta(days=14):
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'REASON'] = reason
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'TOTAL_LOAD_PERCENTAGE'] = total_load_percentage
                        previous_data.loc[previous_data['WAREHOUSE_NAME'] == wh_name,
                                          'TOTAL_SPILLAGE_PERCENTAGE'] = total_spillage_percentage
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'ACTION'] = str(take_action)
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'ACTION_TAKEN'] = False
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'ACTION_TAKEN_TIMESTAMP'] = 'NULL'
                    else:
                        action = json.loads((previous_data.loc[
                            previous_data['WAREHOUSE_NAME'] == wh_name, 'ACTION']).item())
                        previous_data.loc[previous_data['WAREHOUSE_NAME']
                                          == wh_name, 'ACTION'] = str(action)

                else:
                    new_list = []
                    print(wh_name,reason,total_load_percentage,total_spillage_percentage,take_action)
                    new_list.append(wh_name)
                    new_list.append(reason)
                    new_list.append(total_load_percentage)
                    new_list.append(total_spillage_percentage)
                    new_list.append(take_action)
                    new_list.append(False)
                    new_list.append('NULL')
                    print(new_list,len(new_list))
                    #previous_data.loc[len(previous_data.index)] = new_list
                    new_row_df = pd.DataFrame([new_list], columns=previous_data.columns)
                    previous_data = previous_data.append(new_row_df, ignore_index=True)


            mask = previous_data['WAREHOUSE_NAME'].isin(warehouses)
            df_filtered = previous_data[mask]

            for key, inner_dict in df_filtered.iterrows():

                if (inner_dict['ACTION_TAKEN_TIMESTAMP'] == 'NULL'):
                    insert_query = f"""insert into APP.over_utilization_result  (WAREHOUSE_NAME, Reason , 
                                               Total_Load_percentage , Total_spillage_percentage , 
                                               ACTION ,ACTION_TAKEN ,ACTION_TAKEN_TIMESTAMP )
                                               select '{inner_dict['WAREHOUSE_NAME']}'
                                               ,'{inner_dict['REASON']}'
                                               ,{inner_dict['TOTAL_LOAD_PERCENTAGE']}
                                               ,{inner_dict['TOTAL_SPILLAGE_PERCENTAGE']}
                                               ,TO_JSON({inner_dict['ACTION']})
                                               ,{inner_dict['ACTION_TAKEN']}
                                               ,{inner_dict['ACTION_TAKEN_TIMESTAMP']}
                                               """
                else:
                    insert_query = f"""insert into APP.over_utilization_result (WAREHOUSE_NAME, Reason , 
                                               Total_Load_percentage , Total_spillage_percentage , 
                                               ACTION ,ACTION_TAKEN ,ACTION_TAKEN_TIMESTAMP )
                                               select '{inner_dict['WAREHOUSE_NAME']}'
                                               ,'{inner_dict['REASON']}'
                                               ,{inner_dict['TOTAL_LOAD_PERCENTAGE']}
                                               ,{inner_dict['TOTAL_SPILLAGE_PERCENTAGE']}
                                               ,TO_JSON({inner_dict['ACTION']})
                                               ,{inner_dict['ACTION_TAKEN']}
                                               ,'{inner_dict['ACTION_TAKEN_TIMESTAMP']}'
                                               """

                session.sql(insert_query).collect()
            status = 'SUCCESS'
            return status
  except Exception as ex:
    print("Exception", ex)
    status = 'FAILED'
    return status

#print(action_execute(session))



