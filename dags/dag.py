from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.bash import BashOperator

import os 

from datetime import datetime,timedelta

hook = MsSqlHook(
        mssql_conn_id="sqlserver_conn"
    )



def initialize_structure():

    initialize_sql = """

    IF DB_ID('CRICKET') IS NULL
    BEGIN
        CREATE DATABASE CRICKET;
    END;


    IF OBJECT_ID('CRICKET.DBO.BRONZE_RAW_TABLE','U') IS NULL
	BEGIN
		CREATE TABLE CRICKET.DBO.BRONZE_RAW_TABLE(
            delivery_id varchar(200),
            delivery_timestamp varchar(200),
            innings_no varchar(200),
            over_no varchar(200),
            ball_in_over varchar(200),
            runs_off_bat varchar(200),
            extra_type varchar(200),
            extra_runs varchar(200),
            total_runs varchar(200),
            is_wicket varchar(200),
            dismissal_type varchar(200),
            ball_speed_kmph varchar(200),
            non_striker_id varchar(200),
            shot_type varchar(200),
            shot_region varchar(200),
            match_id varchar(200),
            match_date varchar(200),
            match_format varchar(200),
            tournament_name varchar(200),
            season varchar(200),
            match_stage varchar(200),
            toss_decision varchar(200),
            venue_id varchar(200),
            venue_name varchar(200),
            venue_city varchar(200),
            venue_country varchar(200),
            batting_team_id varchar(200),
            batting_team_name varchar(200),
            bowling_team_id varchar(200),
            bowling_team_name varchar(200),
            batsman_id varchar(200),
            batsman_name varchar(200),
            batsman_dob varchar(200),
            batsman_nationality varchar(200),
            batting_hand varchar(200),
            batsman_role varchar(200),
            batsman_email varchar(200),
            batsman_phone varchar(200),
            batsman_icc_rating varchar(200),
            bowler_id varchar(200),
            bowler_name varchar(200),
            bowler_nationality varchar(200),
            bowling_style varchar(200),
            bowler_icc_rating varchar(200),
            contract_id varchar(200),
            contract_grade varchar(200),
            annual_retainer_usd varchar(200),
            contract_status varchar(200),
            drs_review_id varchar(200),
            drs_reviewed_by varchar(200),
            drs_original_decision varchar(200),
            drs_outcome varchar(200),
            bronze_ingest_timestamp datetime default CURRENT_TIMESTAMP
        );
	END;


        
    IF OBJECT_ID('CRICKET.DBO.BRONZE_STAGE_RAW_TABLE','U') IS NULL
        BEGIN
            CREATE TABLE CRICKET.DBO.BRONZE_STAGE_RAW_TABLE(
                delivery_id varchar(200),
                delivery_timestamp varchar(200),
                innings_no varchar(200),
                over_no varchar(200),
                ball_in_over varchar(200),
                runs_off_bat varchar(200),
                extra_type varchar(200),
                extra_runs varchar(200),
                total_runs varchar(200),
                is_wicket varchar(200),
                dismissal_type varchar(200),
                ball_speed_kmph varchar(200),
                non_striker_id varchar(200),
                shot_type varchar(200),
                shot_region varchar(200),
                match_id varchar(200),
                match_date varchar(200),
                match_format varchar(200),
                tournament_name varchar(200),
                season varchar(200),
                match_stage varchar(200),
                toss_decision varchar(200),
                venue_id varchar(200),
                venue_name varchar(200),
                venue_city varchar(200),
                venue_country varchar(200),
                batting_team_id varchar(200),
                batting_team_name varchar(200),
                bowling_team_id varchar(200),
                bowling_team_name varchar(200),
                batsman_id varchar(200),
                batsman_name varchar(200),
                batsman_dob varchar(200),
                batsman_nationality varchar(200),
                batting_hand varchar(200),
                batsman_role varchar(200),
                batsman_email varchar(200),
                batsman_phone varchar(200),
                batsman_icc_rating varchar(200),
                bowler_id varchar(200),
                bowler_name varchar(200),
                bowler_nationality varchar(200),
                bowling_style varchar(200),
                bowler_icc_rating varchar(200),
                contract_id varchar(200),
                contract_grade varchar(200),
                annual_retainer_usd varchar(200),
                contract_status varchar(200),
                drs_review_id varchar(200),
                drs_reviewed_by varchar(200),
                drs_original_decision varchar(200),
                drs_outcome varchar(200)
            );
        END;


     IF OBJECT_ID('CRICKET.DBO.FILES_METADATA','U') IS NULL
            BEGIN
                CREATE TABLE CRICKET.DBO.FILES_METADATA
                (
                    FILE_ID INT IDENTITY(1,1),
                    FILE_NAME VARCHAR(200),
                    LOAD_STATUS BIT,
                    LOAD_TIMESTAMP DATETIME
                );
            END;


    """

   

    hook.run(initialize_sql)
    print("Database and Tables Initialization Successfully !!!")



def load_data():

    # get processed files
    get_file_metadata_sql = """
        SELECT * FROM CRICKET.DBO.FILES_METADATA;
    """

    file_metadata_response = hook.get_records(get_file_metadata_sql)
    print(file_metadata_response)

    
    processed_files = []

    if file_metadata_response:
        for i in range(len(file_metadata_response)):
            processed_files.append(file_metadata_response[i][1])
    print(f"PROCESSED FILES : {processed_files}")

    # get all csv files 
    path = os.path.join(os.path.dirname(__file__), 'data')
    files = os.listdir(path)
    csv_files = []
    for file in files:
        if file.endswith(".csv"):
            csv_files.append(file)
    
    print(f"ALL CSV FILES IN DATA FOLDER : {csv_files}")

    #process only not processed files
    for file in csv_files:
        if file not in processed_files:
            bulk_insert_sql = f"""
                    BULK INSERT CRICKET.DBO.BRONZE_STAGE_RAW_TABLE
                    FROM "D:\\Kenexai Exam\\medallion_exam\\airflow\\dags\\data\\{file}"
                    WITH (
                            FORMAT = 'CSV',
                            FIRSTROW = 2,
                            FIELDQUOTE = '"',
                            FIELDTERMINATOR = ',',
                            ROWTERMINATOR = '0x0a',
                            TABLOCK
                    );


                INSERT INTO CRICKET.DBO.BRONZE_RAW_TABLE(
                    delivery_id,
                    delivery_timestamp ,
                    innings_no ,
                    over_no ,
                    ball_in_over ,
                    runs_off_bat ,
                    extra_type ,
                    extra_runs ,
                    total_runs ,
                    is_wicket ,
                    dismissal_type ,
                    ball_speed_kmph ,
                    non_striker_id ,
                    shot_type ,
                    shot_region ,
                    match_id ,
                    match_date ,
                    match_format ,
                    tournament_name ,
                    season ,
                    match_stage ,
                    toss_decision ,
                    venue_id ,
                    venue_name ,
                    venue_city ,
                    venue_country ,
                    batting_team_id ,
                    batting_team_name ,
                    bowling_team_id ,
                    bowling_team_name ,
                    batsman_id ,
                    batsman_name ,
                    batsman_dob ,
                    batsman_nationality ,
                    batting_hand ,
                    batsman_role ,
                    batsman_email ,
                    batsman_phone ,
                    batsman_icc_rating ,
                    bowler_id ,
                    bowler_name ,
                    bowler_nationality ,
                    bowling_style ,
                    bowler_icc_rating ,
                    contract_id ,
                    contract_grade ,
                    annual_retainer_usd ,
                    contract_status ,
                    drs_review_id ,
                    drs_reviewed_by ,
                    drs_original_decision ,
                    drs_outcome 
                )
                SELECT * FROM CRICKET.DBO.BRONZE_STAGE_RAW_TABLE;

                TRUNCATE TABLE CRICKET.DBO.BRONZE_STAGE_RAW_TABLE;
                """

            insert_in_files_metadata_sql = """
                INSERT INTO CRICKET.DBO.FILES_METADATA
                (FILE_NAME,LOAD_STATUS,LOAD_TIMESTAMP)
                VALUES(%s,1,CURRENT_TIMESTAMP)
            """

            hook.run(bulk_insert_sql)
            hook.run(insert_in_files_metadata_sql, parameters = (file))





with DAG(
    dag_id="CIRCKET_DAG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    initialize_task = PythonOperator(
        task_id = 'initialize_structure',
        python_callable = initialize_structure
    )

    load_data_task = PythonOperator(
        task_id = 'load_data',
        python_callable = load_data
    )

    dbt_silver = BashOperator(
        task_id = 'dbt_silver',
        bash_command=(
            "cd /usr/local/airflow/dbt_exam && "
            "dbt build --select path:models/silver"
        )
    ) 


    initialize_task >> load_data_task >> dbt_silver