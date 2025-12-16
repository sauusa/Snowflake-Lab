/*
Ask:
Build a Teams's webhook to do the following:
1. Identify the failed snowflake tasks of client's choice.
2. Send alerts to selected recipients on microsoft teams.
3. Ability to disable sending the alerts
4. Future state: use this solution for other messangers as well
*/

--Step 1: Metadata Table
--This table defines which tasks should trigger alerts and where to send them.
USE WAREHOUSE SSAHU_LAB;
USE DATABASE SSAHU_LAB;
CREATE OR REPLACE SCHEMA ALERTS;

--Task Name: The actual Snowflake task name for which you need the alert
--Enabled: To turn notification on or off 
--Webhook_URL: Full link to your Webhook
--Notify_User: (Optional) User list for your notificaiton tagging -TBD

CREATE OR REPLACE TABLE SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG (
    TASK_NAME STRING PRIMARY KEY,
    ENABLED BOOLEAN DEFAULT TRUE,
    WEBHOOK_URL STRING,
    NOTIFY_USERS ARRAY -- optional, for future user-level config
);

--Optional: Create a table for the alert log 
/*
CREATE TABLE IF NOT EXISTS SSAHU_LAB.ALERTS.TASK_ALERT_LOG (
    TASK_NAME STRING,
    ALERT_STATUS STRING,
    ERROR_MESSAGE STRING,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
*/
--Add an entry to this table, link webhook with task 
--get the webhook link from external app channel
--test the link 1st, by running the following command in MAC terminal
--replace the webhook link with yours below
--curl -X POST -H 'Content-Type: application/json' -d '{"text":"Hello Saurabh"}' https://snowflakelab007.webhook.office.com/webhookb2/79####1b-d##f-####-####-d08#########b-####-####-####-c3#####d/IncomingWebhook/4281#############ce/............ 

INSERT INTO SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG (TASK_NAME, WEBHOOK_URL)
VALUES
('SAMPLE_MONITOR_TASK','https://snowflakelab007.webhook.office.com/webhookb2/79####1b-d##f-####-####-d08#########b-####-####-####-c3#####d/IncomingWebhook/4281#############ce/............')
;

INSERT INTO SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG (TASK_NAME, WEBHOOK_URL)
VALUES
('SAMPLE_INSERT_TASK','https://snowflakelab007.webhook.office.com/webhookb2/79####1b-d##f-####-####-d08#########b-####-####-####-c3#####d/IncomingWebhook/4281#############ce/............');

SELECT * FROM SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG;

--STEP2: Setup Network Rules 
--We need EGRESS connection for external communication

--CREATE OR REPLACE SCHEMA NETWORK_RULES;
USE SCHEMA SSAHU_LAB.NETWORK_RULES;

-- Populate with webhook host of the target Teams account, such as customer.webhook.office.com
--in the value list provide only the host URL name not the entire link from above
--:443 is the default Network Port for HTTPS 

create or replace network rule "SSAHU_LAB"."NETWORK_RULES"."EGRESS_TEAMS_WEBHOOKS"
  type = HOST_PORT
  mode = EGRESS
  value_list = ('snowflakelab007.webhook.office.com:443') --Replace it with your MS Teams link
  comment = 'Allows traffic to be sent to webhooks in the target Teams environment'
;

---External Access Integration setup is required for external communication
create or replace external access integration "EAI_WEBHOOKS"
  allowed_network_rules = (SSAHU_LAB.NETWORK_RULES.EGRESS_TEAMS_WEBHOOKS)
  enabled = TRUE
  comment = 'External access integration to support traffic sent to webhooks in the target environment'
;
------------------Testing Block-----------------
--Test the SQL for the Procedure:
--Display only the latest failed messages limit by 1 per task
--Select only the chosen tasks from the table above
--Check only in past hour

WITH TaskFailures AS (
    SELECT
        NAME,
        STATE,
        ERROR_MESSAGE,
        COMPLETED_TIME,
        -- Assign a rank to each row partitioned by NAME, ordered by COMPLETED_TIME descending
        ROW_NUMBER() OVER (
            PARTITION BY NAME
            ORDER BY COMPLETED_TIME DESC
        ) AS rn
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    WHERE
        NAME IN (
            SELECT DISTINCT TASK_NAME
            FROM SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG
        )
        AND STATE IN ('FAILED', 'SKIPPED', 'FAILED_AND_AUTO_SUSPENDED')
        AND COMPLETED_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())
)
-- Select only the latest failure for each task (where rn = 1)
SELECT
    NAME,
    STATE,
    ERROR_MESSAGE,
    COMPLETED_TIME
FROM
    TaskFailures
WHERE
    rn = 1;
--Expected results: you should be able to see 1 failed task per task name from your config list which is failed in last 1 hour
--This is to limit the communication message that will be sent to Teams channel
--Only the latest failed message will be sent
------------------Testing Block End-----------------


---------------PROCEDURE TO SEND COMMUNICATIONS TO EXTERNAL CHANNELS---------------
--Important points:
--> Check your EXTERNAL_ACCESS_INTEGRATIONS name, it should match exactly the above
--> task_failure = session.sql("""paste the above tested query here""")

CREATE OR REPLACE PROCEDURE SSAHU_LAB.PROCEDURES.ALERT_TASK_FAILURES()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = ("EAI_WEBHOOKS")
AS
$$
import requests
import json
from snowflake.snowpark.functions import col, lit

def main(session):

    # Step 1: Get only the latest failure (or skipped) per task in last hour
    task_failures = session.sql("""
        WITH TaskFailures AS (
            SELECT
                NAME,
                STATE,
                ERROR_MESSAGE,
                COMPLETED_TIME,
                ROW_NUMBER() OVER (
                    PARTITION BY NAME
                    ORDER BY COMPLETED_TIME DESC
                ) AS rn
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE STATE IN ('FAILED', 'SKIPPED', 'FAILED_AND_AUTO_SUSPENDED')
              AND COMPLETED_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())
              AND NAME IN (
                SELECT DISTINCT TASK_NAME
                FROM SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG
                WHERE ENABLED = TRUE
              )
        )
        SELECT NAME, STATE, ERROR_MESSAGE, COMPLETED_TIME
        FROM TaskFailures
        WHERE rn = 1
    """).collect()

    if not task_failures:
        return "✅ No failed or skipped tasks in the last hour."

    # Step 2: Get task → webhook mappings
    config_rows = session.table("SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG") \
        .filter(col("ENABLED") == lit(True)) \
        .select(col("TASK_NAME"), col("WEBHOOK_URL")) \
        .collect()

    if not config_rows:
        return "⚠️ No webhook configurations found in TASK_ALERT_CONFIG."

    # Step 3: Build lookup dictionary: {task_name: [list of webhook URLs]}
    task_to_webhooks = {}
    for row in config_rows:
        task = row["TASK_NAME"]
        url = row["WEBHOOK_URL"]
        if task not in task_to_webhooks:
            task_to_webhooks[task] = []
        task_to_webhooks[task].append(url)

    # Step 4: Send alert for each failed task to its corresponding webhook(s)
    sent_count = 0
    for row in task_failures:
        task_name = row["NAME"]
        if task_name not in task_to_webhooks:
            continue

        message = (
            f"🚨 **Task Failure Alert** 🚨\n\n"
            f"**Task:** {task_name}\n"
            f"**State:** {row['STATE']}\n"
            f"**Completed:** {row['COMPLETED_TIME']}\n"
            f"**Error:** {row['ERROR_MESSAGE'] or 'N/A'}"
        )

        for webhook_url in task_to_webhooks[task_name]:
            headers = {'Content-Type': 'application/json'}

            # Detect Slack vs Teams (by domain)
            if "hooks.slack.com" in webhook_url.lower():
                payload = {"text": message}
            elif "webhook.office.com" in webhook_url.lower():
                payload = {"text": message}
            else:
                print(f"⚠️ Unknown webhook type for URL: {webhook_url}")
                continue

            try:
                response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    sent_count += 1
                else:
                    print(f"⚠️ Failed for {task_name} → {response.status_code}: {response.text}")
            except Exception as e:
                print(f"❌ Error sending to {webhook_url}: {e}")

    return f"✅ Sent {sent_count} alerts for {len(task_failures)} latest failed tasks in the last hour."

$$;

--Test this procedure
CALL SSAHU_LAB.PROCEDURES.ALERT_TASK_FAILURES();
--Note: sometimes any failed task can take upto 45minutes to appear in the ACCOUNT_USAGE so be patient
--You should be able to see a message in your Team's channel if you have a failed task in last 1 hour
--Once tested successfully, you can create a task for automation
--make sure you have a failed task for you to test

--Create a task to automate this run every 15 minute on the clock: 12:15, 12:30, 12:45 etc
CREATE OR REPLACE SCHEMA SSAHU_LAB.TASKS;

CREATE OR REPLACE TASK SSAHU_LAB.TASKS.ALERT_TASK_FAILURES_MONITOR
  WAREHOUSE = ADHOC_WH
--  SCHEDULE = '15 MINUTE' -- every 15 minute from the run 
  SCHEDULE = 'USING CRON 0,15,30,45 * * * * UTC'  -- every 15 min on the clock--
AS
  CALL SSAHU_LAB.PROCEDURES.ALERT_TASK_FAILURES();

--Resume the task manually after the 1st create
ALTER TASK SSAHU_LAB.TASKS.ALERT_TASK_FAILURES_MONITOR RESUME;
--to test this task immidiately use the following
EXECUTE TASK SSAHU_LAB.TASKS.ALERT_TASK_FAILURES_MONITOR;

/**To check the entry in metadata table
SELECT NAME, STATE, ERROR_MESSAGE, COMPLETED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE 
NAME in (SELECT DISTINCT TASK_NAME FROM SSAHU_LAB.ALERTS.TASK_ALERT_CONFIG) 
AND STATE in ('FAILED','SKIPPED')
AND 
  COMPLETED_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP());
**/
