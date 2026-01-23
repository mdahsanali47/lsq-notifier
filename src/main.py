import smtplib
import time
import os, sys
import logging
from datetime import date, timedelta, datetime, timezone
import requests
import random
import csv
import pytz
import pymysql

import oci

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

from dotenv import load_dotenv
load_dotenv()


class LeadSquaredNotifier:
    def __init__(self):
        logging.info("---------Initializing Configuration from env variables")
        self.host = os.environ.get("LEADSQUARED_HOST")
        self.access_key = os.environ.get("LSQ_ACCESS_KEY")
        self.secret_key = os.environ.get("LSQ_SECRET_KEY")
        self.visit_plan_type_id = os.environ.get("VISIT_PLAN_TYPE_ID")

        self.db_host = os.environ.get("DB_HOST")
        self.db_user = os.environ.get("DB_USER")
        self.db_password = os.environ.get("DB_PASSWORD")
        self.db_name = os.environ.get("DB_NAME")
        self.db_port = os.environ.get("DB_PORT")

        self.smtp_host = os.environ.get("SMTP_HOST")
        self.smtp_port = os.environ.get("SMTP_PORT")
        self.smtp_user = os.environ.get("SMTP_USER")
        self.smtp_password = os.environ.get("SMTP_PASSWORD")
        self.sender_email = self.smtp_user
        self.sender_name = "LeadSquared Automation Bot"

        # --- OCI Configuration ---
        self.oci_config_profile = os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT")
        self.oci_tenancy_ocid = os.environ.get("OCI_TENANCY_OCID")
        self.oci_user_ocid = os.environ.get("OCI_USER_OCID")
        self.oci_key_fingerprint = os.environ.get("OCI_KEY_FINGERPRINT")
        self.oci_private_key_path = os.environ.get("OCI_PRIVATE_KEY_PATH") # Path inside the container
        self.oci_region = os.environ.get("OCI_REGION")
        self.oci_bucket_name = os.environ.get("OCI_BUCKET_NAME")
        self.oci_folder_path = os.environ.get("OCI_FOLDER_PATH", "visit-plan-reports")

        self.dry_run = int(os.environ.get("DRY_RUN"))

        # query to get active sales users from database
        self.db_query = os.environ.get("DB_QUERY")

        required_vars = [
            self.host, self.access_key, self.secret_key, self.visit_plan_type_id,
            self.smtp_host, self.smtp_user, self.smtp_password,
            self.oci_tenancy_ocid, self.oci_user_ocid, self.oci_key_fingerprint,
            self.oci_private_key_path, self.oci_region, self.oci_bucket_name
        ]
        if not all(required_vars):
            logging.error("One or more required environment variables are not set. Exiting.")
            sys.exit(1)
            
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CaptainSteel-LSQ-Notifier/1.0'})
        
        # Initialize OCI client
        self.oci_object_storage_client = None
        if not self.dry_run: 
            self.initialize_oci_client()


    def initialize_oci_client(self):
        try:
            config = {
                "user": self.oci_user_ocid,
                "fingerprint": self.oci_key_fingerprint,
                "key_file": self.oci_private_key_path,
                "tenancy": self.oci_tenancy_ocid,
                "region": self.oci_region,    
            }
            self.oci_object_storage_client = oci.object_storage.ObjectStorageClient(config)
            self.oci_namespace = self.oci_object_storage_client.get_namespace().data

            logging.info("OCI Object Storage client initialized successfully.")
        except Exception as e:
            logging.exception(f"Failed to initialize OCI Object Storage client: {e}")
            # Allow script to continue but without OCI upload if client fails
            self.oci_object_storage_client = None

    def get_db(self):
        return pymysql.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
            port=int(self.db_port)
        )

    def get_current_weekdays(self):
        IST = pytz.timezone("Asia/Kolkata")
        UTC = pytz.UTC

        now_ist = datetime.now(IST)

        week_start_ist = (
            now_ist
            .replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=now_ist.weekday())
        )

        week_end_ist = week_start_ist + timedelta(days=5, hours=23, minutes=59, seconds=59)


        week_start_utc = week_start_ist.astimezone(UTC)
        week_end_utc = week_end_ist.astimezone(UTC)

        from_date_str = week_start_utc.strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = week_end_utc.strftime("%Y-%m-%d %H:%M:%S")

        return (
            from_date_str,
            to_date_str,
            week_start_ist.date(),
            week_end_ist.date()
        )

    def get_active_sales_users(self):
        url = f"{self.host}/v2/UserManagement.svc/User.AdvancedSearch"
        params = {'accessKey': self.access_key, 'secretKey': self.secret_key}
        payload = {
                    "Columns": {
                        "Include_CSV": "UserID,FirstName,LastName,EmailAddress,Role,StatusCode,Team,TeamId,EmployeeId"
                    },
                    "GroupConditions": [
                        {
                        "Condition": [
                            {
                            "LookupName": "StatusCode",
                            "Operator": "eq",
                            "LookupValue": 0,
                            "ConditionOperator": "AND"
                            },
                            {
                            "LookupName": "State",
                            "Operator": "eq",
                            "LookupValue": "West Bengal",
                            "ConditionOperator": "AND"
                            },
                            {
                            "LookupName": "Role",
                            "Operator": "neq",
                            "LookupValue": "Administrator",
                            "ConditionOperator": "AND"
                            },
                            {
                            "LookupName": "Role",
                            "Operator": "neq",
                            "LookupValue": "Marketing_User",
                            "ConditionOperator": "AND"
                            },
                            {
                            "LookupName": "Team",
                            "Operator": "neq",
                            "LookupValue": "Captain Steel India Limited",
                            "ConditionOperator": null
                            }
                        ],
                        "GroupOperator": null
                        }
                    ],
                    "Paging": {
                        "PageIndex": 1,
                        "PageSize": 1000
                    }
                }

        try:
            response = self.session.post(url=url, params=params, json=payload, timeout=30)
            response.raise_for_status()
            users_data = response.json()
            total_users = users_data.get('SearchInfo')
            logging.info(f"Found Active {total_users}")
            
            all_users = users_data.get('Users')

            active_sales_users = [
                user for user in all_users
            ]

            logging.info(f"found  {len(active_sales_users)} total active sales users")
            # taking random users for testing
            active_sales_users = random.sample(active_sales_users, 5)
        
            return active_sales_users
        except requests.exceptions.RequestException as e:
            logging.exception(f"Failed to get users from LeadSquared: {e}")
            return None
        
    def get_active_sales_user_from_db(self):
        try:
            stmt = self.db_query
            if not stmt:
                raise ValueError("Database query not found in environment variables")
            with self.get_db() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(stmt)
            
                    users = cursor.fetchall()
            logging.info(f"found {len(users)} total active sales users from database")
            # users = random.sample(users, 35) # ------------------------------------------TESTING FOR 35 USERS
            # logging.info(f"selected {len(users)} random users for testing")
            return users
        except pymysql.MySQLError as db_err:
            logging.exception(f"Failed to get users from database: {db_err}")
            return None
            

    def get_user_tasks(self, email, from_date, to_date):
        url = f"{self.host}/v2/Task.svc/Retrieve"
        params = {'accessKey': self.access_key, 'secretKey': self.secret_key}
        payload = {
                    "Parameter": {
                        "LookupName":   "OwnerEmailAddress",
                        "LookupValue":  email,
                        "FromDate":     from_date,
                        "ToDate":       to_date,
                        "StatusCode":   0,
                        "TypeName":     self.visit_plan_type_id,
                    },
                    "Columns": {
                                "Exclude_CSV": 
                                    "Category,Description,RelatedEntity,RelatedEntityId,RelatedEntityIdName,RelatedSubEntityId," +
                                    "Reminder,ReminderBeforeDays,ReminderTime,NotifyBy," +
                                    "OwnerId,OwnerName,OwnerEmailAddress," +
                                    "CreatedBy,CreatedByName,CreatedOn," +
                                    "ModifiedBy,ModifiedByName,ModifiedOn," +
                                    "CompletedOn,CompletedBy,CompletedByName," +
                                    "EndDate,EffortEstimateUnit,PercentCompleted,Priority," +
                                    "Location,Latitude,Longitude," +
                                    "TaskType,CustomFields"
                                },
                    "Sorting": {
                        "ColumnName": "Duedate",
                        "Direction":  1
                    },
                    "Paging": {
                        "Offset":   0,
                        "RowCount": 200
                    }
                }
        try:
            response = self.session.post(url=url, params=params, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve tasks for {email}: {e}")
            return None



    def process_user_task(self, user, total_incomplete_tasks, all_tasks_for_user, week_start_date, week_end_date):
        user_email = user.get("email_address")
        if not user_email:
            return None

        week_start = week_start_date
        week_end = week_end_date

        IST_OFFSET = timedelta(hours=5, minutes=30)

        daily_task_counts = {}
        current_date = week_start

        while current_date <= week_end:
            daily_task_counts[current_date.strftime("%Y-%m-%d")] = 0
            current_date += timedelta(days=1)

        
        for task in all_tasks_for_user or []:
            due_date_str = task.get("DueDate")
            if not due_date_str:
                continue

            try:
                due_dt_utc = datetime.strptime(
                    due_date_str.split('.')[0],
                    "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)

                due_dt_ist = due_dt_utc + IST_OFFSET
                due_date_ist = due_dt_ist.date()

                due_date = due_date_ist

                if week_start <= due_date <= week_end:
                    key = due_date.strftime("%Y-%m-%d")
                    daily_task_counts[key] += 1

            except (ValueError, TypeError) as e:
                logging.warning(
                    f"Could not parse DueDate '{due_date_str}' "
                    f"for task '{task.get('Name')}' for user {user_email}: {e}"
                )

        return {
            "UserEmail": user_email,
            "FirstName": user.get("first_name"),
            "LastName": user.get("last_name"),
            **daily_task_counts,
            "TotalIncompleteTasks": total_incomplete_tasks
        }

    def save_to_csv(self, data, filename):
        filepath = os.path.join("/tmp", filename)
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data[0].keys())
            for row in data:
                writer.writerow(row.values())
        return filepath

    def upload_to_oci(self, filepath, filename):
        """Uploads the CSV file to the OCI bucket."""
        if self.dry_run or not self.oci_object_storage_client:
            logging.warning(f"OCI Upload skipped: Dry run is enabled or OCI client not initialized. File would be uploaded as: {self.oci_folder_path}/{filename}")
            return False

        object_name = f"{self.oci_folder_path}/{filename}" if self.oci_folder_path else filename

        try:
            logging.info(f"Uploading {filepath} to OCI bucket '{self.oci_bucket_name}' as '{object_name}'...")
            with open(filepath, 'rb') as f:
                put_object_response = self.oci_object_storage_client.put_object(
                    namespace_name=self.oci_namespace,
                    bucket_name=self.oci_bucket_name,
                    object_name=object_name,
                    put_object_body=f,
                    content_type='text/csv'
                )
            # Check if upload was successful (status code 200)
            if put_object_response.status == 200:
                logging.info(f"Successfully uploaded report to OCI.")
                return True
            else:
                logging.error(f"OCI upload failed with status code: {put_object_response.status}")
                return False
        except Exception as e:
            logging.exception(f"Failed to upload report to OCI: {e}")
            return False

    def send_reminder_email(self, user, week_start_date, week_end_date):
        receiver_email = user.get("email_address")
        first_name = user.get('first_name', 'there')

        subject = f"Action Required: Create Your Visit Plan for the Week {week_start_date} to {week_end_date}"
        body = f"""
        Dear {first_name},

        This is an automated reminder.

        We've noticed that you have not yet created any visit plans in LeadSquared for the current week of {week_start_date.strftime('%B %d')} to {week_end_date.strftime('%B %d')}.

        To ensure proper planning and tracking, please create your visit plans for the week at your earliest convenience.

        Thank you,
        Captain Steel India Limited
        """

        message = MIMEMultipart()
        message['From'] = f"{self.sender_name} <{self.sender_email}>"
        message['To'] = receiver_email
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.sender_email, receiver_email, message.as_string())
        except Exception as e:
            logging.exception(f"failed to send email to: {receiver_email}")


    def run(self):
        logging.info("---- Starting Visit Plan Check")

        if self.dry_run:
            logging.warning("!!! RUNNING IN DRY RUN MODE. NO EMAILS OR OCI UPLOADS WILL OCCUR. !!!")
        else:
            logging.info("--- RUNNING IN LIVE MODE. EMAILS AND OCI UPLOADS WILL OCCUR. ---")

        from_date, to_date, week_start, week_end = self.get_current_weekdays()
        logging.info(f"Checking for visit plans for the week: {from_date} to {to_date}")

        active_users = self.get_active_sales_user_from_db()
        if active_users is None:
            logging.error("Could not retrieve user list. Aborting.")
            return

        all_user_task_data = []
        users_to_notify = []

        for user in active_users:
            email = user.get("email_address")
            if not email:
                continue

            logging.info(f"Checking user: {user.get('first_name')} {user.get('last_name')} ({email})")

            time.sleep(0.25)

            tasks = self.get_user_tasks(email=email, from_date=from_date, to_date=to_date)

            if tasks is None:
                logging.error(f"Skipping user {email} due to an API error fetching tasks.")
                continue

            total_incomplete_tasks = tasks.get("RecordCount")
            if total_incomplete_tasks == 0:
                users_to_notify.append(user)
            
            logging.info(f"Total incomplete tasks for user {email}: {total_incomplete_tasks}")
            tasks_list = tasks.get("List")

            # process data 
            task_data = self.process_user_task(user, total_incomplete_tasks, tasks_list, week_start, week_end)
            if task_data:
                all_user_task_data.append(task_data)

        csv_filepath = None
        if all_user_task_data:
            # Generate a dynamic filename based on the week
            report_date_str = week_start.strftime('%Y-%m-%d')
            csv_filename = f"visit_plan_report_{report_date_str}.csv"
            csv_filepath = self.save_to_csv(all_user_task_data, filename=csv_filename)
        else:
            logging.info("No user task data to save to CSV.")

        # --- Upload to OCI ---
        if csv_filepath:
            self.upload_to_oci(csv_filepath, csv_filename)
        
        # --- Send Emails (if not dry run) ---
        if not self.dry_run and users_to_notify:
            logging.info(f"LIVE MODE: Sending reminder emails to {len(users_to_notify)} users...")
            for user in users_to_notify:
                self.send_reminder_email(user, week_start, week_end)
        elif self.dry_run and users_to_notify:
            logging.warning("DRY RUN: The following users would receive a reminder:")
            for user in users_to_notify:
                logging.info(f"  - [WOULD SEND TO]: {user.get('FirstName')} {user.get('LastName')} ({user.get('EmailAddress')})")
        elif not users_to_notify:
            logging.info("All users have visit plans. No notifications needed.")

        logging.info("--- Script Finished ---")
                
        
            

if __name__ == "__main__":
    lsq_notifier = LeadSquaredNotifier()
    lsq_notifier.run()