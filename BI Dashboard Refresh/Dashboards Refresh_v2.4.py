import requests  # type: ignore
import os
import time
import csv
import pandas as pd
from datetime import datetime

CSV_FILE = 'dashboards.csv'
BLOCKED_FILE = 'blocked_datasets.csv'

<credentials>

BATCH_SIZE = 12
CONCURRENT_REFRESHES = 4
RETRY_BATCH_SIZE = 6
MAX_RETRIES = 2

def load_dashboard_configs(csv_file):
    dashboards = []
    seen_users = set()
    with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_key = (row['User name'], row['Password'])
            if user_key not in seen_users:
                seen_users.add(user_key)
                dashboards.append(row)
    return dashboards

def load_blocked_datasets(file_path):
    if not os.path.exists(file_path):
        return set()
    df = pd.read_csv(file_path)
    return set(zip(df['User Name'], df['DATASET_ID']))

def get_token(config):
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    data = {
        'grant_type': 'password',
        'resource': RESOURCE,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': config['User name'],
        'password': config['Password'],
        'scope': SCOPE
    }
    try:
        response = requests.post(url, data=data)
        return response.json().get('access_token')
    except Exception as e:
        print(f"Token fetch failed for {config['User name']}: {str(e)}")
        return None

def get_user_datasets(user_config, token):
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get("https://api.powerbi.com/v1.0/myorg/datasets", headers=headers)
    if response.status_code == 200:
        return response.json().get("value", [])
    print(f"Failed to fetch datasets for {user_config['User name']}: {response.text}")
    return []

def start_refresh(config, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{config['DATASET_ID']}/refreshes"

    try:
        check_response = requests.get(url, headers=headers)
        if check_response.status_code == 200:
            refreshes = check_response.json().get("value", [])
            if refreshes and refreshes[0]["status"] in ["Unknown", "InProgress"]:
                return "InProgress", None

        response = requests.post(url, headers=headers)
        if response.status_code == 202:
            return "Started", headers
        else:
            return f"Failed: {response.text}", None
    except Exception as e:
        return f"Exception: {str(e)}", None

def check_status(dataset_id, headers):
    try:
        url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()["value"][0]["status"]
        else:
            return f"Check Failed: {response.text}"
    except Exception as e:
        return f"Check Exception: {str(e)}"

def batch_process(dashboards, blocked, retry_mode=False):
    refresh_log = []
    failed_datasets = []

    for batch_start in range(0, len(dashboards), BATCH_SIZE if not retry_mode else RETRY_BATCH_SIZE):
        batch = dashboards[batch_start:batch_start + (BATCH_SIZE if not retry_mode else RETRY_BATCH_SIZE)]
        print(f"\nStarting {'Retry' if retry_mode else 'Main'} Batch: {batch_start // (BATCH_SIZE if not retry_mode else RETRY_BATCH_SIZE) + 1}")

        refreshing = []
        batch_index = 0

        while batch_index < len(batch) or refreshing:
            while len(refreshing) < CONCURRENT_REFRESHES and batch_index < len(batch):
                config = batch[batch_index]
                dataset_id = config['DATASET_ID']
                user = config['User name']
                batch_index += 1

                print(f"Attempting refresh: Dataset={dataset_id}, User={user}")
                token = get_token(config)
                if not token:
                    refresh_log.append(log_entry(config, "Failed", "Token Error"))
                    failed_datasets.append(config)
                    continue

                status, headers = start_refresh(config, token)
                start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if status == "Started":
                    refreshing.append({"config": config, "headers": headers, "start_time": start_time})
                elif status == "InProgress":
                    print(f"Already refreshing: {dataset_id} for {user}")
                else:
                    print(f"{status}: {dataset_id} for {user}")
                    refresh_log.append(log_entry(config, "Failed", status, start_time))
                    failed_datasets.append(config)

            if refreshing:
                print("Waiting 60s to check status...")
                time.sleep(60)
                still_refreshing = []
                for r in refreshing:
                    status = check_status(r['config']['DATASET_ID'], r['headers'])
                    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if status == "Completed":
                        print(f"Success: {r['config']['DATASET_ID']} ({r['config']['User name']})")
                        refresh_log.append(log_entry(r['config'], "Success", "", r['start_time'], end_time))
                    elif status == "Failed":
                        print(f"Failed: {r['config']['DATASET_ID']} ({r['config']['User name']})")
                        refresh_log.append(log_entry(r['config'], "Failed", "Refresh Failed", r['start_time'], end_time))
                        failed_datasets.append(r['config'])
                    else:
                        still_refreshing.append(r)
                refreshing = still_refreshing

    return refresh_log, failed_datasets

def log_entry(config, status, message, start_time="", end_time=""):
    return {
        "User Name": config["User name"],
        "Dataset ID": config["DATASET_ID"],
        "Start Time": start_time,
        "End Time": end_time,
        "Status": status,
        "Error Message": message
    }

def main():
    all_users = load_dashboard_configs(CSV_FILE)
    blocked = load_blocked_datasets(BLOCKED_FILE)
    dashboards = []

    for user in all_users:
        token = get_token(user)
        if not token:
            continue
        datasets = get_user_datasets(user, token)
        for ds in datasets:
            key = (user['User name'], ds['id'])
            if key in blocked:
                continue
            config = user.copy()
            config['DATASET_ID'] = ds['id']
            dashboards.append(config)

    logs, failed = batch_process(dashboards, blocked, retry_mode=False)

    for attempt in range(1, MAX_RETRIES + 1):
        if not failed:
            break
        print(f"\nRetry Attempt {attempt} for {len(failed)} failed datasets")
        retry_logs, failed = batch_process(failed, blocked, retry_mode=True)
        for log in retry_logs:
            if log["Status"] == "Success":
                log["Status"] = f"Retry Success ({attempt})"
            elif log["Status"] == "Failed":
                log["Status"] = f"Retry Failed ({attempt})"
        logs.extend(retry_logs)

    df_log = pd.DataFrame(logs)
    today_str = time.strftime("%d-%m-%Y")
    filename = f"refresh_log_{today_str}.xlsx"
    df_log.to_excel(filename, index=False)
    print(f"\nRefresh log saved to {filename} in {os.getcwd()}")
    print("All refresh operations completed.")

if __name__ == "__main__":
    main()
