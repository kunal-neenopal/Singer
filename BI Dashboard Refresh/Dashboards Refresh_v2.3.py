import requests
import time
import csv
import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurations
CSV_FILE = 'dashboards.csv'
BLOCKED_DATASETS_FILE = 'blocked_datasets.csv'

CONCURRENCY_LIMIT = 4
BATCH_SIZE = 12
RETRY_LIMIT = 3

# Load CSV files
def load_dashboard_configs(csv_file):
    dashboards = []
    with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dashboards.append(row)
    return dashboards

def load_blocked_datasets(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return set(df['DATASET_ID'].dropna().unique())
    return set()

# Token
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
    response = requests.post(url, data=data)
    return response.json().get('access_token')

# Fetch datasets for a user without duplicates
def fetch_user_datasets(user, token, blocked_ids, seen_ids):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = "https://api.powerbi.com/v1.0/myorg/datasets"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return []

    datasets = response.json().get("value", [])
    result = []
    for ds in datasets:
        ds_id = ds["id"]
        if ds_id not in blocked_ids and ds_id not in seen_ids:
            seen_ids.add(ds_id)
            result.append({**user, "DATASET_ID": ds_id})
    return result

# Start refresh
def start_refresh(config, headers):
    dataset_id = config["DATASET_ID"]
    check_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
    check_response = requests.get(check_url, headers=headers)
    if check_response.status_code == 200:
        refreshes = check_response.json().get("value", [])
        if refreshes and refreshes[0]["status"] in ["Unknown", "InProgress"]:
            print(f"🔄 Already refreshing: {dataset_id}")
            return None

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
    response = requests.post(url, headers=headers)
    if response.status_code == 202:
        print(f"✅ Started refresh: {dataset_id} (User: {config['User name']})")
        return {"config": config, "headers": headers, "start_time": datetime.now()}
    else:
        print(f"❌ Failed to start refresh: {dataset_id} - {response.text}")
        return {"config": config, "headers": headers, "start_time": datetime.now(), "error": response.text}

# Check refresh status
def check_status(refresh_obj):
    config = refresh_obj["config"]
    headers = refresh_obj["headers"]
    dataset_id = config["DATASET_ID"]
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
    response = requests.get(url, headers=headers)
    status = response.json()["value"][0]["status"]
    print(f"📡 Status: {dataset_id} -> {status}")
    return status

# Main logic
def main():
    dashboard_users = load_dashboard_configs(CSV_FILE)
    blocked_datasets = load_blocked_datasets(BLOCKED_DATASETS_FILE)
    seen_dataset_ids = set()
    all_configs = []

    print("🔍 Gathering datasets in batches...")
    for user in dashboard_users:
        token = get_token(user)
        if not token:
            continue
        user_configs = fetch_user_datasets(user, token, blocked_datasets, seen_dataset_ids)
        all_configs.extend(user_configs)

    refresh_log = []
    retry_queue = []

    for batch_start in range(0, len(all_configs), BATCH_SIZE):
        batch = all_configs[batch_start: batch_start + BATCH_SIZE]
        print(f"\n🚀 Starting batch {batch_start//BATCH_SIZE + 1}: {len(batch)} datasets")
        with ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
            futures = {}
            for config in batch:
                token = get_token(config)
                if not token:
                    continue
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                refresh_obj = start_refresh(config, headers)
                if refresh_obj:
                    futures[executor.submit(check_status, refresh_obj)] = refresh_obj
            for future in as_completed(futures):
                refresh_obj = futures[future]
                status = future.result()
                config = refresh_obj['config']
                dataset_id = config['DATASET_ID']
                user_name = config['User name']
                dashboard_name = config.get('Dashboard Name', '')
                start_time = refresh_obj['start_time']
                end_time = datetime.now()
                error_msg = refresh_obj.get("error", "")

                if status == "Completed":
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Status": "Success",
                        "Error": ""
                    })
                else:
                    print(f"❌ Refresh failed: {dataset_id} - User: {user_name}")
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Status": "Failed",
                        "Error": error_msg
                    })
                    retry_queue.append({**config, "attempt": 1})

        print("⏳ Waiting 60 seconds before next batch...")
        time.sleep(60)

    # Retry logic with concurrency limit
    for attempt in range(2, RETRY_LIMIT + 1):
        print(f"\n🔁 Retry Attempt {attempt}")
        next_retry_queue = []
        with ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
            retrying = []
            for item in retry_queue:
                if item["attempt"] == attempt - 1:
                    token = get_token(item)
                    if not token:
                        continue
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    }
                    item["attempt"] = attempt
                    refresh_obj = start_refresh(item, headers)
                    if refresh_obj:
                        retrying.append((executor.submit(check_status, refresh_obj), refresh_obj))

            for future, refresh_obj in retrying:
                status = future.result()
                config = refresh_obj['config']
                dataset_id = config['DATASET_ID']
                user_name = config['User name']
                dashboard_name = config.get('Dashboard Name', '')
                start_time = refresh_obj['start_time']
                end_time = datetime.now()
                error_msg = refresh_obj.get("error", "")

                if status == "Completed":
                    print(f"✅ Retry success: {dataset_id}")
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Status": f"Success (Retry {attempt})",
                        "Error": ""
                    })
                else:
                    print(f"❌ Retry failed: {dataset_id}")
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Status": f"Failed (Attempt {attempt})",
                        "Error": error_msg
                    })
                    if attempt < RETRY_LIMIT:
                        next_retry_queue.append({**config, "attempt": attempt})

        retry_queue = next_retry_queue
        print("⏳ Waiting 60 seconds before next retry round...")
        time.sleep(60)

    # Save refresh log
    df_log = pd.DataFrame(refresh_log)
    today_str = time.strftime("%d-%m-%Y")
    df_log.to_csv(f'refresh_log_{today_str}.csv', index=False)
    print(f"\n📄 Refresh log saved as refresh_log_{today_str}.csv\n📂 Saved to: {os.getcwd()}")

if __name__ == "__main__":
    main()
