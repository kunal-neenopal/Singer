import requests
import time
import csv
import os
import pandas as pd

CSV_FILE = 'dashboards.csv'
BLOCKED_DATASETS_FILE = 'blocked_datasets.csv'
BATCH_SIZE = 12           # Number of datasets to load at a time
CONCURRENCY_LIMIT = 4     # Number of concurrent refreshes

<Credentials>

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
        print(f"✅ Started refresh: {dataset_id}")
        return {"config": config, "headers": headers, "start_time": time.strftime('%Y-%m-%d %H:%M:%S')}
    else:
        print(f"❌ Failed to start refresh: {dataset_id} - {response.text}")
        return {"config": config, "headers": headers, "start_time": time.strftime('%Y-%m-%d %H:%M:%S'), "error": response.text}

def check_status(refresh_obj):
    config = refresh_obj["config"]
    headers = refresh_obj["headers"]
    dataset_id = config["DATASET_ID"]
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
    response = requests.get(url, headers=headers)
    if "value" not in response.json():
        return "Failed"
    status = response.json()["value"][0]["status"]
    print(f"📡 Status: {dataset_id} -> {status}")
    return status

def refresh_batch(datasets_batch, refresh_log, retry_queue):
    refreshing = []
    while datasets_batch or refreshing:
        still_refreshing = []
        for refresh_obj in refreshing:
            status = check_status(refresh_obj)
            config = refresh_obj['config']
            dataset_id = config['DATASET_ID']
            user_name = config['User name']
            dashboard_name = config.get('Dashboard Name', '')
            error_msg = refresh_obj.get("error", "")
            if status == "Completed":
                refresh_log.append({
                    "User name": user_name,
                    "Dashboard Name": dashboard_name,
                    "Dataset ID": dataset_id,
                    "Start Time": refresh_obj['start_time'],
                    "End Time": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Status": "Success",
                    "Error": ""
                })
            elif status == "Failed":
                refresh_log.append({
                    "User name": user_name,
                    "Dashboard Name": dashboard_name,
                    "Dataset ID": dataset_id,
                    "Start Time": refresh_obj['start_time'],
                    "End Time": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Status": "Failed",
                    "Error": error_msg
                })
                retry_queue.append({**config, "attempt": 1})
            else:
                still_refreshing.append(refresh_obj)
        refreshing = still_refreshing

        while len(refreshing) < CONCURRENCY_LIMIT and datasets_batch:
            config = datasets_batch.pop(0)
            token = get_token(config)
            if not token:
                print(f"❌ Token fetch failed for {config['User name']}")
                continue
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            refresh_obj = start_refresh(config, headers)
            if refresh_obj:
                refreshing.append(refresh_obj)

        if refreshing:
            print("⏳ Waiting 60 seconds before next status check...")
            time.sleep(60)

def retry_failed(retry_queue, refresh_log):
    for attempt in [2, 3]:
        print(f"\n🔁 Retry Attempt {attempt}")
        next_retry = []
        retrying_now = []
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
                    retrying_now.append(refresh_obj)

        while retrying_now:
            still_retrying = []
            for refresh_obj in retrying_now:
                status = check_status(refresh_obj)
                config = refresh_obj['config']
                dataset_id = config['DATASET_ID']
                user_name = config['User name']
                dashboard_name = config.get('Dashboard Name', '')
                error_msg = refresh_obj.get("error", "")
                if status == "Completed":
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": refresh_obj['start_time'],
                        "End Time": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Status": f"Success (Retry {attempt})",
                        "Error": ""
                    })
                elif status == "Failed":
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": refresh_obj['start_time'],
                        "End Time": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Status": f"Failed (Retry {attempt})",
                        "Error": error_msg
                    })
                    if attempt < 3:
                        next_retry.append({**config, "attempt": attempt})
                else:
                    still_retrying.append(refresh_obj)
            retrying_now = still_retrying
            if retrying_now:
                print("⏳ Waiting 60 seconds before next retry check...")
                time.sleep(60)
        retry_queue = next_retry

def main():
    dashboards = load_dashboard_configs(CSV_FILE)
    blocked = load_blocked_datasets(BLOCKED_DATASETS_FILE)
    all_datasets = []
    for user in dashboards:
        token = get_token(user)
        if not token:
            print(f"Could not ger token for {user['User name']}")
            continue
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        resp = requests.get("https://api.powerbi.com/v1.0/myorg/datasets", headers=headers)
        if resp.status_code != 200:
            print(f"Failed to get datasets for {user['User name']}: {resp.text}")
            continue
        datasets = resp.json().get("value", [])
        for ds in datasets:
            if ds["id"] not in blocked:
                all_datasets.append({**user, "DATASET_ID": ds["id"]})

    refresh_log = []
    retry_queue = []
    i = 0
    while i < len(all_datasets):
        batch = all_datasets[i:i + BATCH_SIZE]
        refresh_batch(batch, refresh_log, retry_queue)
        i += BATCH_SIZE

    retry_failed(retry_queue, refresh_log)

    log_df = pd.DataFrame(refresh_log)
    today_str = time.strftime('%d-%m-%Y')
    filename = f'refresh_log_{today_str}.csv'
    log_df.to_csv(filename, index=False)
    print(f"\n📄 Refresh log saved as {filename} in {os.getcwd()}")

if __name__ == "__main__":
    main()
