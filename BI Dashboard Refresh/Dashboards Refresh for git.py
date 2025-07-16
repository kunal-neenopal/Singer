import requests
import time
import csv
import os
import pandas as pd



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
    # Check status before triggering
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
    status = response.json()["value"][0]["status"]
    print(f"📡 Status: {dataset_id} -> {status}")
    return status

def main():
    dashboard_users = load_dashboard_configs(CSV_FILE)
    blocked_datasets = load_blocked_datasets(BLOCKED_DATASETS_FILE)
    refresh_log = []
    retry_queue = []

    for user in dashboard_users:
        print(f"\n🧾 Processing user: {user['User name']}")
        token = get_token(user)
        if not token:
            print(f"❌ Token failed for {user['User name']}")
            continue
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Load datasets
        response = requests.get("https://api.powerbi.com/v1.0/myorg/datasets", headers=headers)
        if response.status_code != 200:
            print(f"❌ Dataset fetch failed for {user['User name']}")
            continue
        datasets = response.json().get("value", [])

        user_pending = [
            {**user, "DATASET_ID": ds["id"]}
            for ds in datasets if ds["id"] not in blocked_datasets
        ]

        user_refreshing = []
        while user_pending or user_refreshing:
            still_refreshing = []
            for refresh_obj in user_refreshing:
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
            user_refreshing = still_refreshing

            while len(user_refreshing) < 4 and user_pending:
                config = user_pending.pop(0)
                refresh_obj = start_refresh(config, headers)
                if refresh_obj:
                    user_refreshing.append(refresh_obj)

            if user_refreshing:
                print("⏳ Waiting 60 seconds before next check...")
                time.sleep(60)

    # RETRY PHASE
    for attempt in [2, 3]:
        print(f"\n🔁 Retry Attempt {attempt} for failed datasets")
        retry_queue_new = []
        retrying_now = []

        for item in retry_queue:
            if item["attempt"] == attempt - 1:
                token = get_token(item)
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
                        "Status": "Success (Retry)",
                        "Error": ""
                    })
                elif status == "Failed":
                    refresh_log.append({
                        "User name": user_name,
                        "Dashboard Name": dashboard_name,
                        "Dataset ID": dataset_id,
                        "Start Time": refresh_obj['start_time'],
                        "End Time": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Status": f"Failed (Attempt {attempt})",
                        "Error": error_msg
                    })
                    if attempt < 3:
                        retry_queue_new.append({**config, "attempt": attempt})
                else:
                    still_retrying.append(refresh_obj)
            retrying_now = still_retrying

            if retrying_now:
                print("⏳ Waiting 60 seconds before retry status check...")
                time.sleep(60)

        retry_queue = retry_queue_new

    # SAVE LOG FILE
    log_df = pd.DataFrame(refresh_log)
    today_str = time.strftime('%d-%m-%Y')
    filename = f'refresh_log_{today_str}.csv'
    log_df.to_csv(filename, index=False)
    print(f"\n📄 Refresh log saved as {filename} in {os.getcwd()}")

if __name__ == "__main__":
    main()
