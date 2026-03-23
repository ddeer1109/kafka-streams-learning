import re
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/tasks"]


def get_service(token_path):
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    return build("tasks", "v1", credentials=creds)


def find_task_list(service, list_name):
    results = service.tasklists().list(maxResults=100).execute()
    for tl in results.get("items", []):
        if tl["title"] == list_name:
            return tl["id"]
    return None


def fetch_tasks(service, list_id):
    results = service.tasks().list(
        tasklist=list_id,
        showCompleted=False,
        showHidden=False,
    ).execute()
    return results.get("items", [])


def parse_tags(text):
    tags = re.findall(r"#(\w+)", text)
    clean = re.sub(r"\s*#\w+", "", text).strip()
    category = None
    for tag in tags:
        category = tag.lower()
        break
    return clean, category, [t.lower() for t in tags]


def complete_task(service, list_id, task_id):
    service.tasks().patch(
        tasklist=list_id,
        task=task_id,
        body={"status": "completed"},
    ).execute()
    log.info("Marked task %s as completed in Google Tasks", task_id)
