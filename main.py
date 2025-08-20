from cvFetch_Parse import pdf_to_text
from dbFetch import fetch_and_save
import pandas as pd
import json
import requests

# This calls the function in dbFetch.py, which fetches K candidate info and stores the JSON object array in result.json
def fetch_db():
    df = fetch_and_save("results.json")

# This calls the function in cvFetch_Parse.py, which parse the pdf CV into txt, and strips extra coding, and appends to the json object in result.json
def fetch_cv_text():
    with open("results.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # This part extracts the pdf (downloaded from url) to txt, and if successful, appends to the JSON object
    for entry in data:
        if entry.get("cv_url"):
            try:
                entry["cvtext"] = pdf_to_text(entry["cv_url"])
            except Exception as e:
                entry["cvtext"] = None
                print(f"Failed to extract text for {entry['cv_url']}: {e}")
        else:
            entry["cvtext"] = None
    # writes to the json file
    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        
def _none_to_empty(obj):
    # This function just makes sure we are not getting nulls
    if isinstance(obj, dict):
        return {k: _none_to_empty(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_none_to_empty(v) for v in obj]
    return "" if obj is None else obj

def request_match():
    """Reads results.json and sends a match request for each entry."""
    with open("results.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    # for each JSON object in array, build the POST request BODY
    for entry in data:
        payload = {
            "job_description": entry.get("job_description", ""),
            "notes": entry.get("notes", ""),
            "sourcing_requirements": {
                "must_have_keyword": entry.get("must_have_keyword", ""),
                "nice_to_have_keyword": entry.get("nice_to_have_keyword", ""),
                "target_locations": entry.get("target_locations", ""),
                "education_preferences": entry.get("education_preferences", ""),
                "target_schools": "",           
                "experience": entry.get("experience", ""),
                "current_titles": entry.get("current_titles", ""),
                "target_companies": "",         
                "employment_type": entry.get("employment_type", ""),
            },
            "resume_service_urls": [
                {
                    "id": str(entry.get("person_id", "")),
                    "url": entry.get("cv_url", ""),
                    "resume_text": entry.get("cvtext", ""),
                }
            ],
        }
        # making sure we don't have null
        payload = _none_to_empty(payload) 
        # sends the POST request
        resp = requests.post("http://localhost:8080/match", json=payload)
        print(f"person_id={entry.get('person_id')}, status={resp.status_code}")
        # If the request is successful, we write the result to the txt log file
        try:
            resp_json = resp.json()
            with open("performance.txt", "a", encoding="utf-8") as log_file:
                for result in resp_json.get("results", []):
                    log_file.write(f"ID: {result.get('id')}\n")
                    log_file.write(f"Score: {result.get('score')}\n")
                    log_file.write(f"Confidence: {result.get('confidence')}\n")
                    log_file.write(f"Consistency: {result.get('consistency')}\n")
                    log_file.write(f"Detailed Scoring: {result.get('detailed_scoring')}\n")
                    log_file.write("-" * 40 + "\n")
        except Exception:
            with open("performance.txt", "a", encoding="utf-8") as log_file:
                log_file.write(f"Response text: {resp.text}\n")
                log_file.write("-" * 40 + "\n")



         
   
if __name__ == "__main__":
    fetch_db()
    fetch_cv_text()
    request_match()
