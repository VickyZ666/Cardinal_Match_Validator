import os
import re
import html
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) 

QUERY = """
WITH ranked_submissions AS (
  SELECT
    s.person_id,
    s.job_id,
    j.name AS job_name,
    j.description AS job_description,
    j.experience_years,
    j.nice_to_have_skills,
    j.skills,
    j.school_names,
    j.location,
    j.prefered_titles,
    j.location_preference,
    j.employment_type,
    j.add_notes,
    p.cv_url,
    s.submission_type,
    CASE
      WHEN s.submission_type = 'hired' THEN 1
      WHEN s.submission_type = 'offer' THEN 2
      WHEN s.submission_type = 'second_interview' THEN 3
      WHEN s.submission_type = 'first_interview' THEN 4
      WHEN s.submission_type = 'recruiter_screen' THEN 5
      WHEN s.submission_type = 'applicant' THEN 6
      WHEN s.submission_type = 'submitted' THEN 7
      WHEN s.submission_type = 'lead' THEN 8
      WHEN s.submission_type = 'onhold' THEN 9
      WHEN s.submission_type = 'reject' THEN 10
      ELSE 11
    END AS status_rank,
    ROW_NUMBER() OVER (
      PARTITION BY s.person_id, s.job_id
      ORDER BY
        CASE
          WHEN s.submission_type = 'hired' THEN 1
          WHEN s.submission_type = 'offer' THEN 2
          WHEN s.submission_type = 'second_interview' THEN 3
          WHEN s.submission_type = 'first_interview' THEN 4
          WHEN s.submission_type = 'recruiter_screen' THEN 5
          WHEN s.submission_type = 'applicant' THEN 6
          WHEN s.submission_type = 'submitted' THEN 7
          WHEN s.submission_type = 'lead' THEN 8
          WHEN s.submission_type = 'onhold' THEN 9
          WHEN s.submission_type = 'reject' THEN 10
          ELSE 11
        END
    ) AS rn
  FROM submissions s
  LEFT JOIN jobs j ON s.job_id = j.id
  LEFT JOIN people p ON s.person_id = p.id
  WHERE (
    j.name ILIKE '%software%' OR j.name ILIKE '%developer%' OR
    j.name ILIKE '%frontend%' OR j.name ILIKE '%front end%' OR
    j.name ILIKE '%backend%' OR j.name ILIKE '%back end%' OR
    j.name ILIKE '%full stack%' OR j.name ILIKE '%application%' OR
    j.name ILIKE '%web%' OR j.name ILIKE '%java%' OR j.name ILIKE '%python%' OR
    j.name ILIKE '%ruby%' OR j.name ILIKE '%qa%' OR j.name ILIKE '%devops%' OR
    j.name ILIKE '%site reliability%' OR j.name ILIKE '%ml%' OR
    j.name ILIKE '%machine learning%' OR j.name ILIKE '%ai%' OR
    j.name ILIKE '%data engineer%' OR j.name ILIKE '%dataops%'
  )
  AND p.cv_url IS NOT NULL
  AND p.cv_url <> ''
)
SELECT
  person_id,
  job_id,
  job_name,
  job_description,
  cv_url,
  submission_type,
  experience_years,
  nice_to_have_skills,
  skills,
  school_names,
  prefered_titles,
  location_preference,
  employment_type,
  add_notes,
  location
FROM ranked_submissions
WHERE rn = 1
ORDER BY status_rank, job_name
LIMIT 1;
"""

def clean_text(value):
    if pd.isna(value):
        return value
    value = str(value)
    value = re.sub(r'<[^>]+>', ' ', value)  
    value = html.unescape(value)            
    value = value.replace('\\/', '/').replace('\\\\', '\\')
    value = re.sub(r'[\n\r\t]+', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value

def _combine_locations(row):
    parts = []
    for col in ("location_preference", "location"):
        v = row.get(col)
        if pd.isna(v) or not str(v).strip():
            continue
        for seg in str(v).split(","):
            seg = seg.strip()
            if seg and seg not in parts:
                parts.append(seg)
    return ", ".join(parts)

def get_connection(
    host=None, port=None, user=None, password=None, dbname=None, sslmode="require"
):      
    return psycopg2.connect(
        host=host or os.getenv("PGHOST"),
        port=int(port or os.getenv("PGPORT", "5432")),
        user=user or os.getenv("PGUSER"),
        password=password or os.getenv("PGPASSWORD"),
        dbname=dbname or os.getenv("PGDATABASE"),
    )

def fetch_dataframe(conn) -> pd.DataFrame:
    return pd.read_sql(QUERY, conn)

def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].map(clean_text)

    # Build API fields
    df["notes"] = df["add_notes"]
    df["target_locations"] = df.apply(_combine_locations, axis=1)
    df["must_have_keyword"] = df["skills"]
    df["nice_to_have_keyword"] = df["nice_to_have_skills"]
    df["education_preferences"] = df["school_names"]
    df["experience"] = df["experience_years"]
    df["current_titles"] = df["prefered_titles"]

    # Drop source columns
    df = df.drop(
        columns=[
            "location_preference",
            "add_notes",
            "location",
            "skills",
            "nice_to_have_skills",
            "school_names",
            "experience_years",
            "prefered_titles",
        ]
    )

    df = df.where(pd.notna(df), None)
    return df

def save_results(df: pd.DataFrame, path: str = "results.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False)

def fetch_and_save(
    out_path: str = "results.json",
    *,
    host=None, port=None, user=None, password=None, dbname=None, sslmode="require"
):
    """Top-level helper you can call from other files."""
    with get_connection(host, port, user, password, dbname, sslmode) as conn:
        df = fetch_dataframe(conn)
    df = transform_dataframe(df)
    save_results(df, out_path)
    return df

if __name__ == "__main__":
    fetch_and_save("results.json")
    print("saved")
