import csv
import json
from pathlib import Path


def load_csv(filepath: Path) -> list[dict]:
    with filepath.open("r", encoding="utf-8") as file:
        return [dict(row) for row in csv.DictReader(file)]


def load_json(filepath: Path) -> list[dict]:
    with filepath.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if isinstance(payload, dict) and len(payload) == 1:
        value = next(iter(payload.values()))
        if isinstance(value, list):
            payload = value
    if not isinstance(payload, list):
        raise ValueError("JSON feed must be a list of records or a single-key wrapped list")
    return [dict(row) for row in payload]


def load_txt(filepath: Path) -> list[dict]:
    rows: list[dict] = []
    with filepath.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue
            row: dict = {}
            for part in stripped.split(","):
                if ":" not in part:
                    continue
                key, value = part.split(":", 1)
                row[key.strip()] = value.strip()
            if row:
                rows.append(row)
    return rows


def load_records(filepath: Path) -> list[dict]:
    ext = filepath.suffix.lower()
    if ext == ".csv":
        return load_csv(filepath)
    if ext == ".json":
        return load_json(filepath)
    if ext == ".txt":
        return load_txt(filepath)
    raise ValueError(f"Unsupported file type: {ext}")
