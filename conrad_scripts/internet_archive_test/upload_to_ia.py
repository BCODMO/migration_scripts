import os
import json
import logging
import tempfile
import hashlib

import requests
from pathlib import Path
from internetarchive import upload, get_item
from dotenv import load_dotenv

load_dotenv()

BCODMO_API_KEY = os.environ["BCODMO_API_KEY"]
IA_ACCESS_KEY = os.environ["IA_ACCESS_KEY"]
IA_SECRET_KEY = os.environ["IA_SECRET_KEY"]

DOI_URL = "https://prod.bco-dmo.org/doi/getUrls?Id=986789&DoiPrefix=10.26008&DoiTypeId=1912&DoiType=Dataset&Version=1"
COLLECTION = "test_collection"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def fetch_doi_package(url):
    """GET the BCO-DMO API, return parsed JSON response."""
    logger.info("Fetching DOI package info from %s", url)
    resp = requests.get(url, headers={"x-api-key": BCODMO_API_KEY})
    resp.raise_for_status()
    data = resp.json()
    logger.info("Got package: Id=%s, Version=%s, Status=%s",
                data["data"]["Id"], data["data"]["Version"], data["data"]["Status"])
    return data


def download_file(url, dest_path, expected_md5=None):
    """Download a single file from a URL to dest_path.
    Optionally verifies MD5 checksum. Returns the actual MD5."""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    md5 = hashlib.md5()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            md5.update(chunk)

    actual_md5 = md5.hexdigest()
    if expected_md5 and actual_md5 != expected_md5:
        raise ValueError(
            f"MD5 mismatch for {dest_path.name}: expected {expected_md5}, got {actual_md5}"
        )
    return actual_md5


def download_datapackage(files_dict, dest_dir):
    """Download datapackage_public.json from the API's presigned URL,
    save it as datapackage.json. Returns (local_path, parsed_json)."""
    dest_dir = Path(dest_dir)

    file_info = files_dict.get("datapackage_public.json")
    if not file_info:
        raise ValueError("datapackage_public.json not found in API response files")

    url = file_info["url"] if isinstance(file_info, dict) else file_info
    expected_md5 = file_info.get("md5") if isinstance(file_info, dict) else None

    local_path = dest_dir / "datapackage.json"
    logger.info("Downloading datapackage_public.json -> datapackage.json")
    actual_md5 = download_file(url, local_path, expected_md5)
    logger.info("Downloaded datapackage.json (md5: %s)", actual_md5)

    with open(local_path) as f:
        dp = json.load(f)

    return local_path, dp


def download_resource_files(resources, dest_dir):
    """Download resource files listed in the datapackage's resources array.

    Each resource has 'filename' and 'path' (a public download URL).
    Returns dict mapping filename -> local_path.
    """
    dest_dir = Path(dest_dir)
    downloaded = {}

    for resource in resources:
        filename = resource["filename"]
        url = resource["path"]
        local_path = dest_dir / filename

        logger.info("Downloading resource: %s", filename)
        actual_md5 = download_file(url, local_path)
        logger.info("Downloaded %s (%s bytes, md5: %s)",
                    filename, resource.get("size", "?"), actual_md5)

        downloaded[filename] = str(local_path)

    return downloaded


def _ld_val(obj, key, default=None):
    """Extract a value from a JSON-LD style object.
    Handles both {"@value": "..."} wrappers and plain strings."""
    val = obj.get(key, default)
    if isinstance(val, dict) and "@value" in val:
        return val["@value"]
    return val


def parse_datapackage_metadata(datapackage_path):
    """Parse datapackage.json (BCO-DMO public format) to extract title,
    description, creators, license, keywords, DOI, dates, and spatial info
    for IA metadata.

    The rich metadata lives under the "bcodmo:" key in JSON-LD format,
    where most values are wrapped as {"@value": "...", "@type": "..."}.
    """
    if not datapackage_path or not Path(datapackage_path).exists():
        logger.warning("No datapackage.json found, skipping rich metadata extraction")
        return {}

    with open(datapackage_path) as f:
        dp = json.load(f)

    meta = {}
    bcodmo = dp.get("bcodmo:", {})

    # Title — use top-level title (plain string), fall back to bcodmo: title
    if "title" in dp:
        meta["title"] = dp["title"]
    elif "title" in bcodmo:
        meta["title"] = _ld_val(bcodmo, "title")

    # Description — from bcodmo: abstract
    abstract = _ld_val(bcodmo, "abstract")
    if abstract:
        meta["description"] = abstract

    # Creators — build from hasAgentWithRole, resolving author order
    # First build a lookup of person ID -> name from hasAgentWithRole
    person_names = {}
    agents = bcodmo.get("hasAgentWithRole", [])
    for agent in agents:
        performed_by = agent.get("performedBy", {})
        person_id = performed_by.get("@id", "")
        name = _ld_val(performed_by, "label")
        if person_id and name:
            person_names[person_id] = name

    # Use the author list (which has the correct order) to build creators
    authors = bcodmo.get("author", [])
    if authors and person_names:
        creator_names = []
        for author_ref in authors:
            person_id = author_ref.get("performedBy", {}).get("@id", "")
            if person_id in person_names:
                creator_names.append(person_names[person_id])
        if creator_names:
            meta["creator"] = creator_names if len(creator_names) > 1 else creator_names[0]

    # License
    licenses = bcodmo.get("license", [])
    if licenses:
        lic = licenses[0]
        lic_url = _ld_val(lic, "url")
        if lic_url:
            meta["licenseurl"] = lic_url
        lic_label = _ld_val(lic, "label")
        if lic_label:
            meta["license"] = lic_label

    # Keywords
    keywords = bcodmo.get("keywords", [])
    if keywords:
        kw_list = [_ld_val(kw, "label") for kw in keywords if _ld_val(kw, "label")]
        if kw_list:
            meta["subject"] = kw_list if len(kw_list) > 1 else kw_list[0]

    # DOI
    identifier = bcodmo.get("identifier", {})
    doi_val = _ld_val(identifier, "identifierValue")
    if doi_val:
        meta["doi"] = doi_val

    # Published date
    published = bcodmo.get("publishedDate", {})
    pub_date = _ld_val(published, "time:inXSDDate")
    if pub_date:
        meta["date"] = pub_date

    # Spatial extent description
    spatial = bcodmo.get("spatialExtent", {})
    spatial_desc = _ld_val(spatial, "description")
    if spatial_desc:
        meta["coverage"] = spatial_desc

    # Funding
    funders = bcodmo.get("fundedBy", [])
    if funders:
        funder_names = [_ld_val(f, "label") for f in funders if _ld_val(f, "label")]
        awards = []
        for f in funders:
            for award in f.get("grantedAward", []):
                award_label = _ld_val(award, "label")
                if award_label:
                    awards.append(award_label)
        if funder_names:
            meta["funding"] = "; ".join(funder_names)
        if awards:
            meta["award"] = "; ".join(awards)

    logger.info("Extracted datapackage metadata: %s", list(meta.keys()))
    return meta


def build_ia_metadata(package_data, datapackage_meta):
    """Merge BCO-DMO API metadata + datapackage metadata
    into an IA-compatible metadata dict."""
    metadata = {
        "mediatype": "data",
        "collection": COLLECTION,
        "publisher": "BCO-DMO",
        # Custom BCO-DMO fields
        "bcodmo_id": package_data["Id"],
        "bcodmo_doi_type": package_data["DoiType"],
        "bcodmo_version": package_data["Version"],
        "bcodmo_status": package_data["Status"],
    }

    if "CreatedAt" in package_data:
        metadata["bcodmo_created_at"] = package_data["CreatedAt"]
    if "UpdatedAt" in package_data:
        metadata["bcodmo_updated_at"] = package_data["UpdatedAt"]

    # Standard IA fields from datapackage, with fallbacks
    metadata["title"] = datapackage_meta.get(
        "title",
        f"BCO-DMO {package_data['DoiType']} {package_data['Id']} v{package_data['Version']}"
    )

    for field in ("description", "creator", "licenseurl", "license",
                  "subject", "coverage", "funding", "award"):
        if field in datapackage_meta:
            metadata[field] = datapackage_meta[field]

    # DOI as both a custom field and IA's external-identifier
    if "doi" in datapackage_meta:
        metadata["bcodmo_doi"] = datapackage_meta["doi"]
        metadata["external-identifier"] = f"urn:doi:{datapackage_meta['doi']}"

    # Date — prefer published date from datapackage, fall back to API CreatedAt
    if "date" in datapackage_meta:
        metadata["date"] = datapackage_meta["date"]
    elif "CreatedAt" in package_data:
        metadata["date"] = package_data["CreatedAt"]

    return metadata


def upload_to_ia(identifier, files_dict, metadata):
    """Upload files to IA item. Creates or updates as needed.
    Uses queue_derive=False and checksum=True."""
    item = get_item(identifier)

    if item.exists:
        logger.info("Item %s already exists — updating in place", identifier)
    else:
        logger.info("Item %s does not exist — creating new item", identifier)

    file_paths = list(files_dict.values())
    logger.info("Uploading %d files to %s: %s", len(file_paths), identifier, list(files_dict.keys()))

    responses = upload(
        identifier,
        files=file_paths,
        metadata=metadata,
        access_key=IA_ACCESS_KEY,
        secret_key=IA_SECRET_KEY,
        queue_derive=False,
        checksum=True,
    )

    for resp in responses:
        if resp.status_code == 200:
            logger.info("Upload response: %s %s", resp.status_code, resp.url)
        else:
            logger.warning("Upload response: %s %s — %s", resp.status_code, resp.url, resp.text)

    return responses


def verify_upload(identifier):
    """Fetch item back and log files + metadata."""
    logger.info("Verifying upload for %s", identifier)
    item = get_item(identifier)

    if not item.exists:
        logger.error("Item %s does not exist after upload!", identifier)
        return

    logger.info("Item URL: https://archive.org/details/%s", identifier)

    logger.info("Files:")
    for f in item.files:
        logger.info("  %s (%s bytes)", f["name"], f.get("size", "?"))

    logger.info("Metadata:")
    for key, value in item.metadata.items():
        logger.info("  %s: %s", key, value)


def main():
    package = fetch_doi_package(DOI_URL)
    data = package["data"]

    with tempfile.TemporaryDirectory() as tmp:
        # Step 1: Download the public datapackage and parse it
        dp_path, dp_json = download_datapackage(data["files"], tmp)

        # Step 2: Download all resource files listed in the datapackage
        resources = dp_json.get("resources", [])
        files = download_resource_files(resources, tmp)

        # Include the datapackage.json itself in the upload
        files["datapackage.json"] = str(dp_path)
        logger.info("Files to upload: %s", list(files.keys()))

        # Step 3: Extract rich metadata from the datapackage
        dp_meta = parse_datapackage_metadata(str(dp_path))

        # Step 4: Build IA identifier and metadata, then upload
        identifier = f"bco-dmo-{data['DoiType'].lower()}-{data['Id']}-v{data['Version']}"
        logger.info("Using identifier: %s", identifier)

        metadata = build_ia_metadata(data, dp_meta)
        logger.info("Metadata: %s", json.dumps(metadata, indent=2, default=str))

        upload_to_ia(identifier, files, metadata)
        verify_upload(identifier)

    logger.info("Done!")


if __name__ == "__main__":
    main()
