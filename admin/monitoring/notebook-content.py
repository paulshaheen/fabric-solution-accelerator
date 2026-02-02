# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2e757f60-7c56-4fc4-ae1c-5db13cfc020d",
# META       "default_lakehouse_name": "Diagnostic_Lakehouse",
# META       "default_lakehouse_workspace_id": "aba5d898-6b6a-4c5b-af11-62bb9163e914",
# META       "known_lakehouses": [
# META         {
# META           "id": "2e757f60-7c56-4fc4-ae1c-5db13cfc020d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Creat Schema and tables

# CELL ********************

# ============================================
# Central Logging Lakehouse Bootstrap (Fabric)
# - Creates standard folder structure in Files/
# - Creates standardized Delta tables in Tables/
# Idempotent: safe to rerun
# ============================================

from notebookutils import fs
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


spark = SparkSession.builder.getOrCreate()

# ----------------------------
# 1) Lakehouse folder structure (Files/)
# ----------------------------
FOLDERS = [
    "Files/_control/schemas",
    "Files/_control/checkpoints",
    "Files/_control/etl_runs",
    "Files/bronze/onelake_diagnostics_raw",
    "Files/bronze/workspace_monitoring_raw",
    "Files/silver/onelake_diagnostics_normalized",
    "Files/silver/workspace_monitoring_normalized",
    "Files/gold/monitoring_marts"
]

for p in FOLDERS:
    fs.mkdirs(p)

print("✅ Folder structure created/verified:")
for p in FOLDERS:
    print(" -", p)

# Optional: quick sanity listing (won't fail if empty)
print("\n📁 Files/_control contents:", fs.ls("Files/_control"))


# ----------------------------
# 2) Delta tables (Tables/)
# ----------------------------
# A) OneLake diagnostics normalized table (recommended)
# OneLake diagnostics streams data access events as JSON logs into a Lakehouse you choose. [1](https://teams.microsoft.com/l/message/19:6b68c5ae-277d-46fc-881f-658969ed280b_81cd5d98-a2b9-45bd-885c-bf99e02bc629@unq.gbl.spaces/1764767997020?context=%7B%22contextType%22:%22chat%22%7D)
spark.sql("""
CREATE TABLE IF NOT EXISTS log_event_access (
  EventId              STRING,
  EventTimeUtc         TIMESTAMP,
  IngestedTimeUtc      TIMESTAMP,
  WorkspaceId          STRING,
  WorkspaceName        STRING,
  ItemId               STRING,
  ItemKind             STRING,
  ItemName             STRING,
  Operation            STRING,
  Path                 STRING,
  Source               STRING,
  PrincipalId          STRING,
  PrincipalType        STRING,
  PrincipalUpn         STRING,
  ClientIp             STRING,
  RequestId            STRING,
  Result               STRING,
  HttpStatus           STRING,
  DurationMs           BIGINT,
  CorrelationId        STRING,
  RawJson              STRING
)
USING DELTA
""")

# B) Workspace monitoring job event logs (aligned to published ItemJobEventLogs schema)
# Item job event logs schema (Workspace monitoring) includes these columns. [2](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-local-model-context-protocol)
spark.sql("""
CREATE TABLE IF NOT EXISTS log_job_event (
  Timestamp                 TIMESTAMP,
  ItemId                     STRING,
  ItemKind                   STRING,
  ItemName                   STRING,
  WorkspaceId                STRING,
  WorkspaceName              STRING,
  CapacityId                 STRING,
  DurationMs                 BIGINT,
  ExecutingPrincipalId       STRING,
  ExecutingPrincipalType     STRING,
  WorkspaceMonitoringTableName STRING,
  JobInstanceId              STRING,
  JobInvokeType              STRING,
  JobType                    STRING,
  JobStatus                  STRING,
  JobDefinitionObjectId      STRING,
  JobScheduleTime            TIMESTAMP,
  JobStartTime               TIMESTAMP,
  JobEndTime                 TIMESTAMP,

  -- Extensions (helpful when you export/copy from monitoring sources)
  IngestedTimeUtc            TIMESTAMP,
  RawJson                    STRING
)
USING DELTA
""")

# C) ETL / pipeline self-observability (your own ingestion runs)
spark.sql("""
CREATE TABLE IF NOT EXISTS log_etl_run (
  RunId                STRING,
  SourceSystem         STRING,
  WorkspaceId          STRING,
  WorkspaceName        STRING,
  PipelineOrJobName    STRING,
  RunStartUtc          TIMESTAMP,
  RunEndUtc            TIMESTAMP,
  Status               STRING,
  RowsRead             BIGINT,
  RowsWritten          BIGINT,
  FilesProcessed       BIGINT,
  ErrorMessage         STRING,
  CommitHash           STRING,
  IngestedTimeUtc      TIMESTAMP
)
USING DELTA
""")

# D) Optional lightweight dimensions (handy for reporting)
spark.sql("""
CREATE TABLE IF NOT EXISTS dim_workspace (
  WorkspaceId     STRING,
  WorkspaceName   STRING,
  CapacityId      STRING,
  Environment     STRING,
  Owner           STRING,
  UpdatedAtUtc    TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dim_item (
  ItemId          STRING,
  ItemKind        STRING,
  ItemName        STRING,
  WorkspaceId     STRING,
  Domain          STRING,
  UpdatedAtUtc    TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dim_principal (
  PrincipalId     STRING,
  PrincipalType   STRING,
  PrincipalUpn    STRING,
  DisplayName     STRING,
  UpdatedAtUtc    TIMESTAMP
)
USING DELTA
""")

print("\n✅ Delta tables created/verified:")
for t in ["log_event_access", "log_job_event", "log_etl_run", "dim_workspace", "dim_item", "dim_principal"]:
    print(" -", t)

# Stamp a schema version record (optional but recommended)
schema_stamp_path = "Files/_control/schemas/schema_version.txt"
stamp = f"{datetime.utcnow().isoformat()}Z | central-logging-schema v1\n"
fs.put(schema_stamp_path, stamp, overwrite=True)
print(f"\n🧾 Wrote schema stamp to {schema_stamp_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Script to Move Data into Bronze from all linked OneLake Logs

# CELL ********************

# ------------------------------------------------------------
# OneLake Diagnostics → Bronze Landing Script
# Searches ONLY under "DiagnosticLogs"
# Copies new JSON files into Bronze folder
# Idempotent (checkpoint-based)
# ------------------------------------------------------------

# -----------------------
# CONFIGURATION
# -----------------------
SEARCH_ROOT = "Files/DiagnosticLogs"
BRONZE_DIR  = "Files/bronze/onelake_diagnostics_raw"
CHECKPOINT  = "Files/control/checkpoints/onelake_diag_last_copy.txt"

# -----------------------
# HELPERS
# -----------------------
def read_checkpoint(path: str) -> str:
    if fs.exists(path):
        return fs.head(path, 2048).strip()
    return ""

def write_checkpoint(path: str, value: str):
    fs.put(path, value, overwrite=True)

def list_json_files(root: str, max_files: int = 200000):
    """
    Recursively list .json files under a folder
    """
    files = []
    stack = [root]

    while stack and len(files) < max_files:
        current = stack.pop()
        try:
            entries = fs.ls(current)
        except Exception:
            continue

        for e in entries:
            path = e.path
            if getattr(e, "isDir", False) or path.endswith("/"):
                stack.append(path)
            elif path.lower().endswith(".json"):
                files.append(path)

    return files

# -----------------------
# PREP
# -----------------------
fs.mkdirs(BRONZE_DIR)
fs.mkdirs("Files/control/checkpoints")

print(f"🔍 Searching diagnostics under: {SEARCH_ROOT}")

# -----------------------
# DISCOVER FILES
# -----------------------
json_files = sorted(list_json_files(SEARCH_ROOT))

if not json_files:
    print("⚠️ No JSON files found under DiagnosticLogs.")
    print("   - Verify OneLake diagnostics is enabled")
    print("   - Trigger a data access event")
    raise SystemExit(0)

print(f"✅ Found {len(json_files)} diagnostic JSON files")

# -----------------------
# INCREMENTAL COPY
# -----------------------
last_checkpoint = read_checkpoint(CHECKPOINT)
print(f"📍 Last checkpoint: {last_checkpoint or '<none>'}")

new_files = [f for f in json_files if f > last_checkpoint]

if not new_files:
    print("✅ No new files to copy")
    raise SystemExit(0)

print(f"📦 New files to copy: {len(new_files)}")

copied = 0

for src in new_files:
    # Flatten filename to avoid deep folder trees
    filename = src.split("/")[-1]
    dst = f"{BRONZE_DIR}/{filename}"

    if not fs.exists(dst):
        fs.cp(src, dst)
        copied += 1

# -----------------------
# UPDATE CHECKPOINT
# -----------------------
new_checkpoint = max(new_files)
write_checkpoint(CHECKPOINT, new_checkpoint)

print("✅ Copy complete")
print(f"   Files copied : {copied}")
print(f"   New checkpoint: {new_checkpoint}")
print(f"   Completed at : {datetime.utcnow().isoformat()}Z")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Process Files

# CELL ********************

# ============================================================
# OneLake Diagnostics Normalization (Bronze JSON -> Delta table)
# Target: log_event_access
# Notes:
#  - Uses data.* fields (matches your sample payload)
#  - Stores RawJson for forward compatibility
#  - Idempotent: safe to rerun
# ============================================================

spark = SparkSession.builder.getOrCreate()

# --------------- CONFIG ----------------
RAW_DIR = "Files/bronze/onelake_diagnostics_raw"   # <-- set this to your bronze folder
TARGET_TABLE = "log_event_access"                  # Delta table name
MULTILINE_JSON = True                              # set False if files are JSONL (one json per line)
MAX_FILES = 200000

# --------------- HELPERS ----------------
def list_json_files(root: str, max_files: int = MAX_FILES):
    """Recursively list .json files under a Lakehouse Files/ folder."""
    stack = [root]
    out = []
    while stack and len(out) < max_files:
        cur = stack.pop()
        try:
            entries = fs.ls(cur)
        except Exception:
            continue
        for e in entries:
            p = e.path
            # notebookutils often uses trailing "/" for directories
            is_dir = getattr(e, "isDir", None)
            if is_dir is None:
                is_dir = p.endswith("/")
            if is_dir:
                stack.append(p)
            elif p.lower().endswith(".json"):
                out.append(p)
    return sorted(out)

def ensure_target_table():
    """Create the target Delta table if it doesn't exist."""
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        EventId           STRING,
        EventTimeUtc      TIMESTAMP,
        IngestedTimeUtc   TIMESTAMP,

        Category          STRING,
        EventType         STRING,
        SchemaVersion     STRING,
        ResourceId        STRING,

        TenantId          STRING,
        CapacityId        STRING,
        WorkspaceId       STRING,
        WorkspaceName     STRING,

        ItemId            STRING,
        ItemType          STRING,
        DirectoryId       STRING,
        Resource          STRING,
        ServiceEndpoint   STRING,
        IsShortcut        BOOLEAN,

        OperationCategory STRING,
        OperationName     STRING,
        AccessStartTime   TIMESTAMP,
        AccessEndTime     TIMESTAMP,
        DurationMs        BIGINT,

        ExecutingPrincipalId   STRING,
        ExecutingPrincipalType STRING,
        PrincipalUpn           STRING,
        AuthType               STRING,
        CallerIpAddress        STRING,
        HttpStatusCode         INT,
        CorrelationId          STRING,
        OriginatingApp         STRING,

        RawJson           STRING
    )
    USING DELTA
    """)

def j(path):
    """Convenience for get_json_object against RawJson."""
    return F.get_json_object(F.col("RawJson"), path)

def to_ts(col_expr):
    """Parse ISO timestamps safely."""
    return F.to_timestamp(col_expr)

# --------------- RUN ----------------
ensure_target_table()

json_files = list_json_files(RAW_DIR)
print(f"Found {len(json_files)} JSON files under {RAW_DIR}")
if not json_files:
    raise RuntimeError(f"No JSON files found under {RAW_DIR}. Check your bronze path.")

# Load JSON
reader = spark.read
if MULTILINE_JSON:
    reader = reader.option("multiLine", "true")

raw_df = reader.json(json_files).withColumn("IngestedTimeUtc", F.current_timestamp())

# Build a stable RawJson string from the entire parsed object (best for schema drift)
raw_cols = [F.col(c) for c in raw_df.columns if c != "RawJson"]
raw_df = raw_df.withColumn("RawJson", F.to_json(F.struct(*raw_cols)))

# Normalize (match your sample: top-level + data.*)
norm_df = (
    raw_df.select(
        # Top-level
        j("$.id").alias("EventId"),
        to_ts(j("$.time")).alias("EventTimeUtc"),
        F.col("IngestedTimeUtc"),

        j("$.category").alias("Category"),
        j("$.type").alias("EventType"),
        j("$.schemaVersion").alias("SchemaVersion"),
        j("$.resourceId").alias("ResourceId"),

        # data.* fields (these are the ones you pasted)
        j("$.data.tenantId").alias("TenantId"),
        j("$.data.capacityId").alias("CapacityId"),
        j("$.data.workspaceId").alias("WorkspaceId"),
        j("$.data.workspaceName").alias("WorkspaceName"),

        j("$.data.itemId").alias("ItemId"),
        j("$.data.itemType").alias("ItemType"),
        j("$.data.directory").alias("DirectoryId"),
        j("$.data.resource").alias("Resource"),
        j("$.data.serviceEndpoint").alias("ServiceEndpoint"),
        j("$.data.isShortcut").cast("boolean").alias("IsShortcut"),

        j("$.data.operationCategory").alias("OperationCategory"),
        j("$.data.operationName").alias("OperationName"),
        to_ts(j("$.data.accessStartTime")).alias("AccessStartTime"),
        to_ts(j("$.data.accessEndTime")).alias("AccessEndTime"),

        # Duration (prefer explicit duration; else compute from start/end if available)
        F.coalesce(
            j("$.data.durationMs").cast("bigint"),
            (
                (F.unix_timestamp(to_ts(j("$.data.accessEndTime"))) -
                 F.unix_timestamp(to_ts(j("$.data.accessStartTime")))) * 1000
            ).cast("bigint")
        ).alias("DurationMs"),

        j("$.data.executingPrincipalId").alias("ExecutingPrincipalId"),
        j("$.data.executingPrincipalType").alias("ExecutingPrincipalType"),
        j("$.data.executingPrincipalUpn").alias("PrincipalUpn"),
        j("$.data.authType").alias("AuthType"),
        j("$.data.callerIpAddress").alias("CallerIpAddress"),
        j("$.data.httpStatusCode").cast("int").alias("HttpStatusCode"),
        j("$.data.correlationId").alias("CorrelationId"),
        j("$.data.originatingApp").alias("OriginatingApp"),

        F.col("RawJson").alias("RawJson")
    )
)

# Append to Delta
(norm_df
 .write
 .format("delta")
 .mode("append")
 .saveAsTable(TARGET_TABLE)
)

print(f"✅ Appended {norm_df.count()} rows into {TARGET_TABLE}")
print("✅ Done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from log_event_access

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
