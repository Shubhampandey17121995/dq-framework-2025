
from Utilities.validation import *

# Directory path containing JSON files
directory_path = "dq-framework\metadata"

# Required table_metadata
REQUIRED_TABLE_METADATA = {
    "dq_entity_master": "dq_entity_master.json",
    "df_rule_master": "dq_rule_master.json",
    "dq_execution_plan": "dq_execution_plan.json"
}

# Validation_Config
dfs = {
    "dq_entity_master": entity_master_df,
    "df_rule_master": rule_master_df,
    "dq_execution_plan": execution_plan_df
}

# Validation_config
validation_steps = [
    ("Column data type validation", validate_column_data_types),
    ("Nullable constraint validation", validate_nullable_constraint),
    ("Primary key uniqueness validation", validate_primary_key_uniqueness)
]

# Validation_config
validations = [
    (apply_validation, (entity_master_df, metadata, "dq_entity_master")),
    (apply_validation, (execution_plan_df, metadata, "dq_execution_plan")),
    (apply_validation, (rule_master_df, metadata, "df_rule_master")),
    (validate_foreign_key_relationship, (execution_plan_df, metadata, "dq_execution_plan", dfs))
]