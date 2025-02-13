def fetch_execution_plan(execution_plan_with_rule_df):
    try:
        plan_list = [tuple(row) for row in execution_plan_with_rule_df.filter(execution_plan_with_rule_df.is_active == "Y").collect()]
        return plan_list
    except Exception as e:
        logger.error(f"Exception occured in fetch_execution_plan(): {e}")

def merge_plans_with_rules(execution_plan_df,rules_df):
    try:
        execution_plan_with_rule_df = execution_plan_df.join(rules_df.select("rule_id", "rule_name"), execution_plan_df.rule_id == rules_df.rule_id, "inner").drop(execution_plan_df.last_update_date).drop(rules_df.rule_id).orderBy("ep_id")
        return execution_plan_with_rule_df
    except Exception as e:
        logger.error(f"Exception occured in merge_plans_with_rules(): {e}")

# fetch path from entity master table path
def fetch_entity_path(entity_master_df):
    try:
        pass
    except Exception as e:
        pass

