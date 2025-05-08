import json
 
 
def parse_us_common(json_data):
    data = json_data.get("data", [{}])[0]
    scores = data.get("scores", {})
 
    def add_scores(section, parent_key=None):
        updated_section = {}
        for key, value in section.items():
            if isinstance(value, dict):
                updated_section[key] = add_scores(value, key)
            elif isinstance(value, list):
                if key == "primary_naics_2017":
                    updated_section[key] = [
                        {"naics_code": item.get("code", ""), "naics_desc": item.get("desc", "")}
                        for item in value
                    ]
                elif key == "primary_sic":
                    updated_section[key] = [
                        {"sic_code": item.get("code", ""), "sic_desc": item.get("desc", "")}
                        for item in value
                    ]
                else:
                    updated_section[key] = value
            else:
                updated_section[key] = {"value": value, "score": scores.get(key, "")}
        return updated_section

 
    # Extracting Firmographics with scores
    firmographics = add_scores(data.get("facts", {}))
 
    # Extracting Broker Details with scores
    options = data.get("options", {})
    misc = data.get("cleansed_input", {})

    broker_details = add_scores(
        {
            "broker_name": options.get("broker_name", ""),
            "broker_address": options.get("broker_address", ""),
            "broker_city": options.get("broker_city", ""),
            "broker_state": options.get("broker_state", ""),
            "broker_postal_code": options.get("broker_postal_code", ""),
            "broker_contact_points": options.get("broker_contact_points", ""),
            "broker_email": options.get("broker_email", ""),
            "broker_contact_phone": options.get("broker_contact_phone", ""),
            "submission_received_date": options.get("submission_received_date", "")
        }
    )
 
    # Extracting Product Details with scores
    product_details = add_scores(
        {
            "normalized_product": options.get(
                "normalized_product", []
            ),  # Lists remain unchanged
            "policy_inception_date": options.get("policy_inception_date", ""),
            "end_date": options.get("end_date", ""),
            "submission_received_date": options.get("submission_received_date", ""),
            "target_premium": options.get("target_premium", ""),
            "underwriter": options.get("underwriter", ""),
            "underwriter_email": options.get("underwriter_email", ""),
            "workers_comp_estimated_annual_payroll": options.get(
                "workers_comp_estimated_annual_payroll", ""
            ),
            "document_date": options.get("document_date", ""),
            "expiring_premium": options.get("expiring_premium", ""),
            "lob": options.get("lob", ""),
        }
    )
 
    # Extracting Limits and Coverages with scores
    limits_and_coverages = add_scores(
        {
            "100_pct_limit": options.get("100_pct_limit", {}),  # Handling nested dict
            "normalized_coverage": options.get(
                "normalized_coverage", []
            ),  # Lists remain unchanged
            "coverage": options.get("coverage", []),  # Lists remain unchanged
            "coverage_details": json_data.get("additional_data")
        }
    )
 
    # Returning the structured data
    structured_data = {
        "Firmographics": firmographics,
        "Broker_Details": broker_details,
        "Product_Details": product_details,
        "Limits_and_Coverages": limits_and_coverages,
        "Legal_Entity_Type": ""
    }
 
    return structured_data
 
 
def parse_property_json(property_json):
    parsed_data = []
 
    for item in property_json.get("data", []):
        facts = item.get("facts", {})
        options = item.get("options", {})
        scores = item.get("scores", {})
 
        # Skip entries where both building_number and location_address are missing
        if not facts.get("building_number") and not facts.get("location_address"):
            continue
 
        # Create standard_facts with scores
        standard_facts = {
            key: {"value": value, "score": scores.get(key, "")}
            for key, value in facts.items()
        }
 
        # Create limits section, ensuring only keys present in input JSON are included
        limits = {}
        if "100_pct_coverage_limits" in options:
            limits["100_pct_coverage_limits"] = {
                k: {"value": v, "score": scores.get("100_pct_coverage_limits", "")}
                for k, v in options["100_pct_coverage_limits"].items()
                if k
                in options[
                    "100_pct_coverage_limits"
                ]  # Only include keys that exist in input
            }
 
        if "100_pct_limit" in options:
            limits["100_pct_limit"] = {
                "value": options["100_pct_limit"],
                "score": scores.get("100_pct_limit", ""),
            }
 
        # Create building_details section
        building_details = {
            "location_doc_id": {
                "value": options.get("location_doc_id", ""),
                "score": scores.get("location_doc_id", ""),
            },
            "atc_occupancy_description": {
                "value": options.get("atc_occupancy_description", ""),
                "score": scores.get("atc_occupancy_description", ""),
            },
        }
 
        parsed_data.append(
            {
                "standard_facts": standard_facts,
                "limits": limits,
                "building_details": building_details,
            }
        )
 
    return parsed_data
 
 
def parse_advanced_property(input_json):
    data = input_json
    advanced_property = []
 
    standard_facts_keys = {
        "building_number",
        "location_address",
        "location_city",
        "location_state",
        "location_postal_code",
        "location_country",
        "location_occupancy_description",
        "year_built",
    }
 
    for entry in data["data"]:
        facts = entry.get("facts", {})
        options = entry.get("options", {})
        scores = entry.get("scores", {})
 
        # Skip if both building_number and location_address are missing or empty
        if not facts.get("building_number") and not facts.get("location_address"):
            continue
 
        advanced_entry = {
            "advanced_facts": {},
            "rms_details": {},
            "atc_details": {},
            "protection_details": {},
        }
 
        # Separate standard facts and advanced facts
        for key, value in facts.items():
            if key not in standard_facts_keys:
                advanced_entry["advanced_facts"][key] = {
                    "value": value,
                    "score": scores.get(key, ""),
                }
 
        # Separate RMS details
        for key in ["rms_construction_code", "rms_construction_description"]:
            if key in options:
                advanced_entry["rms_details"][key] = {
                    "value": options[key],
                    "score": scores.get(key, ""),
                }
 
        # Separate ATC details
        for key in ["atc_construction_code", "atc_construction_description"]:
            if key in options:
                advanced_entry["atc_details"][key] = {
                    "value": options[key],
                    "score": scores.get(key, ""),
                }
 
        # Separate Protection details
        for key in ["burglar_alarm_type"]:
            if key in options:
                advanced_entry["protection_details"][key] = {
                    "value": options[key],
                    "score": scores.get(key, ""),
                }
 
        advanced_property.append(advanced_entry)
 
    return advanced_property
 
 
def parse_general_liability(gl_json):
    data = gl_json
 
    first_item = data.get("data", [{}])[0]
    facts = first_item.get("facts", {})
    options = first_item.get("options", {})
    scores = first_item.get("scores", {})

    # Process gl_facts with scores
    gl_facts = {
        key: {"value": value, "score": scores.get(key, "")}
        for key, value in facts.items()
    }
 
    # Process gl_options with scores
    gl_options = {
        key: {"value": value, "score": scores.get(key, "")}
        for key, value in options.items()
    }
 
    processed_gl = {"gl_facts": gl_facts, "gl_options": gl_options}
 
    return processed_gl
 
 
def parse_auto(auto_json):
    data = auto_json
    first_item = data.get("data", [{}])[0]  # Safely get first dict from list

    facts = first_item.get("facts", {})
    scores = first_item.get("scores", {})

    auto_facts = {}

    for key, value in facts.items():
        if isinstance(value, (dict, list)):
            auto_facts[key] = {"value": value, "score": scores.get(key, "")}
        else:
            auto_facts[key] = {"value": str(value), "score": scores.get(key, "")}

    return {"Auto": {"auto_facts": auto_facts}}
 
    return transformed_auto