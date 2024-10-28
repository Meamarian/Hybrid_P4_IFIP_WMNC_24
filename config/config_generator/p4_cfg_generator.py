import json
import ipaddress
import random

def generate_config_table_1(teid_max, qfi_max, rb_max):
    config = {
        "tables": {
            "ingress::TEIDs_QFIs_to_RBs_RQI": {
                "rules": []
            }
        }
    }

    for teid in range(teid_max + 1):
        for qfi in range(qfi_max + 1):
            teid_binary = format(teid, '032b')
            qfi_binary = format(qfi, '06b')
            binary_concatenated = teid_binary + qfi_binary
            decimal_value = int(binary_concatenated, 2)
                     
            rb_value = str(random.randint(0, rb_max))
            rqi_value = str(random.randint(0, 1))  # Randomly choose 0 or 1 for rqi
            rule = {
                "action": {
                    "data": {
                        "rb": {"value": str(rb_value)},
                        "rqi": {"value": str(rqi_value)}
                    },
                    "type": "ingress::assigning_drb_rqi",
                },
                "name": f"teid_{teid}_qfi_{qfi}",
                "match": {
                    "scalars.qos_metadata_t@teid_qfi": {
                        "value": str(teid)
                    }
                }

            }
            config["tables"]["ingress::TEIDs_QFIs_to_RBs_RQI"]["rules"].append(rule)

    return config

def generate_config_table_2(ip_start, ip_end, teid_max):
    config = {
        "tables": {
            "ingress::UL_ASSIGN_TEID": {
                "rules": []
            }
        }
    }

    current_ip = ipaddress.IPv4Address(ip_start)
    end_ip = ipaddress.IPv4Address(ip_end)

    # Loop through IP addresses within the range
    while current_ip <= end_ip:
        current_teid = 0
        # Create rule for each TEID value up to teid_max
        while current_teid <= teid_max:
            rule = {
                "action": {
                    "data": {
                        "teid": {"value": str(current_teid)}
                    },
                    "type": "ingress::assigning_teid",
                },
                "name": f"ip_{current_ip}_teid_{current_teid}",
                "match": {
                    "ipv4.dstAddr": {
                        "value": str(current_ip)
                    }
                }
            }
            config["tables"]["ingress::UL_ASSIGN_TEID"]["rules"].append(rule)
            current_teid += 1
        # Move to the next IP address
        current_ip += 1

    return config

# Define inputs for table 1 and table 2
table_1_inputs = (64000, 0, 1) # Example teid_max and qfi_max
table_2_inputs = ("10.0.0.0", "10.0.250.0", 0) # Example IP range and teid_max

# Generate table configurations
table_1_config = generate_config_table_1(*table_1_inputs)
table_2_config = generate_config_table_2(*table_2_inputs)

# Combine both table configs
final_config = {**table_1_config, **table_2_config}

# Convert final config to JSON and print
#print(json.dumps(final_config, indent=4))

# Generate filename based on inputs
filename = f"config_table1_{table_1_inputs[0]}_{table_1_inputs[1]}_table2_{table_2_inputs[0]}_{table_2_inputs[1]}_{table_2_inputs[2]}.json"

# Combine both table configs
final_config = {"tables": {}}
final_config["tables"].update(table_1_config["tables"])
final_config["tables"].update(table_2_config["tables"])

# Save final config to file
with open(filename, 'w') as outfile:
    json.dump(final_config, outfile, indent=4)

print(f"Configuration saved to {filename}")
