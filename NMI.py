import csv
import requests
from datetime import datetime

INPUT_FILE_NAME = "input.csv"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # For test version propose, can be remote
OUT_FILE_NAME = f"output_{timestamp}.csv"

# API headers
headers = {
    "Cache-Control": "no-cache",
    "Ocp-Apim-Subscription-Key": "6c02d3b07c4f4f2881a2fb91b6a1758b",
    "Tally-OrgId": "6501125D-7E58-4EFE-995B-FBDDD2F86F16",
}

# API_ENDPOINT for different types
API_ENDPOINTS = {
    "NMI": "https://deapi.diamond-energy.com/address/nmi_detail?nmi=",
    "Meter": "https://deapi.diamond-energy.com/address/nmi_discovery/meter_serial_number?meter_serial_number=",
}

# Output CSV header
csv_header = [
    "nmi",
    "JurisdictionCode",
    "TransmissionNodeIdentifier",
    "DistributionLossFactorCode",
    "SuburbOrPlaceOrLocality",
    "StateOrTerritory",
    "PostCode",
    "Status",
    "CustomerClassificationCode",
    "Address",
    "Network",
    "MC",
    "MDP",
    "MPB",
    "LoadPerDay",
    "Tariffs",
    "mlfValue",
    "distributionLossFactorCode",
    "dlfValue",
    "MeterType",
    "nmiclassificationcode",
    "NSR",
    "ParentEmbeddedNetworkCode",
    "ChildEmbeddedNetworkCode",
]

output_lines = []

print(f"Output will be written to: {OUT_FILE_NAME}\n")


def get_role_party(roles, role_name):
    if not roles:
        return ""
    for role in roles:
        if role.get("role") == role_name:
            return role.get("party", "")
    return ""

def get_network_tariff_code(response_json):

    tariff_code = ""

    meters = response_json.get("meters", [])
    for meter in meters:
        if meter.get("status") == "C" and meter.get("networkTariffCode"):
            tariff_code = meter.get("networkTariffCode")
            return tariff_code
        registers = meter.get("registers", [])
        for register in registers:
            if register.get("status") == "C" and register.get("networkTariffCode"):
                tariff_code = register.get("networkTariffCode")
                return tariff_code
    streams = response_json.get("streams", [])
    for stream in streams:
        if stream.get("status") == "C" and stream.get("networkTariffCode"):
            tariff_code = stream.get("networkTariffCode")
            return tariff_code
    registers = response_json.get("registers", [])
    for register in registers:
        if register.get("status") == "C" and register.get("networkTariffCode"):
            tariff_code = register.get("networkTariffCode")
            return tariff_code

    return tariff_code



# Read input CSV
with open(INPUT_FILE_NAME, "r") as f_read:
    reader = csv.reader(f_read)

    for line in reader:
        # Skip header row
        if line[0] == "Input":
            continue

        # Skip empty lines
        if not line or len(line) < 2:
            continue

        Input = line[0]
        type = line[1].strip()

        print(f"Processing: {Input} (Type: {type})")

        try:
            # Select the appropriate API endpoint based on type
            if type in API_ENDPOINTS:
                URL = f"{API_ENDPOINTS[type]}{Input}"
            else:
                print(f"  ✗ Unknown type '{type}'. Valid types are: {', '.join(API_ENDPOINTS.keys())}")
                output_lines.append([f"{Input} - Invalid type: {type}"] + [""] * 22)
                continue

            print(f"  Calling: {URL}")

            # Call API
            response = requests.get(URL, headers=headers)

            if response.status_code == 200:
                response_json = response.json()


                master = response_json.get("master", {})
                roles = response_json.get("roles", [])
                meters = response_json.get("meters", [])
                streams = response_json.get("streams", [])

                # Check if API returned no data
                if not master and not roles and not meters and not streams:
                    print(f"  ⚠ No data returned for this {type}")
                    output_lines.append([f"{Input} this {type} has no data"] + [""] * 22)
                    continue

                # Get MC, MDP, MPB from roles
                mc_party = get_role_party(roles, "RP")
                mdp_party = get_role_party(roles, "MDP")
                mpb_party = get_role_party(roles, "MPB")
                lnsp_party = get_role_party(roles, "LNSP")



                meter_type = ""
                next_read = ""
                if meters and len(meters) > 0:
                    current_meter = None
                    for meter in meters:
                        if meter.get("status") == "C":
                            current_meter = meter
                            break

                    if current_meter is None and len(meters) > 0:
                        current_meter = meters[0]

                    if current_meter:
                        meter_type = current_meter.get("installationTypeCode", "")
                        next_read = current_meter.get("nextScheduledReadDate", "")

                avg_daily_load = ""
                if streams and len(streams) > 0:
                    for stream in streams:
                        if stream.get("suffix") == "E1" and stream.get("status") == "A":
                            avg_daily_load = stream.get("averagedDailyLoad", "")
                            break

                # Build combined address
                combined_address = master.get("combinedAddress", "")
                # Get network tariff code from status is 'C'
                network_tariff = get_network_tariff_code(response_json)
                output_row = [
                    Input,
                    master.get("jurisdictionCode", ""),
                    master.get("transmissionNodeIdentifier", ""),
                    master.get("distributionLossFactorCode", ""),
                    master.get("suburbOrPlaceOrLocality", ""),
                    master.get("stateOrTerritory", ""),
                    master.get("postCode", ""),
                    master.get("status", ""),
                    master.get("customerClassificationCode", ""),
                    combined_address,
                    lnsp_party,  # Network (LNSP)
                    mc_party,  # MC in RP role
                    mdp_party,  # MDP
                    mpb_party,  # MPB
                    avg_daily_load,  # LoadPerDay
                    network_tariff,
                    master.get("mlfValue", ""),
                    master.get("distributionLossFactorCode", ""),
                    master.get("dlfValue", ""),
                    meter_type,  # MeterType
                    master.get("nmiClassificationCode", ""),
                    next_read,  # NSR - NextScheduledReadDate
                    master.get("parentEmbeddedNetworkIdentifier", ""),
                    master.get("childEmbeddedNetworkIdentifier", ""),
                ]

                output_lines.append(output_row)
                filled_fields = sum(1 for x in output_row[1:] if x)
                print(f"  ✓ Success - Got {filled_fields} out of 22 fields")
            else:
                print(f"  ✗ API Error: Status {response.status_code}")
                output_lines.append([f"{Input} this {type} has no data"] + [""] * 22)

        except Exception as e:
            print(f"  ✗ Error: {e}")
            output_lines.append([f"{Input} this {type} has no data"] + [""] * 22)

# Write output CSV
try:
    with open(OUT_FILE_NAME, "w") as f_write:
        writer = csv.writer(f_write)
        writer.writerow(csv_header)

        for row in output_lines:
            writer.writerow(row)

    print(f"\n✓ Completed! Output written to {OUT_FILE_NAME}")
    print(f"  Total rows processed: {len(output_lines)}")

except PermissionError:
    print(f"\n✗ ERROR: Cannot write to {OUT_FILE_NAME}")
    print("  The file is open in another program. Please close it and try again.")