import re
import pandas as pd
import os 
import glob
#file_path = "/host_data/TILT_DATA.txt"

def main():
    file_path = next(glob.iglob('*.txt', recursive=False), None)

    if file_path is None:
        print("No TXT file found in the directory")
        return

    # Read file contents
    with open(file_path, 'r') as file:
        file_contents = file.readlines()
    #print("File Contents:")
    #for line in file_contents:
        #print(line.strip())
 
 
##########didnt work
 #   with open('TILT_DATA.txt', 'r') as file:
  #      file_contents = file.readlines()
        
    # Initial lists
    site_names_list = []
    device_numbers_list = []
    device_names_list = []
    actual_tilts_list = []
    inside_section = False
    current_block = []
    
    # li device name
    device_name_pattern = r'(\S+?_sect\d+)'

   
   
    for line in file_contents:
        if line.startswith("==========Succeeded MML Command=========="):
            # flag ywalli tru ki nod5lou fi section elli 7achetna bech ne5dmou 3leha
            inside_section = True
        elif inside_section:
            if line.startswith("MML Command"):
                # process the previous block ken current_block mech fera4
                if current_block:
                    site_name = None
                    device_numbers = []
                    device_names = []
                    actual_tilts = []
                    for block_line in current_block:
                        if "NE :" in block_line:
                            site_name = block_line.split("NE :")[1].strip()
                        elif block_line.startswith("Device No."):
                            # Skip header
                            continue
                        else:
                            # Extract device numbers using regular expression
                            matches = re.findall(r'^(\d+)', block_line)
                            if matches:
                                device_numbers.extend([int(match) for match in matches])
                                # Extract device names using regular expression
                                device_name_match = re.search(device_name_pattern, block_line)
                                if device_name_match:
                                    device_names.append(device_name_match.group())
                                # Extract actual tilts
                                tilt_match = re.search(r'AVAILABLE\s+(\d+|NULL)', block_line)
                                if tilt_match:
                                    tilt_value = tilt_match.group(1)
                                    if tilt_value.isdigit():
                                        actual_tilts.append(int(tilt_value))
                                    else:
                                        actual_tilts.append(tilt_value)

                    # Fill in missing values with 'NULL' bech ken lists mech 9ad9ad , mandhaya3ch el other values
                    max_length = max(len(device_numbers), len(device_names), len(actual_tilts))
                    device_numbers.extend([None] * (max_length - len(device_numbers)))
                    device_names.extend([None] * (max_length - len(device_names)))
                    actual_tilts.extend([None] * (max_length - len(actual_tilts)))

                    # Append to lists
                    site_names_list.extend([site_name] * max_length)
                    device_numbers_list.extend(device_numbers)
                    device_names_list.extend(device_names)
                    actual_tilts_list.extend(actual_tilts)

                # Clear current block for the new 
                current_block = []
            # Add line to the current block
            current_block.append(line)

    # Create a DataFrame from the lists
    dfTILT7 = pd.DataFrame({'Site Name': site_names_list, 'Device No': device_numbers_list, 
                            'Device Name': device_names_list, 'Actual Tilt': actual_tilts_list})

    # Replace NULL values with -1
    dfTILT7['Actual Tilt'] = dfTILT7['Actual Tilt'].replace("NULL", -1)

    # Sector feature combinaison bin site and device name
    dfTILT7['Sector'] = dfTILT7['Site Name'].str.split('_').str[:2].str.join('_') + '_' + dfTILT7['Device Name'].str.split('_').str[-1]

    # function for mapping device names to bands
    def map_band(device_name):
        if device_name is None:
            return None
        first_part = device_name.split('_')[0][1:]  # Extracting the first part and ignoring the first letter (B)
        if '21&18' in first_part:
            return ['L2100', 'L1800']
        elif '21' in first_part:
            return 'L2100'
        elif '18' in first_part:
            return 'L1800'
        elif '09&08' in first_part or '08' in first_part:
            return 'L800'
        else:
            return None  # Handle other cases 

    # Apply mapping function to create Band
    dfTILT7['Band'] = dfTILT7['Device Name'].apply(map_band)

    # Save df
    dfTILT7.to_csv('Tilt_output.csv', index=False)

    print("Tilt data manipulation completed successfully ^^ ")

if __name__ == "__main__":
    main()
