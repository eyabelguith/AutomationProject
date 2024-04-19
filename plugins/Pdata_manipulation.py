import re
import pandas as pd
import os 
import glob
def main():
    site_names = []
    local_cell_ids = []
    reference_signal_powers = []
    sectors = []
    bands = []

    inside_section = False
    current_block = []

    # Define the regular expression pattern to mask the first set of numbers before the white space
    mask_pattern = r'^\d+\s+'
    # Define the regular expression pattern to capture the first set of numbers after masking
    number_pattern = r'^(\d+)\s+'



    file_path = next(glob.iglob('POWER_DATA.txt', recursive=False), None)

    if file_path is None:
        print("No TXT file found in the directory")
        return

    # Read file contents
    with open(file_path, 'r') as file:
        Pfile_contents = file.readlines()


    for line in Pfile_contents:
        if line.startswith("==========Succeeded MML Command=========="):
            # Set the flag to indicate we're inside the desired section
            inside_section = True
        elif inside_section:
            if line.startswith("MML Command"):
                # If current_block is not empty, extract site name from it
                if current_block:
                    site_name = None
                    local_cell_ids_block = []
                    reference_signal_powers_block = []
                    for block_line in current_block:
                        if "NE :" in block_line:
                            site_name = block_line.split("NE :")[1].strip()
                        else:
                            # Mask the first set of numbers before the white space
                            masked_line = re.sub(mask_pattern, '', block_line)
                            # Extract the first set of numbers after masking
                            match = re.search(number_pattern, masked_line)
                            if match:
                                local_cell_id = int(block_line.split()[0])
                                local_cell_ids_block.append(local_cell_id)
                                reference_signal_powers_block.append(int(match.group(1)))
                                # Determine sector based on local cell ID
                                if local_cell_id in [1, 4, 7, 104]:
                                    sector = "sect1"
                                elif local_cell_id in [2, 5, 8, 105]:
                                    sector = "sect2"
                                elif local_cell_id in [3, 6, 9, 106]:
                                    sector = "sect3"
                                elif local_cell_id in [11, 14, 17, 114]:
                                    sector = "sect4"
                                else:
                                    sector = None
                                # Determine band based on local cell ID
                                if local_cell_id in [1, 2, 3, 11]:
                                    band = "L800"
                                elif local_cell_id in [4, 5, 6, 14]:
                                    band = "L1800"
                                elif local_cell_id in [7, 8, 9, 17]:
                                    band = "L2100"
                                elif local_cell_id in [104, 105, 106, 114]:
                                    band = "L1800²"
                                else:
                                    band = None
                                # Concatenate the first two parts of the site name with the sector
                                if site_name:
                                    site_parts = site_name.split("_")
                                    site_prefix = "_".join(site_parts[:2])
                                    sectors.append(site_prefix + "_" + sector if sector else None)
                                    bands.append(band)
                    # Extend the lists with the values from the current block
                    if site_name:
                        site_names.extend([site_name] * max(len(local_cell_ids_block), len(reference_signal_powers_block)))
                        local_cell_ids.extend(local_cell_ids_block)
                        reference_signal_powers.extend(reference_signal_powers_block)
                # Clear current block for the new one
                current_block = []
            # Add line to the current block
            current_block.append(line)

    # Add the last block if any
    if current_block:
        site_name = None
        local_cell_ids_block = []
        reference_signal_powers_block = []
        for block_line in current_block:
            if "NE :" in block_line:
                site_name = block_line.split("NE :")[1].strip()
            else:
                # Mask the first set of numbers before the white space
                masked_line = re.sub(mask_pattern, '', block_line)
                # Extract the first set of numbers after masking
                match = re.search(number_pattern, masked_line)
                if match:
                    local_cell_id = int(block_line.split()[0])
                    local_cell_ids_block.append(local_cell_id)
                    reference_signal_powers_block.append(int(match.group(1)))
                    # Determine sector based on local cell ID
                    if local_cell_id in [1, 4, 7, 104]:
                        sector = "sect1"
                    elif local_cell_id in [2, 5, 8, 105]:
                        sector = "sect2"
                    elif local_cell_id in [3, 6, 9, 106]:
                        sector = "sect3"
                    elif local_cell_id in [11, 14, 17, 114]:
                                            sector = "sect4"
                    else:
                        sector = None
                    # Determine band based on local cell ID
                    if local_cell_id in [1, 2, 3, 11]:
                        band = "L800"
                    elif local_cell_id in [4, 5, 6, 14]:
                        band = "L1800"
                    elif local_cell_id in [7, 8, 9, 17]:
                        band = "L2100"
                    elif local_cell_id in [104, 105, 106, 114]:
                        band = "L1800²"
                    else:
                        band = None
                    # Concatenate the first two parts of the site name with the sector
                    if site_name:
                        site_parts = site_name.split("_")
                        site_prefix = "_".join(site_parts[:2])
                        sectors.append(site_prefix + "_" + sector if sector else None)
                        bands.append(band)
        # Extend the lists with the values from the current block
        if site_name:
            site_names.extend([site_name] * max(len(local_cell_ids_block), len(reference_signal_powers_block)))
            local_cell_ids.extend(local_cell_ids_block)
            reference_signal_powers.extend(reference_signal_powers_block)

    # Create a DataFrame from the lists
    dfPOWER15 = pd.DataFrame({'Site Name': site_names, 'Local cell ID': local_cell_ids, 'Reference signal power': reference_signal_powers, 'Sector': sectors, 'Band': bands})

    # Save the DataFrame as CSV
    dfPOWER15.to_csv('Power_output.csv', index=False)

    print("Power data manipulation done successfully ^^ ")

if __name__ == "__main__":
    main()

