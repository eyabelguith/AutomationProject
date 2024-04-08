import pandas as pd
import numpy as np
import os
#csv_file_path = "/host_data/KPI Analysis Result_Query_Result_20240325102721558(KPI Analysis Result).csv"
###// kont njib bih data elli 3andi specifiquement

def extract_sector(cell_name):
    parts = cell_name.split('_')
    sector_prefix = '_'.join(parts[:-3])  # all parts except last 3
    sector_suffix = 'sect' + parts[-1][-1]  # last digit of last part
    return sector_prefix + '_' + sector_suffix

def extract_bande(cell_name):
    parts = cell_name.split('_')
    if len(parts) > 1:  # Ensure there are parts to split
        last_part = parts[-1]  # Get the last part
        if last_part[0] == 'f':
            return '1800'
        elif last_part[0] == 'h':
            return 'L1800'
        elif last_part[0] == 'j':
            return 'L1800²'
        elif last_part[0] == 'l':
            return 'L2100'
    return None
def mean_without_null(series):
    numeric_values = pd.to_numeric(series, errors='coerce')
    numeric_values = numeric_values.dropna()  # Drop NaN values
    return numeric_values.mean()

def main():
############
    # partie elli tcherchi 3la dataset elli tebda b KPI
    directory = "/host_data"
    files = os.listdir(directory)
    kpi_files = [f for f in files if f.startswith("KPI")]
    if not kpi_files:
        print("No KPI files found in the directory")
        return
    csv_file_path = os.path.join(directory, kpi_files[0])
##############
    df1 = pd.read_csv(csv_file_path, encoding='latin1', skiprows=6)
    df1 = df1.drop(df1.index[-1])
    
    

    nil_count = df1.apply(lambda x: x.isin(['NIL', 'nil', 'NUL', 'nul'])).sum().sum()
    print("Number of replaced values:", nil_count)

    df1 = df1.applymap(lambda x: pd.NA if str(x) in ['NIL', 'nil', 'NUL', 'nul'] else x)

    df2 = df1.copy()
    numeric_cols = df2.select_dtypes(include=['int', 'float']).columns
    df2[numeric_cols] = df2[numeric_cols].apply(pd.to_numeric)
    df2.info()

    valid_num = ['-114', '-113', '-115', '-116', '-111', '-118', '-110', '-107', '-117', '-101',
                 '-108', '-97', '-105', '-109', '-103', '-112', '-106', '-100', '-119', '-84',
                 '-120', '-99', '-121', '-104', '-98', '-102', '-125', '-95', '-122', '-90', '-96',
                 '-94', '-129', '-123', '-87']

    df2['FT_UL.Interference'] = df2['FT_UL.Interference'].apply(lambda x: np.nan if pd.isna(x) or x == '<NA>' else x)
    df2['FT_UL.Interference'] = pd.to_numeric(df2['FT_UL.Interference'], errors='coerce')
    print(df2['FT_UL.Interference'].unique())
    print(df2['FT_UL.Interference'].dtype)

    FDD_df = df2[df2["Cell FDD TDD Indication"] == "CELL_FDD"]
    FDD_df = FDD_df[FDD_df["FT_4G/LTE DL TRAFFIC VOLUME (GBYTES)"] != 0]

    FDD_df['Sector'] = FDD_df['Cell Name'].apply(extract_sector)
    FDD_df['Bande'] = FDD_df['Cell Name'].apply(extract_bande)

    # Assuming mean_values is calculated in this script
    float_columns = FDD_df.select_dtypes(include=['float']).columns
    mean_values = FDD_df.groupby('Cell Name')[float_columns].agg(mean_without_null)
    mean_values.reset_index(inplace=True)

    # Merge with mean_values
    FDD_df = pd.merge(FDD_df, mean_values, on='Cell Name', suffixes=('', '_mean'))
    # Display all columns
    pd.set_option('display.max_columns', None)

    # Calculate coefficient of variation for throughput
    coefficient_variation = (FDD_df['FT_AVE 4G/LTE DL USER THRPUT without Last TTI(ALL) (KBPS)(kbit/s)'].std() / 
                             FDD_df['FT_AVE 4G/LTE DL USER THRPUT without Last TTI(ALL) (KBPS)(kbit/s)'].mean()) * 100
    FDD_df['Coef Var thruput'] = coefficient_variation
    print("Data manipulation completed successfully.")
    
if __name__ == "__main__":
    main()
