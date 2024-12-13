import pandas as pd

# File paths
input_file = "/Users/matthewmiller/Desktop/BasicCompanyDataAsOneFile-2024-12-01.csv"
output_file = "nw4_companies_filtered.csv"

# Define target address columns
address_columns = [
    "RegAddress.AddressLine1",
    "RegAddress.AddressLine2",
    "RegAddress.PostTown",
    "RegAddress.PostCode"
]

# Function to filter companies based on NW4 in address columns
def filter_nw4_companies(input_file, output_file):
    chunk_size = 10000  # Adjust for performance
    filtered_data = []

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Normalize column names by stripping whitespace
        chunk.columns = chunk.columns.str.strip()

        # Debugging: Print available columns to verify correctness
        print("Columns available in chunk:", chunk.columns.tolist())

        # Check if target columns exist
        available_columns = [col for col in address_columns if col in chunk.columns]

        if not available_columns:
            print("No address columns found in this chunk.")
            continue

        # Filter rows containing "NW4" in any of the available address columns
        filtered_chunk = chunk[chunk[available_columns].apply(
            lambda row: row.str.contains("NW4", na=False, case=False)
        ).any(axis=1)]

        filtered_data.append(filtered_chunk)

    # Combine all filtered chunks and save
    if filtered_data:
        filtered_df = pd.concat(filtered_data, ignore_index=True)
        filtered_df.to_csv(output_file, index=False)
        print(f"Filtered data saved to {output_file}")
    else:
        print("No matching data found.")

# Run the function
filter_nw4_companies(input_file, output_file)