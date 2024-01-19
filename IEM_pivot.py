# Adapted from code created by ChatGPT

import pandas as pd

# Read csv file
file_path = 'IEM_market_data.csv'
df = pd.read_csv(file_path)

# Function to categorize Page_Title
def categorize_market_id(Page_Title):
    if 'HFM' in Page_Title:
        return 'Hurricane'
    elif any(keyword in Page_Title for keyword in ['FLU', 'mrsa', 'FVE_', 'AFPolicy', 'AFHumNum', 'AFHumLocAFAnimal', 'AFAnimal', 'AFHumLoc']):
        return 'Health'
    elif any(keyword in Page_Title for keyword in ['WTA', 'VS', 'Congress', 'CONGRESS', 'HOUSE', 'SENATE', 'House', 'Senate', 'RECALL', 'CONV', 'conv', 'MX_', 'FR22', 'QUEB22', 'NL', 'NYC', 'NYSENATE', 'Hessen', 'IACaucus', 'Saxony', 'REFORM00', 'TAIWAN', 'CDU', 'Conv', 'Cong', 'EU99', 'EPparties99', 'FVparti', 'FVdate', 'FVprime', 'Appointments']):
        return 'Election'
    elif any(keyword in Page_Title for keyword in ['GOOGLE', 'MSFT', 'FedPolicy', 'Comp', 'IntInd', 'Index', 'UN_INT', 'GInflation', 'GRSpread']):
        return 'Economic'    
    elif 'Movie' in Page_Title:
        return 'Movie'
    else:
        return 'Other'  # Default category

# Apply the categorization function to the 'Page_Title' column
df['Category'] = df['Page_Title'].apply(categorize_market_id)

# Convert 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Calculate the number of days between the oldest and newest date for each 'Page_Title'
days_range = df.groupby('Page_Title')['Date'].agg(lambda x: (x.max() - x.min()).days).reset_index()
days_range.rename(columns={'Date': 'Days'}, inplace=True)

# Clean and convert 'Units' column to float
df['Units'] = df['Units'].str.replace(',', '').astype(float)

# Convert 'Dollar Volume' column to float
df['Dollar Volume'] = df['Dollar Volume'].str.replace(',', '').astype(float)

# Pivot Table based on 'Page_Title'
pivot = df.pivot_table(
    index=['Page_Title', 'Market_ID', 'Category'],
    values=['Contract', 'Units', 'Dollar Volume'],
    aggfunc={
        'Contract': pd.Series.nunique,
        'Units': 'sum',
        'Dollar Volume': 'sum'
    }
)

pivot.reset_index(inplace=True)

pivot = pivot.merge(days_range, on='Page_Title')

pivot.rename(columns={'Contract': 'Unique Contracts'}, inplace=True)

# Save the pivot table to a new CSV file
output_file_path = 'IEM_pivot_markets.csv'
pivot.to_csv(output_file_path, index=False)

print(pivot)

# Create a second pivot table from the 'pivot' DataFrame
second_pivot = pivot.pivot_table(
    index=['Category'],
    values=['Page_Title', 'Units', 'Dollar Volume'],
    aggfunc={
        'Page_Title': pd.Series.nunique,
        'Units': 'sum',
        'Dollar Volume': 'sum'
    },
    margins=True,  # Add totals row
    margins_name='Total'  # Name for the totals row
)

second_pivot.rename(columns={'Page_Title': 'Markets'}, inplace=True)

# Save the second pivot table to a new CSV file
output_file_path_second_pivot = 'IEM_pivot_categories.csv'
second_pivot.to_csv(output_file_path_second_pivot)

print(second_pivot)