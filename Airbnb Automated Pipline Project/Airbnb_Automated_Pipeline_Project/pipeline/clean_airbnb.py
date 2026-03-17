import os
import re
import glob
import pandas as pd
import numpy as np

# ----------------------------
# Project paths
# ----------------------------
PROJECT_DIR = "/files"
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_DIR, "data", "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ----------------------------
# Find dataset automatically (multiple formats)
# ----------------------------
def find_listings_file():
    """Search for Airbnb dataset inside the raw data folder."""

    supported_extensions = ["*.csv", "*.xlsx", "*.xls", "*.parquet", "*.json"]

    data_files = []

    for ext in supported_extensions:
        data_files.extend(glob.glob(os.path.join(RAW_DIR, ext)))

    if not data_files:
        raise FileNotFoundError("No supported data files found in data/raw folder")

    for file in data_files:
        name = os.path.basename(file).lower()

        if "listing" in name:
            return file

    # fallback → return first detected file
    return data_files[0]

# ----------------------------
# Load dataset based on file type
# ----------------------------
def load_dataset(file_path):
    """Load dataset depending on file extension."""

    extension = os.path.splitext(file_path)[1].lower()

    if extension == ".csv":
        try:
            df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="latin1", low_memory=False)

    elif extension in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)

    elif extension == ".parquet":
        df = pd.read_parquet(file_path)

    elif extension == ".json":
        df = pd.read_json(file_path)

    else:
        raise ValueError(f"Unsupported file format: {extension}")

    return df

# ----------------------------
# Basic cleaning
# ----------------------------
def basic_cleaning(df):
    """Perform initial cleaning steps."""

    # standardize column names
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
    )

    # remove duplicate rows
    before_rows = df.shape[0]
    df = df.drop_duplicates()
    # remove duplicate listing_id if available
    if "listing_id" in df.columns:
        before_listing = df.shape[0]
        df = df.drop_duplicates(subset="listing_id")
        after_listing = df.shape[0]
    else:
        before_listing = df.shape[0]
        after_listing = df.shape[0]

    after_rows = df.shape[0]

    print("\nBasic Cleaning Summary:")
    print(f"Rows before removing duplicates: {before_rows}")
    print(f"Rows after removing duplicates:  {after_rows}")
    print(f"Duplicate rows removed:          {before_rows - after_rows}")
    print(f"Duplicate listing_id removed:  {before_listing - after_listing}")


    return df

# ----------------------------
# Column type cleaning
# ----------------------------
def clean_column_types(df):
    """Clean and convert important columns."""
        
    # datetime conversion
    if "host_since" in df.columns:
        df["host_since"] = pd.to_datetime(df["host_since"], errors="coerce")
    # price column
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # percentage columns
    percent_cols = ["host_response_rate", "host_acceptance_rate"]

    for col in percent_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.strip()
                .replace(["nan", "None", ""], np.nan)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # boolean columns from notebook
    bool_cols = [
        "host_is_superhost",
        "host_has_profile_pic",
        "host_identity_verified",
        "instant_bookable"
    ]

    for col in bool_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .replace({"t": True, "f": False, "True": True, "False": False})
                .astype("boolean")
            )

    # numeric columns that should be numeric
    numeric_cols = [
        "bedrooms",
        "accommodates",
        "minimum_nights",
        "maximum_nights",
        "host_total_listings_count",
        "review_scores_rating",
        "review_scores_accuracy",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
        "review_scores_value"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("\nColumn type cleaning completed.")
    return df

# ----------------------------
# Missing value handling
# ----------------------------
def handle_missing_values(df):
    """Handle missing values for important columns."""
    df = df.replace(["None", "nan", "NaN", ""], np.nan)
    # bedrooms
    if "bedrooms" in df.columns:
        df["bedrooms"] = df["bedrooms"].fillna(df["bedrooms"].median())

    # other important numeric cols
    for col in ["accommodates", "minimum_nights", "maximum_nights"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # host response columns -> median, not 0
    for col in ["host_response_rate", "host_acceptance_rate"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # review scores
    review_cols = [
        "review_scores_rating",
        "review_scores_accuracy",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
        "review_scores_value"
    ]

    for col in review_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # categorical / text rules from notebook
    if "host_response_time" in df.columns:
        df["host_response_time"] = df["host_response_time"].fillna("no response")

    if "host_location" in df.columns:
        df["host_location"] = df["host_location"].fillna("Unknown")

    # host total listings count
    if "host_total_listings_count" in df.columns:
        df["host_total_listings_count"] = df["host_total_listings_count"].fillna(
            df["host_total_listings_count"].median()
        )
        
    # fill missing booleans with False
    bool_cols = [
        "host_is_superhost",
        "host_has_profile_pic",
        "host_identity_verified",
        "instant_bookable"
    ]

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].fillna(False)

    print("\nMissing value handling completed.")

    return df

# ----------------------------
# Outlier handling
# ----------------------------
def handle_outliers(df):
    """Apply outlier rules from notebook / EDA."""

    # price outlier removal using IQR
    if "price" in df.columns:
        before_rows = df.shape[0]

        # remove invalid prices first
        df = df[df["price"].notna()]
        df = df[df["price"] > 0]

        q1 = df["price"].quantile(0.25)
        q3 = df["price"].quantile(0.75)
        iqr = q3 - q1
        upper_bound = q3 + 1.5 * iqr

        df = df[df["price"] <= upper_bound]

        after_rows = df.shape[0]

        print("\nPrice Outlier Handling:")
        print(f"IQR upper bound used: {upper_bound:.2f}")
        print(f"Rows before price filter: {before_rows}")
        print(f"Rows after price filter:  {after_rows}")
        print(f"Rows removed:             {before_rows - after_rows}")

    return df

# ----------------------------
# Feature Engineering
# ----------------------------
def feature_engineering(df):

    print("\nStarting feature engineering...")

    # ----------------------------
    # Price per guest
    # ----------------------------
    if {"price", "accommodates"}.issubset(df.columns):
        df["price_per_guest"] = df["price"] / df["accommodates"]

    # ----------------------------
    # Price per bedroom
    # ----------------------------
    if {"price", "bedrooms"}.issubset(df.columns):
        df["price_per_bedroom"] = df["price"] / df["bedrooms"].replace(0, np.nan)

    # ----------------------------
    # Host experience (years)
    # ----------------------------
    if "host_since" in df.columns:
        today = pd.Timestamp.today()
        df["host_experience_years"] = (
            (today - df["host_since"]).dt.days / 365
        ).round(1)

    # ----------------------------
    # Host type classification
    # ----------------------------
    if "host_total_listings_count" in df.columns:

        df["host_type"] = np.where(
            df["host_total_listings_count"] > 3,
            "professional",
            "individual"
        )

    # ----------------------------
    # Luxury listing flag
    # ----------------------------
    if "price" in df.columns:

        luxury_threshold = df["price"].quantile(0.75)

        df["luxury_flag"] = np.where(
            df["price"] > luxury_threshold,
            "Luxury",
            "Standard"
        )
    # ----------------------------
    # Overall review score
    # ----------------------------
    review_cols = [
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value"
    ]

    existing_cols = [col for col in review_cols if col in df.columns]

    if existing_cols:
        df["overall_avg_review_score"] = df[existing_cols].mean(axis=1)

    # ----------------------------
    # Long stay flag
    # ----------------------------
    if "minimum_nights" in df.columns:
        df["long_stay_flag"] = np.where(
            df["minimum_nights"] >= 7,
            "Long stay",
            "Short stay"
        )

    print("Feature engineering completed.")

    return df

# ----------------------------
# Data validation check
# ----------------------------
def data_validation(df):
    if "maximum_nights" in df.columns:
        invalid_max_nights = (df["maximum_nights"] <= 0).sum()
        very_high_max_nights = (df["maximum_nights"] > 3650).sum()

        print("\nmaximum_nights validation:")
        print(f"Values <= 0: {invalid_max_nights}")
        print(f"Very high values (>3650): {very_high_max_nights}")
    return df

# ----------------------------
# Quality check report
# ----------------------------
def quality_check(df):
    """Print simple post-cleaning checks."""
    print("\nPost-cleaning quality check:")
    print(f"Shape: {df.shape}")

    missing_summary = df.isnull().sum()
    missing_summary = missing_summary[missing_summary > 0].sort_values(ascending=False)

    if missing_summary.empty:
        print("No missing values left in cleaned important columns.")
    else:
        print("\nRemaining missing values:")
        print(missing_summary.head(20))

    if "price" in df.columns:
        print("\nPrice summary:")
        print(df["price"].describe())

    return df

# ----------------------------
# Dataset metadata summary
# ----------------------------
def dataset_metadata(df):

    print("\nDataset Metadata Summary")
    print("-" * 40)

    print(f"Total rows: {df.shape[0]}")
    print(f"Total columns: {df.shape[1]}")

    print("\nColumn Data Types:")
    print(df.dtypes.value_counts())

    print("\nMemory Usage:")
    print(f"{df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

    print("\nFeature Columns Added:")
    engineered_features = [
        "price_per_guest",
        "price_per_bedroom",
        "host_experience_years",
        "host_type",
        "luxury_flag",
        "overall_avg_review_score",
        "long_stay_flag"
    ]

    existing_features = [col for col in engineered_features if col in df.columns]

    for feature in existing_features:
        print(f"- {feature}")

    print("\nDataset metadata generated successfully.\n")

    return df

# ----------------------------
# Save processed dataset
# ----------------------------
def save_processed_data(df):
    """Save cleaned dataset."""

    output_path = os.path.join(PROCESSED_DIR, "clean_listings.parquet")
    csv_path = os.path.join(PROCESSED_DIR, "clean_piplistings.csv")

    df.to_parquet(output_path, index=False)
    df.to_csv(csv_path, index=False)

    print(f"\nProcessed dataset saved to:")
    print(output_path)
    print(csv_path)
# ----------------------------
# Main execution
# ----------------------------
def main():
    input_file = find_listings_file()
    print(f"Dataset found: {input_file}")

    df = load_dataset(input_file)
    print(f"Dataset loaded successfully.")
    print(f"Shape of dataset: {df.shape}")

    print("\nColumns in dataset:")
    print(df.columns.tolist())

    # check duplicate columns
    print("\nDuplicate columns:")
    print(df.columns[df.columns.duplicated()])

    df = basic_cleaning(df)
    print(f"\nShape after cleaning: {df.shape}")
    df = clean_column_types(df)
    df = handle_missing_values(df)
    df = handle_outliers(df)
    df = feature_engineering(df)
    df = data_validation(df)
    df = quality_check(df)
    df = dataset_metadata(df)
    save_processed_data(df)

if __name__ == "__main__":
    main()

