import logging
import os
import re
import time
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="output_distance_calculator.log",
)

# Configuration constants
# Column names in input file - modify these to match your input file structure
COL_COMPANY = "Firma1"  # Company name column
COL_STREET = "Strasse"  # Street address column
COL_POSTAL = "PLZ"  # Postal code column
COL_CITY = "Ort"  # City column
COL_URL = "URL"  # URL column (if available)

# Define which columns to include in the output file (always includes distance_km, latitude, longitude)
OUTPUT_COLUMNS = [
    COL_COMPANY,
    COL_URL,
]  # Configurable - add or remove columns as needed


def clean_address(
    street: Optional[str], plz: Optional[Union[str, int]], city: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Clean address components to improve geocoding accuracy.
    - Removes "OT" district information from city names
    - Strips parenthetical information from city names
    - Removes building/unit/floor information like "Gebäude" or "Haus"
    - Simplifies street number ranges to use only the first number
    - Validates postal codes and flags invalid ones
    - Handles "Ecke" (corner) notation appropriately
    - Safely handles None or NaN values

    Args:
        street: Street address including number
        plz: Postal code (can be string or int)
        city: City name

    Returns:
        tuple: (cleaned_street, cleaned_plz, cleaned_city)
    """
    # Initial safety checks - handle None or NaN values
    if pd.isna(street):
        cleaned_street = None
    else:
        cleaned_street = str(street)

    if pd.isna(plz):
        cleaned_plz = None
    else:
        cleaned_plz = str(plz)

    if pd.isna(city):
        cleaned_city = None
    else:
        cleaned_city = str(city)

    # 1. Clean city name - remove "OT" and text after it
    if cleaned_city and " OT " in cleaned_city:
        cleaned_city = cleaned_city.split(" OT ")[0].strip()

    # 2. Remove parenthetical information from city
    if cleaned_city:
        cleaned_city = re.sub(r"\s+\([^)]*\)", "", cleaned_city)

    # 3. Clean building/unit/floor information from street
    if cleaned_street:
        building_patterns = [
            r"Gebäude\s+\w+",
            r"Haus\s+\w+",
            r"Bau\s+\w+",
            r"Building\s+Nr\.\s*\w+",
            r"TE\d+",
            r"Geb\.\s*\w+",
        ]
        for pattern in building_patterns:
            cleaned_street = re.sub(pattern, "", cleaned_street)

    # 4. Handle street number ranges/multiple numbers - take only the first number
    if cleaned_street:
        # Find the street number pattern and simplify it
        street_number_match = re.search(
            r"(\d+)(?:[a-z])?(?:-\d+[a-z]?)?(?:,\s*\d+[a-z]?)*\s*$", cleaned_street
        )
        if street_number_match:
            base_number = street_number_match.group(1)
            # Replace the complex number with just the first number
            cleaned_street = re.sub(
                r"(\d+)(?:[a-z])?(?:-\d+[a-z]?)?(?:,\s*\d+[a-z]?)*\s*$",
                base_number,
                cleaned_street,
            )

    # 5. Check PLZ validity
    if cleaned_plz:
        # Make sure it's a string and remove any spaces
        cleaned_plz = str(cleaned_plz).strip()

        # Check if it contains only digits and is 5 characters long
        if len(cleaned_plz) != 5 or not cleaned_plz.isdigit():
            logging.warning(
                f"Invalid postal code: {cleaned_plz}, will be omitted from geocoding query"
            )
            cleaned_plz = None

    # 6. Handle "Ecke" notation
    if cleaned_street and " Ecke " in cleaned_street:
        cleaned_street = cleaned_street.split(" Ecke ")[0].strip()

    # Final cleanup - remove double spaces
    if cleaned_street:
        cleaned_street = re.sub(r"\s+", " ", cleaned_street).strip()
    if cleaned_city:
        cleaned_city = re.sub(r"\s+", " ", cleaned_city).strip()

    return cleaned_street, cleaned_plz, cleaned_city


def geocode_address(
    address: str,
    geolocator: Nominatim,
    retries: int = 3,
    fallback_to_city: bool = True,
    parsed_plz: Optional[str] = None,
    parsed_city: Optional[str] = None,
) -> Optional[Tuple[float, float]]:
    """
    Geocode an address with retry logic and rate limiting.
    If initial geocoding fails and fallback_to_city is True, will try to geocode just the PLZ and city.

    Args:
        address: The full address to geocode
        geolocator: The geocoder instance to use
        retries: Number of retries for each geocoding attempt
        fallback_to_city: Whether to try PLZ+city fallback if full address fails
        parsed_plz: The postal code (if already extracted)
        parsed_city: The city name (if already extracted)

    Returns:
        Optional tuple of (latitude, longitude)
    """
    for attempt in range(retries):
        try:
            # Rate limiting - delay for 1 second
            time.sleep(1)
            location = geolocator.geocode(address, exactly_one=True)
            if location:
                return (location.latitude, location.longitude)  # type: ignore
            elif attempt == retries - 1:
                logging.warning(
                    f"Could not geocode address after {retries} attempts: {address}"
                )

                # Try fallback to just PLZ and city if specified
                if fallback_to_city and parsed_plz and parsed_city:
                    logging.info(
                        f"Trying fallback geocoding with just PLZ and city for: {address}"
                    )
                    fallback_address = f"{parsed_plz} {parsed_city}"

                    # Retry with just PLZ and city
                    try:
                        time.sleep(1)  # Rate limiting
                        fallback_location = geolocator.geocode(
                            fallback_address, exactly_one=True
                        )
                        if fallback_location:
                            logging.info(
                                f"✓ Fallback geocoding successful for: {address}"
                            )
                            logging.info(
                                f"  Using approximate coordinates from: {fallback_address}"
                            )
                            return (
                                fallback_location.latitude,  # type: ignore
                                fallback_location.longitude,  # type: ignore
                            )
                    except Exception:
                        logging.error(f"Fallback geocoding also failed for: {address}")

                return None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logging.warning(
                f"Geocoding error for {address}: {str(e)}. Attempt {attempt + 1}/{retries}"
            )
            if attempt == retries - 1:
                logging.error(f"Failed to geocode after {retries} attempts: {address}")

                # Try fallback to just PLZ and city if specified
                if fallback_to_city and parsed_plz and parsed_city:
                    logging.info(
                        f"Trying fallback geocoding with just PLZ and city for: {address}"
                    )
                    fallback_address = f"{parsed_plz} {parsed_city}"

                    # Retry with just PLZ and city
                    try:
                        time.sleep(1)  # Rate limiting
                        fallback_location = geolocator.geocode(fallback_address)
                        if fallback_location:
                            logging.info(
                                f"✓ Fallback geocoding successful for: {address}"
                            )
                            logging.info(
                                f"  Using approximate coordinates from: {fallback_address}"
                            )
                            return (
                                fallback_location.latitude,  # type: ignore
                                fallback_location.longitude,  # type: ignore
                            )
                    except Exception:
                        logging.error(f"Fallback geocoding also failed for: {address}")

                return None
            # Exponential backoff
            time.sleep(2**attempt)
    return None


def calculate_distances(
    input_file: str,
    reference_address: str,
    output_file: Optional[str] = None,
    testing: bool = False,
) -> bool:
    """
    Calculate distances between addresses in input file and reference address

    Args:
        input_file: Path to the input CSV/Excel file
        reference_address: The reference address to calculate distances from
        output_file: Path to save the output CSV file (optional)
        testing: If True, only process the first 5 addresses (default: False)

    Returns:
        bool: True if successful, False if an error occurred
    """
    logging.info(f"Starting distance calculation from {reference_address}")

    # If no output file specified, create one based on input filename
    if output_file is None:
        filename, extension = os.path.splitext(input_file)
        output_file = f"{filename}_with_distances.csv"

    # Hardcoded fallback for the common reference address to avoid API limits
    REFERENCE_ADDRESS_FALLBACK = {
        "Hollerithallee 17, 30419 Hannover": (52.413158354566825, 9.632402768164662)
    }

    # Read input file with error handling
    try:
        if input_file.endswith(".csv"):
            df = pd.read_csv(input_file)
        else:  # Assume Excel
            df = pd.read_excel(input_file)

        # Verify required columns exist
        required_columns = [COL_COMPANY, COL_STREET, COL_POSTAL, COL_CITY]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logging.error(
                f"Required columns missing from input file: {', '.join(missing_columns)}"
            )
            logging.error(f"Available columns: {', '.join(df.columns)}")
            logging.error(
                "Please check column names in configuration constants at the top of the script"
            )
            return False

    except FileNotFoundError:
        logging.error(f"Input file not found: {input_file}")
        return False
    except Exception as e:
        logging.error(f"Error reading input file {input_file}: {str(e)}")
        return False

    # Check for existing output file to identify already processed companies and failed geocoding attempts
    processed_companies: Dict[
        str, Tuple[Optional[float], Optional[float]]
    ] = {}  # Dictionary to store company name -> (lat, lon) mapping
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file, encoding="utf-8-sig")
            if (
                COL_COMPANY in existing_df.columns
                and "latitude" in existing_df.columns
                and "longitude" in existing_df.columns
            ):
                # Create a dict mapping company names to their lat/lon values (which might be NaN for failed geocoding)
                for _, row in existing_df.iterrows():
                    company = row[COL_COMPANY]
                    lat = row["latitude"]
                    lon = row["longitude"]
                    processed_companies[company] = (lat, lon)

                logging.info(
                    f"Found {len(processed_companies)} previously processed companies"
                )
        except Exception as e:
            logging.warning(f"Error reading existing output file: {str(e)}")

    # Filter dataframe to keep only new companies or those with failed geocoding (empty coordinates)
    if processed_companies:
        original_count = len(df)

        # Keep rows where either:
        # 1. Company name not in processed_companies, OR
        # 2. Company was processed but has NaN coordinates (failed geocoding)
        # This logic preserves companies that need to be retried
        df_filtered = df[
            ~df[COL_COMPANY].apply(
                lambda x: x in processed_companies
                and not (
                    pd.isna(processed_companies[x][0])
                    or pd.isna(processed_companies[x][1])
                )
            )
        ]

        df = df_filtered
        skipped_count = original_count - len(df)

        # Count companies to retry (previously failed)
        retry_count = sum(
            1
            for company in df[COL_COMPANY]
            if company in processed_companies
            and (
                pd.isna(processed_companies[company][0])
                or pd.isna(processed_companies[company][1])
            )
        )

        logging.info(
            f"Skipping {skipped_count} already processed companies with valid coordinates"
        )
        logging.info(f"Will retry {retry_count} companies with failed geocoding")

        if len(df) == 0:
            logging.info(
                "All companies have already been processed successfully. Nothing to do."
            )
            return True

    logging.info(f"Processing {len(df)} companies from {input_file}")

    # Limit to first 5 entries for testing
    if testing:
        df = df.head(5)
        logging.info("TESTING MODE: Limited to first 5 addresses")

    # Initialize geolocator with a proper User-Agent
    geolocator = Nominatim(user_agent="distance_calculator_script/1.0")

    # Geocode reference address
    logging.info(f"Geocoding reference address: {reference_address}")

    # Use hardcoded fallback coordinates if available
    if reference_address in REFERENCE_ADDRESS_FALLBACK:
        ref_coords = REFERENCE_ADDRESS_FALLBACK[reference_address]
        logging.info(f"Using fallback coordinates for reference address: {ref_coords}")
    else:
        ref_coords = geocode_address(reference_address, geolocator)

    if not ref_coords:
        logging.error(f"Could not geocode reference address: {reference_address}")
        return False

    logging.info(f"Reference coordinates: {ref_coords}")

    # Create a complete address field for each company
    df["full_address"] = df.apply(
        lambda row: f"{row[COL_STREET]}, {row[COL_POSTAL]} {row[COL_CITY]}", axis=1
    )

    # Dictionary to cache geocoded addresses
    geocode_cache: Dict[str, Tuple[float, float]] = {}
    failed_addresses: List[str] = []

    # Prepare output columns
    output_cols = [col for col in OUTPUT_COLUMNS if col in df.columns]
    output_cols.extend(["distance_km", "latitude", "longitude"])

    # Create/append mode for output file
    "a" if os.path.exists(output_file) else "w"
    (
        not os.path.exists(output_file) or os.path.getsize(output_file) == 0
    )

    # Process each company and write incrementally
    for idx, row in df.iterrows():
        company_name = row[COL_COMPANY]
        address = row["full_address"]
        logging.info(
            f"Processing company {idx + 1}/{len(df)}: {company_name}, {address}" # type: ignore
        )

        # Clean address components
        cleaned_street, cleaned_plz, cleaned_city = clean_address(
            row[COL_STREET], row[COL_POSTAL], row[COL_CITY]
        )

        # Build a clean address string, omitting invalid PLZ if needed
        if cleaned_plz:
            address = f"{cleaned_street}, {cleaned_plz} {cleaned_city}".strip()
        else:
            # Skip postal code if invalid
            address = f"{cleaned_street}, {cleaned_city}".strip()

        logging.debug(f"Original address: {row['full_address']}")
        logging.debug(f"Cleaned address: {address}")

        # Create a single-row dataframe for this company
        company_df = pd.DataFrame([row], columns=df.columns)

        # Check cache first
        if address in geocode_cache:
            coords = geocode_cache[address]
            logging.info(f"Using cached coordinates for: {address}")
        else:
            # Pass PLZ and city for potential fallback geocoding
            coords = geocode_address(
                address,
                geolocator,
                fallback_to_city=True,
                parsed_plz=cleaned_plz,
                parsed_city=cleaned_city,
            )
            if coords:
                geocode_cache[address] = coords

        if coords:
            # Calculate distance in kilometers
            lat, lon = coords
            distance_km = round(geodesic(ref_coords, coords).kilometers, 2)
            company_df["distance_km"] = distance_km
            company_df["latitude"] = lat
            company_df["longitude"] = lon
            logging.info(f"Distance: {distance_km} km, Coordinates: {lat}, {lon}")
        else:
            company_df["distance_km"] = None
            company_df["latitude"] = None
            company_df["longitude"] = None
            failed_addresses.append(address)
            logging.warning(f"Could not calculate distance for: {address}")

        # Select only the output columns
        result_df = company_df[output_cols]

        # Update existing output file if it exists, otherwise create new
        if existing_df is not None:
            # Check if current company already exists in the output file
            company_exists = company_name in existing_df[COL_COMPANY].values

            if company_exists:
                # Update the existing record instead of appending
                existing_df.loc[existing_df[COL_COMPANY] == company_name] = (
                    result_df.values[0]
                )
                logging.info(f"Updated existing record for {company_name}")

                # Write the entire updated dataframe back to file
                existing_df.to_csv(output_file, index=False, encoding="utf-8-sig")
            else:
                # Append the new record to existing dataframe and save
                existing_df = pd.concat([existing_df, result_df], ignore_index=True)
                existing_df.to_csv(output_file, index=False, encoding="utf-8-sig")
                logging.info(f"Added {company_name} to existing file")
        else:
            # First company being processed, write with header
            result_df.to_csv(
                output_file, mode="w", header=True, index=False, encoding="utf-8-sig"
            )
            logging.info(f"Created new output file with {company_name}")

    # Log failed geocoding
    if failed_addresses:
        logging.warning(f"Failed to geocode {len(failed_addresses)} addresses:")
        for addr in failed_addresses:
            logging.warning(f"  - {addr}")

    return True


def calculate_single_distance(
    address: str, reference_address: str
) -> Optional[Tuple[float, float, float]]:
    """
    Calculate the distance between a single address and the reference address

    Args:
        address: The address to check (expected format: "Street, PLZ City")
        reference_address: The reference address to calculate distance from

    Returns:
        tuple: (distance_km, lat, lon) or None if geocoding failed
    """
    logging.info(f"Processing single address: {address}")
    logging.info(f"Reference address: {reference_address}")

    # Initialize geolocator
    geolocator = Nominatim(user_agent="distance_calculator_script/1.0")

    # Geocode reference address
    ref_coords = geocode_address(reference_address, geolocator)
    if not ref_coords:
        logging.error(f"Could not geocode reference address: {reference_address}")
        return None

    # Parse the address components with more robust handling
    # Expected format: "Street, PLZ City" but handle variations
    try:
        # First try to parse as "Street, PLZ City"
        components = address.split(",", 1)
        street = components[0].strip() if len(components) > 0 else ""

        if len(components) > 1:
            # Try to find postal code (usually 5 digits)
            city_part = components[1].strip()

            # Look for a 5-digit postal code
            plz_match = re.search(r"\b(\d{5})\b", city_part)

            if plz_match:
                plz = plz_match.group(1)

                # Extract city - everything after the postal code
                city_start = plz_match.end()
                city = city_part[city_start:].strip()
            else:
                # No postal code found, treat everything after comma as city
                plz = None
                city = city_part
        else:
            # No comma in address, can't parse reliably
            logging.warning(f"Address format not recognized: {address}")
            logging.warning("Expected format: 'Street, PLZ City'")
            # Try geocoding the whole address anyway
            coords = geocode_address(address, geolocator)

            if coords:
                lat, lon = coords
                distance_km = round(geodesic(ref_coords, coords).kilometers, 2)
                logging.info(f"Distance: {distance_km} km, Coordinates: {lat}, {lon}")
                return (distance_km, lat, lon)
            else:
                return None
    except Exception as e:
        logging.error(f"Error parsing address '{address}': {str(e)}")
        # Try geocoding the whole address anyway
        coords = geocode_address(address, geolocator)

        if coords:
            lat, lon = coords
            distance_km = round(geodesic(ref_coords, coords).kilometers, 2)
            return (distance_km, lat, lon)
        else:
            return None

    # Clean the address components
    cleaned_street, cleaned_plz, cleaned_city = clean_address(street, plz, city)

    # Build clean address string
    if cleaned_plz:
        clean_address_str = f"{cleaned_street}, {cleaned_plz} {cleaned_city}".strip()
    else:
        clean_address_str = f"{cleaned_street}, {cleaned_city}".strip()

    logging.info(f"Original address: {address}")
    logging.info(f"Parsed components: Street='{street}', PLZ='{plz}', City='{city}'")
    logging.info(f"Cleaned address: {clean_address_str}")

    # Geocode the cleaned address with fallback to PLZ+city if full address fails
    coords = geocode_address(
        clean_address_str,
        geolocator,
        fallback_to_city=True,
        parsed_plz=cleaned_plz,
        parsed_city=cleaned_city,
    )

    # If first attempt fails, try the original address
    if not coords:
        logging.warning(f"Could not geocode cleaned address: {clean_address_str}")
        logging.info("Trying with original address...")
        coords = geocode_address(address, geolocator)

    if coords:
        lat, lon = coords
        distance_km = round(geodesic(ref_coords, coords).kilometers, 2)
        logging.info(f"Distance: {distance_km} km, Coordinates: {lat}, {lon}")
        return (distance_km, lat, lon)
    else:
        logging.warning(f"Could not geocode address: {address}")
        # Log addresses that completely failed geocoding to a separate file
        with open("failed_geocoding_addresses.txt", "a") as fail_file:
            fail_file.write(f"{address}\n")
        return None


if __name__ == "__main__":
    import argparse

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description="Calculate distances from addresses to a reference point."
    )
    parser.add_argument(
        "--file", "-f", type=str, help="Input Excel/CSV file with addresses"
    )
    parser.add_argument(
        "--reference",
        "-r",
        type=str,
        default="Hollerithallee 17, 30419 Hannover",
        help="Reference address to calculate distances from",
    )
    parser.add_argument(
        "--address",
        "-a",
        type=str,
        help="Single address to check (will not save to file)",
    )
    parser.add_argument("--output", "-o", type=str, help="Output file path (optional)")
    parser.add_argument(
        "--test",
        "-t",
        action="store_true",
        help="Testing mode - only process first 5 addresses",
    )

    args = parser.parse_args()

    # Set default reference address
    REFERENCE_ADDRESS = args.reference

    # Process a single address if provided
    if args.address:
        print(f"\nCalculating distance for address: {args.address}")
        print(f"Reference address: {REFERENCE_ADDRESS}\n")

        result = calculate_single_distance(args.address, REFERENCE_ADDRESS)

        if result:
            distance_km, lat, lon = result
            print("Results:")
            print(f"- Distance: {distance_km} km")
            print(f"- Coordinates: {lat}, {lon}")
        else:
            print("Could not calculate distance for the provided address.")
            print("Please check the address format and try again.")

    # Process a file of addresses
    elif args.file:
        INPUT_FILE = args.file
        OUTPUT_FILE = args.output
        TESTING_MODE = args.test

        print(f"Processing addresses from {INPUT_FILE}")
        print(f"Reference address: {REFERENCE_ADDRESS}")
        if TESTING_MODE:
            print("TESTING MODE: Only processing first 5 addresses")

        calculate_distances(
            INPUT_FILE, REFERENCE_ADDRESS, OUTPUT_FILE, testing=TESTING_MODE
        )

    # If no arguments provided, show help
    else:
        parser.print_help()
