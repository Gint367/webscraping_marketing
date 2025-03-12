from bs4 import BeautifulSoup
import json
import os

def extract_company_and_hall_from_all(html):
    """
    Extracts the company name and hall location from all divs with IDs starting with one of 
    the prefixes: ah100, ah200, ah300, or ah400 within the provided HTML.

    Args:
        html: A string containing the HTML document.

    Returns:
        A list of tuples, where each tuple contains the hall location, company name, and description
        extracted from a single matching div. If information is not found in a particular div,
        the corresponding tuple element will be None. Returns an empty list if no matching
        divs are found.
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Try different ID prefixes until we find matching elements
    prefixes = ['ah100', 'ah200', 'ah300', 'ah400']
    matching_divs = []
    
    for prefix in prefixes:
        matching_divs = soup.find_all('div', id=lambda x: x and x.startswith(prefix))
        if matching_divs:
            print(f"Found {len(matching_divs)} elements with prefix '{prefix}'")
            break
    
    if not matching_divs:
        print("No matching elements found with any of the prefixes")
        return results

    for div in matching_divs:
        hall_location, company_name, description = extract_company_and_hall(str(div))  # Pass the div's HTML
        results.append((hall_location, company_name, description))

    return results

def extract_company_and_hall(html):
    """
    Extracts the company name, hall location, and description from a given HTML snippet
    assuming it follows the structure provided.

    Args:
        html: A string containing the HTML of a card element.

    Returns:
        A tuple containing the hall location, company name, and description.
        If any information is not found, the corresponding tuple element will be None.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Extract Hall Location
    hall_location_element = soup.find('div', class_='card__title-row__left')
    if hall_location_element:
        # Get the text from the span that contains text using lambda instead of text=True
        hall_text = hall_location_element.find('span', lambda tag: tag.name == 'span' and tag.string)
        if hall_text:
            hall_location = hall_text.text.strip()
        else:
            hall_location = None
    else:
        hall_location = None

    # Extract Company Name
    company_name_element = soup.find('div', class_='h2 card__title')
    if company_name_element:
        company_name = company_name_element.find('a', class_='link link--primary').text.strip()
    else:
        company_name = None
    
    # Extract Company Description
    description_element = soup.find('p')
    if description_element and description_element.text.strip():
        description = description_element.text.strip()
    else:
        description = None

    return hall_location, company_name, description

def save_to_json(data, output_file):
    """
    Saves the extracted data to a JSON file.
    
    Args:
        data: List of tuples containing hall location, company name, and description
        output_file: Path to the output JSON file
    """
    # Convert tuples to dictionaries for better JSON structure
    json_data = []
    for item in data:
        hall_location, company_name, description = item if len(item) == 3 else (*item, None)
        json_data.append({
            "hall_location": hall_location,
            "company_name": company_name,
            "description": description
        })
    
    # Write the data to the JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
    
    print(f"Data saved to {output_file}")

# Example Usage
if __name__ == "__main__":
    input_file = 'halle4.html'
    
    # Construct the output filename
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = f"intec_{base_name}.json"
    
    # Read the HTML content
    with open(input_file, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    # Extract the data
    results = extract_company_and_hall_from_all(html_content)
    
    # Print the results
    for result in results:
        hall_location, company_name, description = result if len(result) == 3 else (*result, None)
        print(f"Hall Location: {hall_location}")
        print(f"Company Name: {company_name}")
        print(f"Description: {description}")
        print("-" * 20)
    
    # Save the results to a JSON file
    save_to_json(results, output_file)