import os
import math
import pandas as pd
import requests
from io import StringIO
from geopy.geocoders import ArcGIS
from flask import Flask, request, render_template

app = Flask(__name__)

def download_csv(csv_url):
    """Download CSV from the given URL and return it as a DataFrame."""
    response = requests.get(csv_url)
    response.raise_for_status()
    csv_data = StringIO(response.text)
    return pd.read_csv(csv_data)

# Main work
def normalize_state(state):
    state_mapping = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY"
    }
    return state_mapping.get(state, state)

def normalize_role(advertiser_name, title):
    role_normalizations = pd.read_excel("Role Normalisations.xlsx")
    for _, row in role_normalizations.iterrows():
        advertiser = str(row["Advertiser"]).strip().lower()
        search_for = str(row["Search For"]).strip().lower()
        if_in_title = str(row["If in Title"])

        if advertiser == advertiser_name.strip().lower():
            keywords = [keyword.strip() for keyword in search_for.split(',')]
            if any(keyword in title.strip().lower() for keyword in keywords):
                return if_in_title
    return title

class JobGroupWithLocation:
    def __init__(self, role, city, state, population):
        self.city = city
        self.state = state    
        self.role = role
        self.jobcount = 1
        self.location = f"{city}, {state}"
        self.population = population

    def increment_jobcount(self):
        self.jobcount += 1

    def add_to_population(self, value):
        self.population += value

def process_csv_with_location(file_path, advertiser):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    
    job_groups = {}

    for _, row in df.iterrows():
        city = row.get("city", "").strip()
        state = normalize_state(row.get("state", "").strip())
        title = row.get("title", "").strip()
        population = pd.to_numeric(row.get("population", 0), errors="coerce") or 0

        role = normalize_role(advertiser, title)

        identifier = f"{role}_{city}_{state}"

        if identifier in job_groups:
            job_groups[identifier].increment_jobcount()
            job_groups[identifier].add_to_population(population)
        else:
            job_group = JobGroupWithLocation(role, city, state, population)
            job_groups[identifier] = job_group

    return job_groups

def convert_job_groups_to_df_with_location(job_groups):
    output_data = []
    for identifier, group in job_groups.items():
        output_data.append({
            "Role": group.role,
            "Job Count": group.jobcount,
            "Location (City, State)": group.location,
            "Population": group.population
        })
    return pd.DataFrame(output_data)

def process_csv_based_on_state(file_path, advertiser):
    df = pd.read_csv(file_path)

    job_groups = {}

    for _, row in df.iterrows():
        state = normalize_state(row["state"])
        title = normalize_role(advertiser, row["title"])
        city = row["city"]
        advertiser_name = row[advertiser] if advertiser in row else "default_advertiser"

        normalized_role = normalize_role(advertiser, title)
        group = next((g for g in job_groups.values() if g.identifier == f"{state}_{normalized_role}"), None)

        if group is None:
            group = JobGroupWithState(name=state, role=normalized_role)
            job_groups[group.identifier] = group

        group.add_to_list(city)
        group.increment_job_count()
        
    return job_groups

def convert_job_groups_to_df_with_state(job_groups):
    output_data = []
    for identifier, group in job_groups.items():
        output_data.append({
            "Role": group.role,
            "Job Count": group.job_count,
            "State": group.name,
            "City Count": group.city_count,
        })
    return pd.DataFrame(output_data)

class JobGroupWithState:
    def __init__(self, name, role):
        self.name = name 
        self.role = role  
        self.items = []  
        self.job_count = 0 
        self.city_count = 0  
        self.identifier = f"{self.name}_{self.role}"

    def add_to_list(self, item):
        if item not in self.items:
            self.items.append(item)
            self.increment_city_count()
        return False

    def increment_job_count(self):
        self.job_count += 1

    def increment_city_count(self):
        self.city_count += 1

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

def is_50_miles(city, state):
    state_short = normalize_state(state)

    folder_path = "cities"
    file_name = f"{state_short}_distances.csv"
    file_path = os.path.join(folder_path, file_name)

    if not os.path.exists(file_path):
        return None

    try:
        data = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

    if "City 2" in data.columns:
        if city in data["City 2"].values:
            row = data[data["City 2"] == city]
            return row.iloc[0]["City 1"]

    if "City 1" in data.columns:
        if city in data["City 1"].values:
            return city
        

    return None

    # If the city is not found in either column Finding the distance
    # nom = ArcGIS()
    # location1 = nom.geocode(f"{city}, {state}")

    # Lati1 = location1.latitude
    # Longi1 = location1.longitude

    # unique_cities = data["City 1"].unique()
    # closest_city = None
    # distance = float('inf')
    # itr=1
    # for unique_city in unique_cities:
    #     city_with_state = f"{unique_city}, {state_short}"

    #     location2 = nom.geocode(city_with_state)
    #     if location2:
    #         Lati2 = location2.latitude
    #         Longi2 = location2.longitude

    #         find_distance = haversine(Lati1, Longi1, Lati2, Longi2)

    #         if find_distance <= 50:
    #             print("I found the city, ", unique_city)
    #             distance = find_distance
    #             closest_city = unique_city
    #             break
    #         # elif distance!=None:
    #         #     if find_distance < distance:
    #         #         distance = find_distance
    #         #         closest_city = unique_city
    #         # print("I will search for the city, ", itr)
    #         # itr+=1

    # if distance is not None and distance <= 50:
    #     df = pd.read_csv(file_path)
    #     new_row = {'City 1': closest_city, 'City 2': city, 'Distance': distance }
    #     df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    #     df.to_csv(file_path, index=False)
    #     print("Added the city to the file")

    # return closest_city if closest_city else None

class JobGroupClustering:
    def __init__(self, city, name, role):
        self.city = city  
        self.name = name  
        self.role = role  
        self.job_count = 1  
        self.population = 0 
        self.identifier = f"{self.city}_{self.name}_{self.role}" 

    def increment_job_count(self):
        """Increment the job count by 1."""
        self.job_count += 1

    def increment_population_count(self, population):
        self.population += population

def convert_job_groups_to_df_with_clustering(job_groups):
    output_data = []
    for identifier, group in job_groups.items():
        output_data.append({
            "Role": group.role,
            "Job Count": group.job_count,
            "Location + 50 Miles": f"{group.city}_{group.name}",
            "Population": group.population
        })
    return pd.DataFrame(output_data)

def process_csv_with_clustering(file_path, advertiser):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    
    job_groups = {}

    for _, row in df.iterrows():
        city = row.get("city", "").strip()
        state = normalize_state(row.get("state", "").strip())
        check = is_50_miles(city, state)
        if(check!=None):
            city = check
        else:
            continue
        title = row.get("title", "").strip()
        population = pd.to_numeric(row.get("population", 0), errors="coerce") or 0

        role = normalize_role(advertiser, title)

        identifier = f"{city}_{state}_{role}"

        if identifier in job_groups:
            job_groups[identifier].increment_job_count()
            job_groups[identifier].increment_population_count(population)
        else:
            job_group = JobGroupClustering(city, state, role)
            job_group.increment_population_count(population)
            job_groups[identifier] = job_group

    return job_groups
# Main work

def remove_column(df, column_index):
    """Remove a specific column by index from the DataFrame and return the new DataFrame."""
    return df.drop(df.columns[column_index], axis=1)

@app.route('/')
def index():
    """Render the main input form."""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    """Process the CSV file based on user inputs and display results."""
    try:
        csv_url = request.form['csv_url']
        advertiser_name = request.form['advertiser_name']

        # Download and process the CSV file
        df = download_csv(csv_url)

        # Task 1:
        response = requests.get(csv_url)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        job_groups = process_csv_with_location(csv_data, advertiser_name)
        df_output = convert_job_groups_to_df_with_location(job_groups)
        table_1 = df_output.to_html(classes='table table-bordered', index=False)

        # Task 2:
        response1 = requests.get(csv_url)
        response1.raise_for_status()
        csv_data = StringIO(response1.text)
        job_groups = process_csv_based_on_state(csv_data, advertiser_name)
        df_output = convert_job_groups_to_df_with_state(job_groups)
        table_2 = df_output.to_html(classes='table table-bordered', index=False)

        # Task 3:
        response2 = requests.get(csv_url)
        response2.raise_for_status()
        csv_data = StringIO(response2.text)
        job_groups = process_csv_with_clustering(csv_data, advertiser_name)
        df_without_third = convert_job_groups_to_df_with_clustering(job_groups)
        table_3 = df_without_third.to_html(classes='table table-bordered', index=False)

        return render_template('result.html', 
                               advertiser_name=advertiser_name,
                               table_1=table_1, 
                               table_2=table_2, 
                               table_3=table_3)
    except Exception as e:
        return f"An error occurred: {e}"

if __name__ == '__main__':
    app.run(debug=True)