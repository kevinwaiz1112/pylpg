from pylpg import lpg_execution
from mapping_pattern_tags import *
import multiprocessing
import time
import shutil
import os
import pandas as pd
import sqlite3


"""""
Filtert die Templates auf Basis der angegebenen Anzahl an Personen und dem Gender und wählt auf dieser Basis das Template aus.
Wird kein Template gefunden, so wird ein zufälliges passendes gewählt

Für die Filterung die LivingPatternTags nutzen mit: 
self.LivingPatternTag = value ; analog zu Alter und Gender für PersonData 

"""""

def get_completed_buildings(status_file):
    if os.path.exists(status_file):
        with open(status_file, 'r') as file:
            completed_buildings = file.read().splitlines()
            return set(completed_buildings)
    return set()

def update_status_file(status_file, building_id):
    with open(status_file, 'a') as file:
        file.write(f"{building_id}\n")

def simulate_building(building_id, households, startdate, enddate, output_folder, calc_folder, status_file, resolution, city, weather_path, house_class):
    all_households_unsorted = list(households.values())
    # all_households = sorted(all_households_unsorted, key=lambda x: x.HouseholdNameSpec.HouseholdReference)
    all_households = sorted(
        all_households_unsorted,
        key=lambda x: x.HouseholdNameSpec.HouseholdReference
        if x.HouseholdNameSpec.HouseholdReference is not None else ""
    )
    startdate = startdate.strftime("%m.%d.%Y")
    enddate = enddate.strftime("%m.%d.%Y")
    # Aktualisiere den Ausgabeordner für dieses Gebäude
    output_folder = os.path.join(output_folder, f"Results_{building_id}")
    # Directory in welchem die Berechnung durchgeführt wird; Wird nach Ende der Sim gelöscht
    house_category = HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling
    if house_class == "SFH":
        house_category = HouseTypes.HT20_Single_Family_House_no_heating_cooling

    # Führen Sie lpg_execution.execute_lpg_with_many_householdata für das aktuelle Gebäude durch
    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2020,
        householddata=all_households,
        housetype=house_category,
        startdate=startdate,
        enddate=enddate,
        simulate_transportation=False,
        resolution=resolution,
        random_seed=-1,
        building_id=building_id,
        output_folder=output_folder,
        calc_folder=calc_folder,
        city=city,
        weather_path=weather_path
    )

    update_status_file(status_file, building_id)
    households_ids = []
    for idx, hhd in enumerate(all_households):
        hhd.Name = hhd.Name or f"Household_{idx + 1}"
        households_ids.append(hhd.Name)
    rename_files(output_folder, households_ids)

    # Liste der zu löschenden Dateien und Ordner
    files_and_folders_to_delete = [
        os.path.join(output_folder, "profilegenerator.copy.db3"),
        os.path.join(output_folder, "finished.flag"),
        os.path.join(output_folder, "Temporary Files"),
        os.path.join(calc_folder, "C1_" + str(building_id)),
        os.path.join(calc_folder, "LPG_win_" + str(building_id))
    ]

    # Durchlaufen der Liste und Löschen der Dateien/Ordner, falls vorhanden
    delete_files = True
    if delete_files == True:
        for path in files_and_folders_to_delete:
            if os.path.exists(path):
                if os.path.isfile(path):
                    os.remove(path)  # Löschen einer Datei
                elif os.path.isdir(path):
                    shutil.rmtree(path)  # Löschen eines Ordners


def rename_files(output_directory, household_ids):
    directory = os.path.join(output_directory, f"Results")
    print(f"Checking directory: {directory}")
    for filename in os.listdir(directory):
        if filename.startswith("SumProfiles_3600s.HH") and filename.endswith(".csv"):
            # Extrahiere die Nummer aus dem Dateinamen
            parts = filename.split('.')
            number = int(parts[1][2:])  # Teile den Dateinamen und hole die Nummer

            # Prüfe, ob die Nummer innerhalb des Bereichs der IDs ist
            if 1 <= number <= len(household_ids):
                new_filename = f"SumProfiles_3600s.HH{number}_{household_ids[number - 1]}.{parts[2]}.csv"
                os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

        elif filename.startswith("BodilyActivityLevel.Outside.HH") and filename.endswith(".json"):
            # Extrahiere die Nummer aus dem Dateinamen
            parts = filename.split('.')
            number = int(parts[2][2:])  # Teile den Dateinamen und hole die Nummer

            # Prüfe, ob die Nummer innerhalb des Bereichs der IDs ist
            if 1 <= number <= len(household_ids):
                new_filename = f"BodilyActivityLevel.Outside.HH{number}_{household_ids[number - 1]}.json"
                os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

    for filename in os.listdir(output_directory):
        if filename.startswith("Persons.HH") and filename.endswith(".txt"):
            # Extrahiere die Nummer aus dem Dateinamen
            parts = filename.split('.')
            number = int(parts[1][2:])  # Teile den Dateinamen und hole die Nummer

            # Prüfe, ob die Nummer innerhalb des Bereichs der IDs ist
            if 1 <= number <= len(household_ids):
                new_filename = f"Persons.HH{number}_{household_ids[number - 1]}.txt"
                os.rename(os.path.join(output_directory, filename), os.path.join(output_directory, new_filename))

def save_annual_requirements_to_csv(building_id, output_folder, resolution):
    seconds = resolution_to_seconds(resolution)
    water_types = ['Cold Water', 'Warm Water', 'Hot Water']
    results = {}

    # Berechne den jährlichen Bedarf für Strom
    electricity_file_path = os.path.join(output_folder, f"Results_{building_id}", "Results",
                                         f"SumProfiles_{seconds}s.House.Electricity.csv")
    if os.path.exists(electricity_file_path):
        df_electricity = pd.read_csv(electricity_file_path, delimiter=';')
        results['Electricity_kWh'] = calculate_annual_requirement(df_electricity.rename(columns={'Sum [kWh]': 'Sum'}))

    # Berechne den jährlichen Bedarf für Wasser (kalt, warm, heiß)
    for water_type in water_types:
        water_file_path = os.path.join(output_folder, f"Results_{building_id}", "Results",
                                       f"SumProfiles_{seconds}s.House.{water_type}.csv")
        if os.path.exists(water_file_path):
            df_water = pd.read_csv(water_file_path, delimiter=';')
            results[f'{water_type.replace(" ", "_")}_L'] = calculate_annual_requirement(
                df_water.rename(columns={'Sum [L]': 'Sum'}))

    # Speichere die Ergebnisse in einer CSV-Datei
    output_file_path = os.path.join(output_folder, f"Results_{building_id}", "annual_requirements.csv")
    header = "Gebaeude-ID,Electricity_kWh,Cold_Water_L,Warm_Water_L,Hot_Water_L\n"
    data_line = f"{building_id},{results.get('Electricity_kWh', 0)},{results.get('Cold_Water_L', 0)},{results.get('Warm_Water_L', 0)},{results.get('Hot_Water_L', 0)}\n"

    # Prüfen, ob die Datei existiert, und entsprechend den Header schreiben
    if not os.path.exists(output_file_path):
        with open(output_file_path, "w") as file:
            file.write(header)

    with open(output_file_path, "a") as file:
        file.write(data_line)

