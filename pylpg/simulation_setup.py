from pylpg import lpg_execution
from mapping_pattern_tags import *
import multiprocessing
import time
import shutil
import os
import pandas as pd


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

def simulate_building(building_id, households, startdate, enddate, output_folder, calc_folder, status_file, resolution):
    all_households = list(households.values())

    # Aktualisiere den Ausgabeordner für dieses Gebäude
    output_folder = os.path.join(output_folder, f"Results_{building_id}")
    # Directory in welchem die Berechnung durchgeführt wird; Wird nach Ende der Sim gelöscht

    # Führen Sie lpg_execution.execute_lpg_with_many_householdata für das aktuelle Gebäude durch
    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2020,
        householddata=all_households,
        housetype=HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,
        startdate=startdate,
        enddate=enddate,
        simulate_transportation=False,
        resolution=resolution,
        random_seed=2,
        building_id=building_id,
        output_folder=output_folder,
        calc_folder=calc_folder
    )

    update_status_file(status_file, building_id)

    # Liste der zu löschenden Dateien und Ordner
    files_and_folders_to_delete = [
        os.path.join(output_folder, "profilegenerator.copy.db3"),
        os.path.join(output_folder, "finished.flag"),
        os.path.join(output_folder, "Temporary Files"),
        os.path.join(calc_folder, "C1_" + str(building_id)),
        os.path.join(calc_folder, "LPG_win_" + str(building_id))
    ]

    # Durchlaufen der Liste und Löschen der Dateien/Ordner, falls vorhanden
    for path in files_and_folders_to_delete:
        if os.path.exists(path):
            if os.path.isfile(path):
                os.remove(path)  # Löschen einer Datei
            elif os.path.isdir(path):
                shutil.rmtree(path)  # Löschen eines Ordners


def save_annual_requirements_to_csv(building_id, output_folder, resolution):
    seconds = resolution_to_seconds(resolution)
    water_types = ['Cold Water', 'Warm Water', 'Hot Water']
    results = {}

    # Berechne den jährlichen Bedarf für Strom
    electricity_file_path = os.path.join(output_folder, f"Results_{building_id}", f"Results",
                                         f"SumProfiles_{seconds}s.House.Electricity.csv")
    df_electricity = pd.read_csv(electricity_file_path, delimiter=';')
    results['Electricity_kWh'] = calculate_annual_requirement(df_electricity.rename(columns={'Sum [kWh]': 'Sum'}))

    # Berechne den jährlichen Bedarf für Wasser (kalt, warm, heiß)
    for water_type in water_types:
        water_file_path = os.path.join(output_folder, f"Results_{building_id}", f"Results",
                                       f"SumProfiles_{seconds}s.House.{water_type}.csv")
        if os.path.exists(water_file_path):
            df_water = pd.read_csv(water_file_path, delimiter=';')
            results[f'{water_type.replace(" ", "_")}_L'] = calculate_annual_requirement(
                df_water.rename(columns={'Sum [L]': 'Sum'}))

    # Speichere die Ergebnisse in einer CSV-Datei
    with open(os.path.join(output_folder, f"Results_{building_id}", "annual_requirements.csv"), "a") as file:
        header = "Gebaeude-ID,Electricity_kWh,Cold_Water_L,Warm_Water_L,Hot_Water_L\n"
        file.write(header)
        data_line = f"{building_id},{results.get('Electricity_kWh', 0)},{results.get('Cold_Water_L', 0)},{results.get('Warm_Water_L', 0)},{results.get('Hot_Water_L', 0)}\n"
        file.write(data_line)
