from pylpg import lpg_execution
import multiprocessing
import time
import shutil
import os
import pandas as pd
from mapping_pattern_tags import *

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

def simulate_building(building_id, households, startdate, enddate, output_folder, status_file, resolution):
    all_households = list(households.values())

    # Aktualisiere den Ausgabeordner für dieses Gebäude
    output_folder = os.path.join(output_folder, f"Results_{building_id}")
    # Directory in welchem die Berechnung durchgeführt wird; Wird nach Ende der Sim gelöscht
    calc_folder = r"C:\03_Repos\sekquasens_interfaces\pylpg\pylpg"

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

    # Löschen der nicht weiter benötigten Dateien (falls vorhanden)
    file_to_delete = os.path.join(output_folder, "profilegenerator.copy.db3")
    if os.path.exists(file_to_delete):
        os.remove(file_to_delete)

    file_to_delete = os.path.join(output_folder, "finished.flag")
    if os.path.exists(file_to_delete):
        os.remove(file_to_delete)

    folder_to_delete = os.path.join(output_folder, "Temporary Files")
    if os.path.exists(folder_to_delete):
        shutil.rmtree(folder_to_delete)

    folder_to_delete = os.path.join(calc_folder, "C" + "1_" + str(building_id))
    if os.path.exists(folder_to_delete):
        shutil.rmtree(folder_to_delete)

    folder_to_delete = os.path.join(calc_folder, "LPG_win_"+ str(building_id))
    if os.path.exists(folder_to_delete):
        shutil.rmtree(folder_to_delete)


"""
    from simulation_setup import *

    # Simulationsparameter
    csv_filename_persons = r"C:\03_Repos\sekquasens_interfaces\pylpg\Data\persons_moabit_oneBuilding.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
    output_folder = r"C:\03_Repos\sekquasens_interfaces\pylpg\Data\Results"
    startdate = "01.01.2024"  # Wichtig: MM.TT.JJJJ
    enddate = "01.01.2024"
    start_time = time.time()
    resolution = "01:00:00"
    num_processes = 1  # Anzahl der parallel auszuführenden Prozesse = Anzahl Kerne (RAM beachten !)

    # Erstellt die benötigten Haushalte pro Gebäude mithilfe der Templates und Klassen des LPG
    household_data = LPG_sekquasens_coupling(csv_filename_persons)
    # Erstelle eine Liste von Argumenten für die Gebäudesimulationen
    status_file = os.path.join(output_folder, "simulation_status.txt")
    force_resimulate_all = False  # Setzen Sie dies auf True, um alle Gebäude neu zu simulieren
    completed_buildings = get_completed_buildings(status_file)
    building_simulations_args = []
    for building_id, households in household_data.items():
        if force_resimulate_all or building_id not in completed_buildings:
            building_simulations_args.append(
                (building_id, households, startdate, enddate, output_folder, status_file, resolution, force_resimulate_all))

    # Erstelle einen Pool von Prozessen, um die Simulationen parallel auszuführen
    pool = multiprocessing.Pool(processes=num_processes)
    # Führe die Simulationen parallel aus
    pool.starmap(simulate_building, building_simulations_args)
    # Beende den Pool und warte auf das Ende aller Prozesse
    pool.close()
    pool.join()

    # Dauer der Simulation
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Das Skript hat {execution_time} Sekunden gedauert.")

"""