from pylpg import lpg_execution, lpgdata, lpgpythonbindings
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
import utils
import random
import pandas
import visualizer
import multiprocessing
import time
import shutil
import os
from Mapping_pattern_tags import *

"""""
Filtert die Templates auf Basis der angegebenen Anzahl an Personen und dem Gender und wählt auf dieser Basis das Template aus.
Wird kein Template gefunden, so wird ein zufälliges passendes gewählt

Für die Filterung die LivingPatternTags nutzen mit: 
self.LivingPatternTag = value ; analog zu Alter und Gender für PersonData 

"""""


def simulate_building(building_id, households, startdate, enddate, output_folder):
    all_households = list(households.values())

    # Aktualisiere den Ausgabeordner für dieses Gebäude
    output_folder = os.path.join(output_folder, f"Results_{building_id}")

    # Führen Sie lpg_execution.execute_lpg_with_many_householdata für das aktuelle Gebäude durch
    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2020,
        householddata=all_households,
        housetype=HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,
        startdate=startdate,
        enddate=enddate,
        simulate_transportation=False,
        resolution="01:00:00",
        random_seed=2,
        building_id=building_id,
        output_folder = output_folder
    )

    # Hier können Sie mit dem DataFrame `df` arbeiten, z.B., es in eine CSV-Datei exportieren.

    # Löschen der Datei "profilegenerator.copy.db3" (falls vorhanden)
    file_to_delete = os.path.join(output_folder, "profilegenerator.copy.db3")
    if os.path.exists(file_to_delete):
        os.remove(file_to_delete)

    # Löschen der Datei "finished.flag" (falls vorhanden)
    file_to_delete = os.path.join(output_folder, "finished.flag")
    if os.path.exists(file_to_delete):
        os.remove(file_to_delete)

    # Löschen des Ordners "Temporary Files" (falls vorhanden)
    folder_to_delete = os.path.join(output_folder, "Temporary Files")
    if os.path.exists(folder_to_delete):
        shutil.rmtree(folder_to_delete)


if __name__ == "__main__":
    csv_filename_persons = r"C:\03_Repos\pylpg\Data\persons_moabit_8Buildings.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
    household_data = LPG_sekquasens_coupling(csv_filename_persons)

    # Simulationsparameter
    startdate = "01.01.2024"  # Wichtig: MM.TT.JJJJ
    enddate = "01.01.2024"
    start_time = time.time()

    # Erstelle eine Liste von Argumenten für die Gebäudesimulationen
    building_simulations_args = [(building_id, households, startdate, enddate, r"C:\03_Repos\pylpg\Data\Results") for building_id, households in
                                 household_data.items()]

    # Erstelle einen Pool von Prozessen, um die Simulationen parallel auszuführen
    num_processes = 2  # Anzahl der parallel auszuführenden Prozesse
    pool = multiprocessing.Pool(processes=num_processes)

    # Führe die Simulationen parallel aus
    pool.starmap(simulate_building, building_simulations_args)

    # Beende den Pool und warte auf das Ende aller Prozesse
    pool.close()
    pool.join()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Das Skript hat {execution_time} Sekunden gedauert.")