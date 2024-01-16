from pylpg import lpg_execution
import multiprocessing
import time
import shutil
import os
from mapping_pattern_tags import *

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
        resolution="01:00:00",
        random_seed=2,
        building_id=building_id,
        output_folder=output_folder,
        calc_folder=calc_folder
    )


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


if __name__ == "__main__":

    """
    Function
    ----------
    retrieves weather data from Climate Change Service (CDS) through the API

    Parameters
    ----------
    csv_filename_persons: string 
        Path to folder that contains the person information
    output_folder: string 
        Path to results directory
    startdate: string
        MM.DD.YYYY
    enddate: string
        MM.DD.YYYY
    num_processes: int
        1 - max. number of cores

    Returns
    -------
    data : csv Format
    
    """

    # Simulationsparameter
    csv_filename_persons = r"C:\03_Repos\sekquasens_interfaces\pylpg\Data\persons_moabit_8Buildings.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
    output_folder = r"C:\03_Repos\sekquasens_interfaces\pylpg\Data\Results"
    startdate = "01.01.2024"  # Wichtig: MM.TT.JJJJ
    enddate = "01.01.2024"
    start_time = time.time()
    num_processes = 8  # Anzahl der parallel auszuführenden Prozesse = Anzahl Kerne (RAM beachten !)

    # Erstellt die benötigten Haushalte pro Gebäude mithilfe der Templates und Klassen des LPG
    household_data = LPG_sekquasens_coupling(csv_filename_persons)
    # Erstelle eine Liste von Argumenten für die Gebäudesimulationen
    building_simulations_args = [(building_id, households, startdate, enddate, output_folder) for building_id, households in
                                 household_data.items()]

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