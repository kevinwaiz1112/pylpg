from pylpg import lpg_execution, lpgdata, lpgpythonbindings
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
import utils
import random
import pandas
import visualizer
import time
import os
from Mapping_pattern_tags import *

"""""
Filtert die Templates auf Basis der angegebenen Anzahl an Personen und dem Gender und wählt auf dieser Basis das Template aus.
Wird kein Template gefunden, so wird ein zufälliges passendes gewählt

Für die Filterung die LivingPatternTags nutzen mit: 
self.LivingPatternTag = value ; analog zu Alter und Gender für PersonData 

"""""

csv_filename_persons = r"C:\03_Repos\pylpg\Data\persons_moabit1.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
household_data = LPG_sekquasens_coupling(csv_filename_persons)

# Simulationsparameter
startdate = "01.01.2020"  # Wichtig: MM.TT.JJJJ
enddate = "01.03.2020"

# Führen Sie die Simulation für jedes Gebäude durch

for building_id, households in household_data.items():
    # Initialisieren eine leere Liste für die Haushalte des aktuellen Gebäudes
    all_households = []

    for hh_data in households.values():
        # Fügen Sie den Haushalt zur all_households-Liste hinzu
        all_households.append(hh_data)

    print(all_households)

    # Führen Sie lpg_execution.execute_lpg_with_many_householdata für das aktuelle Gebäude durch
    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2020,
        householddata=all_households,
        housetype=HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,
        startdate=startdate,
        enddate=enddate,
        simulate_transportation=False,
        resolution="01:00:00",
        # resolution_int="00:01:00",
        random_seed=2,
        building_id=building_id
    )

    # Hier können Sie mit dem DataFrame `df` arbeiten, z.B., es in eine CSV-Datei exportieren.

    output_folder = r"C:\03_Repos\pylpg\Data\Results\Results_" + building_id
    output_filename = f"LPG_{building_id}.csv"
    output_filepath = os.path.join(output_folder, output_filename)
    df.to_csv(output_filepath, index=True, sep=";")
    print(f"Successfully exported dataframe to {output_filepath}")
