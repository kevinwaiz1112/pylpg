from pylpg import lpg_execution, lpgdata, lpgpythonbindings
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
import utils
import random
import pandas
import visualizer
import time
from Mapping_pattern_tags import *

"""""
Filtert die Templates auf Basis der angegebenen Anzahl an Personen und dem Gender und wählt auf dieser Basis das Template aus.
Wird kein Template gefunden, so wird ein zufälliges passendes gewählt

Für die Filterung die LivingPatternTags nutzen mit: 
self.LivingPatternTag = value ; analog zu Alter und Gender für PersonData 

"""""

csv_filename = "personendaten.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
household_data = LPG_sekquasens_coupling(csv_filename)

# Simulationsparameter
random.seed(2)
startdate = "01.01.2020"
enddate = "01.01.2020"

# Führen Sie die Simulation für jedes Gebäude durch
for hhdata in household_data:
    # Suchen Sie alle Haushalte mit derselben Gebäude-ID
    gebaeude_id = hhdata.unique_household_id
    matching_households = [hh for hh in household_data if hh.unique_household_id == gebaeude_id]

    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2020,
        householddata=matching_households,
        housetype=HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,
        startdate=startdate,
        enddate=enddate,
        simulate_transportation=False,
        random_seed=2
    )

    # Hier können Sie mit dem DataFrame `df` arbeiten, z.B., es in eine CSV-Datei exportieren.

    output_filename = f"lpgexport_{hhdata.unique_household_id}.csv"
    df.to_csv(output_filename, index=True, sep=";")
    print(f"Successfully exported dataframe to {output_filename}")

#TODO:
# Für das Plotting eventuell die Grafiken aus CREST nutzen