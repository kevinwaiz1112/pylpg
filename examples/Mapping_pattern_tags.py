import csv
import random
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *

# Lese die CSV-Datei ein
# Möglicherweise muss auch das Alter zugewiesen werden, da in Synthesizer das Alter als Intervall angegeben wird
def read_csv_and_assign_living_pattern_tags(csv_filename):
    tag_mapping = {
        "Kind unter 6": LivingPatternTags.Living_Pattern_Kindergarden,
        "Schüler": LivingPatternTags.Living_Pattern_School,
        "Student": random.choice([# Zufall eventuell entfernen
            LivingPatternTags.Living_Pattern_University,
            LivingPatternTags.Living_Pattern_University_Student_Independent
        ]),
        "Rentner": LivingPatternTags.Living_Pattern_Retiree,
        "sonstige NEP": LivingPatternTags.Living_Pattern_All,
        "Vollzeit": random.choice([# Zufall eventuell entfernen
            LivingPatternTags.Living_Pattern_Office_Job,
            LivingPatternTags.Living_Pattern_Office_Job_Early_5_7am,
            LivingPatternTags.Living_Pattern_Office_Job_Late_9_11am,
            LivingPatternTags.Living_Pattern_Office_Job_Medium_7_9am,
            LivingPatternTags.Living_Pattern_Office_Worker
        ]),
        "Teilzeit": LivingPatternTags.Living_Pattern_Part_Time_Job,
        "Erwerbslos": LivingPatternTags.Living_Pattern_Stay_at_Home,
    }

    with open(csv_filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Überspringe Header-Zeile
        data = list(reader)

    tagged_data = []

    for row in data:
        gebaeude_id, haushalt_id, alter, geschlecht, status = row
        if status in tag_mapping:
            tag = tag_mapping[status]
        else:
            tag = LivingPatternTags.Living_Pattern_All  # Standard-Tag

        tagged_data.append({
            "Gebaeude_ID": gebaeude_id,
            "Haushalt_ID": haushalt_id,
            "Alter": alter,
            "Geschlecht": geschlecht,
            "Status": status,
            "Tag": tag,
        })

    return tagged_data



def LPG_sekquasens_coupling(csv_filename):
    tagged_data = read_csv_and_assign_living_pattern_tags(csv_filename)
    household_data_by_gebaeude = {}

    for entry in tagged_data:
        gebaeude_id = entry["Gebaeude_ID"]
        if gebaeude_id not in household_data_by_gebaeude:
            household_data_by_gebaeude[gebaeude_id] = []

        person_data = PersonData(entry["Alter"], entry["Geschlecht"], entry["Status"], entry["Tag"])
        household_data_by_gebaeude[gebaeude_id].append(person_data)

    household_data = []

    for gebaeude_id, person_data_list in household_data_by_gebaeude.items():
        person_specification = HouseholdDataPersonSpecification(person_data_list)
        household_data_spec = HouseholdDataSpecificationType.ByPersons

        household_data.append(
            HouseholdData(gebaeude_id, "name", person_specification, household_data_spec)
        )

    return household_data


"""""
Beispiel:

csv_filename = "personendaten.csv"  # Ersetzen Sie dies durch den tatsächlichen Dateinamen
tagged_data = read_csv_and_assign_living_pattern_tags(csv_filename)

# Ausgabe der Daten mit den zugewiesenen Tags
for entry in tagged_data:
    print(entry)
    
"""""