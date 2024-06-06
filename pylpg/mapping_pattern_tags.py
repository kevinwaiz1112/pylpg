import csv
import json
import random
import pandas as pd
import statistics
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
from collections import defaultdict
import random


"""""
Skript zur Filterung und Zuweisung der Daten aus Synthesizer an den LPG:

Die Daten der Personen aus Synthesizer werden benutzt, um daraus Haushalte im LPG zu erstellen, die
so gut wie möglich denen aus Synthesizer gleichen. Dabei wird das Alter, das Geschlecht und vor allem das LivingPattern 
der Personen verwendet. Schlussendlich soll die Zuweisung der Haushalte im LPG über die TemplatenNames/HouseholdNames erfolgen

    ByPersons = "ByPersons"
    ByTemplateName = "ByTemplateName"
    ByHouseholdName = "ByHouseholdName"

"""

def read_csv_and_assign_living_pattern_tags(person_presence_data_json):

    # Mapping keys:
    # "toddler" = "Kind unter 6"
    # "pupil" = "Schüler"
    # "student" = "Student"
    # "retiree" = "Rentner"
    # "other non-working individual" = "sonstige NEP"
    # "full-time worker" = "Vollzeit"
    # "part-time worker" = "Teilzeit"
    # "vocational trainee" = "Azubi"
    # "unemployed" = "Erwerbslos"

    tag_mapping = {
        "toddler": LivingPatternTags.Living_Pattern_Kindergarden,
        "pupil": LivingPatternTags.Living_Pattern_School,
        # Living_Pattern_School_Medium_7_9am
        "student": LivingPatternTags.Living_Pattern_University_Student_Independent, # eventuell nach dem String: Student suchen statt dem Pattern
        # Living_Pattern_University
        # Living_Pattern_University_Student_Living_at_Home
        "retiree": LivingPatternTags.Living_Pattern_Retiree,
        "other non-working individual": LivingPatternTags.Living_Pattern_Stay_at_Home_Regular,
        "full-time worker": LivingPatternTags.Living_Pattern_Office_Job_Medium_7_9am,
        # LivingPatternTags.Living_Pattern_Office_Job,
        # LivingPatternTags.Living_Pattern_Office_Job_Early_5_7am,
        # LivingPatternTags.Living_Pattern_Office_Job_Late_9_11am,
        # LivingPatternTags.Living_Pattern_Office_Job_Medium_7_9am,
        # LivingPatternTags.Living_Pattern_Office_Worker
        # Living_Pattern_Shift_work = "Living Pattern / Shift work"
        # Living_Pattern_Shift_work_3_Shifts_A = "Living Pattern / Shift work / 3 Shifts A"
        # Living_Pattern_Shift_work_3_Shifts_B = "Living Pattern / Shift work / 3 Shifts B"
        # Living_Pattern_Work_From_Home
        # Living_Pattern_Work_From_Home_Full_Time_5_days
        "part-time worker": LivingPatternTags.Living_Pattern_Part_Time_Job,
        # Living_Pattern_Work_From_Home_Part_Time
        "vocational trainee": LivingPatternTags.Living_Pattern_Shift_work_3_Shifts_A,
        "unemployed": LivingPatternTags.Living_Pattern_Stay_at_Home,
    }

    """""        
    "6": random.choice([  # Zufall eventuell entfernen
        LivingPatternTags.Living_Pattern_Office_Job,
        LivingPatternTags.Living_Pattern_Office_Job_Early_5_7am,
        LivingPatternTags.Living_Pattern_Office_Job_Late_9_11am,
        LivingPatternTags.Living_Pattern_Office_Job_Medium_7_9am,
        LivingPatternTags.Living_Pattern_Office_Worker
    ]),
    """

    # Funktion zur Konvertierung des Alters von Intervallen in Werte
    def convert_age(age):
        # Intervalle und zugehörige Werte
        age_intervals = [
            (0, 5), (6, 9), (10, 14), (15, 17), (18, 19),
            (20, 24), (25, 29), (30, 34), (35, 39),
            (40, 44), (45, 49), (50, 54), (55, 59),
            (60, 64), (65, 74), (75, 79), (80, 84), (85, float("inf"))
        ]
        age_values = [statistics.median(interval) for interval in age_intervals]

        # Konvertiere das Alter in den entsprechenden Wert
        for i, (lower, upper) in enumerate(age_intervals):
            if lower <= age <= upper:
                return age_values[i]

    # Pfad zur JSON-Datei
    json_dateipfad = person_presence_data_json

    # Lese die JSON-Datei
    with open(json_dateipfad, 'r', encoding='utf-8') as datei:
        daten = json.load(datei)

    tagged_data = []

    # Durchlaufe alle Features im JSON
    for feature in daten['features']:
        gebaeude_id = feature['id']
        households = feature['properties']['household']
        for household in households:
            haushalt_id = household['household_id']
            for person in household['persons']:
                person_id = person['person_id']
                alter = person['age']
                geschlecht = person['sex']
                status = person['status']

                if status in tag_mapping:
                    tag = tag_mapping[status]
                else:
                    tag = LivingPatternTags.Living_Pattern_All  # Standard-Tag

                tagged_data.append({
                    "Gebaeude_ID": gebaeude_id,
                    "Haushalt_ID": haushalt_id,
                    "Person_ID": person_id,
                    "Alter": alter,
                    "Geschlecht": geschlecht,
                    "Status": status,
                    "Tag": tag,
                })

    return tagged_data

# All household templates registered in the LPG
household_sizes = {
        "CHR01 Couple both at Work": 2,
        "CHR02 Couple, 30 - 64 age, with work": 2,
        "CHR03 Family, 1 child, both at work": 3,
        "CHR04 Couple, 30 - 64 years, 1 at work, 1 at home": 2,
        "CHR05 Family, 3 children, both with work": 5,
        "CHR06 Jak Jobless": 1,
        "CHR07 Single with work": 1,
        "CHR08 Single woman, 2 children, with work": 3,
        "CHR09 Single woman, 30 - 64 years, with work": 1,
        "CHR10 Single man, 30 - 64 age, shift worker": 1,
        "CHR11 Student, Female, Philosophy": 1,
        "CHR12 Student 2, Male, Philosophy": 1,
        "CHR13 Student with Work": 1,
        "CHR14 3 adults: Couple, 30- 64 years, both at work + Senior at home": 3,
        "CHR15 Multigenerational Home: working couple, 2 children, 2 seniors": 6,
        "CHR16 Couple over 65 years": 2,
        "CHR17 Shiftworker Couple": 2,
        "CHR18 Family, 2 children, parents without work": 4,
        "CHR19 Couple, 30 - 64 years, both at work, with homehelp": 3,
        "CHR20 one at work, one work home, 3 children": 5,
        "CHR21 Couple, 30 - 64 years, shift worker": 2,
        "CHR22 Single woman, 1 child, with work": 2,
        "CHR23 Single man over 65 years": 1,
        "CHR24 Single woman over 65 years": 1,
        "CHR25 Single woman under 30 years with work": 1,
        "CHR26 Single woman under 30 years without work": 1,
        "CHR27 Family both at work, 2 children": 4,
        "CHR28 Single man under 30 years without work": 1,
        "CHR29 Single man under 30 years with work": 1,
        "CHR30 Single, Retired Man": 1,
        "CHR31 Single, Retired Woman": 1,
        "CHR32 Couple under 30 years without work": 2,
        "CHR33 Couple under 30 years with work": 2,
        "CHR34 Couple under 30 years, one at work, one at home": 2,
        "CHR35 Single woman, 30 - 64 years, with work": 1,
        "CHR36 Single woman, 30 - 64 years, without work": 1,
        "CHR37 Single man, 30 - 64 years, with work": 1,
        "CHR38 Single man, 30 - 64 years, without work": 1,
        "CHR39 Couple, 30 - 64 years, with work": 2,
        "CHR40 Couple, 30 - 64 years, without work": 2,
        "CHR41 Family with 3 children, both at work": 5,
        "CHR42 Single man with 2 children, with work": 3,
        "CHR43 Single man with 1 child, with work": 2,
        "CHR44 Family with 2 children, 1 at work, 1 at home": 4,
        "CHR45 Family with 1 child, 1 at work, 1 at home": 3,
        "CHR46 Single woman, 1 child, without work": 2,
        "CHR47 Single woman, 2 children, without work": 3,
        "CHR48 Family with 2 children, without work": 4,
        "CHR49 Family with 1 child, without work": 3,
        "CHR50 Single woman with 3 children, without work": 4,
        "CHR51 Coupleover 65 years II": 2,
        "CHR52 Student Flatsharing": 3,
        "CHR53 2 Parents, 1 Working, 2 Children": 4,
        "CHR54 Retired Couple, no work": 2,
        "CHR55 Couple with work around 40": 2,
        "CHR56 Couple with 2 children, husband at work": 4,
        "CHR57 Family with 2 Children, Man at work": 4,
        "CHR58 Retired Couple, no work, no cooking": 2,
        "CHR59 Family, 3 children, parents without work": 5,
        "CHR60 Family, 1 toddler, one at work, one at home": 3,
        "CHR61 Family, 1 child, both at work, early living pattern": 3,
        "CHS01 Couple with 2 Children, Dad Employed": 4,
        "CHS04 Retired Couple, no work": 2,
        "CHS12 Shiftworker Couple": 2,
        "OR01 Single Person Office": 1
        #"CHR62 Couple both Working from Home": 2
}


def find_pattern(person_presence_data_json):
    # Liest die json aus und gibt die Daten zu den Haushalten weiter
    household_data = read_csv_and_assign_living_pattern_tags(person_presence_data_json)

    # Erstelle ein Dictionary, um Haushalte zu gruppieren
    grouped_household_data = defaultdict(lambda: {'Gebaeude_ID': None, 'Haushalt_ID': None, 'Personen': []})

    # Gruppiere die Daten nach 'Haushalt_ID'
    for entry in household_data:
        haushalt_id = entry['Haushalt_ID']
        if not grouped_household_data[haushalt_id]['Gebaeude_ID']:
            grouped_household_data[haushalt_id]['Gebaeude_ID'] = entry['Gebaeude_ID']
            grouped_household_data[haushalt_id]['Haushalt_ID'] = haushalt_id

        person_info = {
            'Person_ID': entry['Person_ID'],
            'Alter': entry['Alter'],
            'Geschlecht': entry['Geschlecht'],
            'Tag': entry['Tag']
        }
        grouped_household_data[haushalt_id]['Personen'].append(person_info)

    # Konvertiere das gruppierte Dictionary in eine Liste
    result = list(grouped_household_data.values())
    # Zeige das Ergebnis zur Überprüfung
    # for entry in result:
       # print(entry)

    best_pattern = []
    all_best_pattern = []

    # Iteriere über jeden Haushalt in result
    for household in result:
        # Bestimme die Anzahl der Personen im Haushalt
        num_persons = len(household['Personen'])
        num_children = 0
        num_seniors = 0

        for person_info in household['Personen']:
            age = person_info['Alter']

            if age < 18:
                num_children += 1
            elif age >= 65:
                num_seniors += 1

        if num_persons > 5:
            # Wenn mehr als 6 Personen im Haushalt sind, verwende das spezifische Template
            best_pattern = {
                'Gebaeude_ID': household['Gebaeude_ID'],
                'Haushalt_ID': household['Haushalt_ID'],
                'Template Name': 'CHR15 Multigenerational Home: working couple, 2 children, 2 seniors'
            }
            # print("Template for:", household['Haushalt_ID'], ", Persons:", household['Personen'], "\n", best_pattern,"\n")
            all_best_pattern.append(best_pattern)
        else:
            # Finde die für diese Anzahl von Personen passenden Templates aus household_sizes
            eligible_templates = {template: size for template, size in household_sizes.items() if size == num_persons}
            if not eligible_templates:
                continue
                # eligible_templates = {template: size for template, size in household_sizes.items()}  # Keine passenden Templates gefunden

            # Initialisiere Variablen, um die beste Übereinstimmung zu verfolgen
            template_data = []
            for template_name, template in TemplatePersons.__dict__.items():
                if (
                        isinstance(template, TemplatePersonEntry)
                        and template.TemplateName in eligible_templates
                ):
                    # Überprüfe, ob ein Template mit dem gleichen Namen bereits in der Liste ist
                    existing_template = next(
                        (
                            t
                            for t in template_data
                            if t['Template Name'] == template.TemplateName
                        ),
                        None
                    )
                    if existing_template:
                        # Template mit dem gleichen Namen bereits vorhanden
                        existing_template['ages'].append(template.Age)
                        existing_template['patterns'].append(template.LivingPattern)
                        existing_template['gender'].append(template.Gender.value)
                    else:
                        # Template mit diesem Namen noch nicht in der Liste
                        new_template = {
                            'Template Name': template.TemplateName,
                            'ages': [template.Age],
                            'gender': [template.Gender.value],
                            'patterns': [template.LivingPattern]
                        }
                        template_data.append(new_template)

            # Überprüfe ob es ein passendes Template gibt, dass die Altersstruktur matched
            best_matches = []
            num_children = sum(person['Alter'] < 18 for person in household['Personen'])
            num_seniors = sum(person['Alter'] > 64 for person in household['Personen'])
            best_match = None
            for entry in template_data:
                template_num_children = sum(age < 18 for age in entry['ages'])
                template_num_seniors = sum(age >= 65 for age in entry['ages'])

                if (
                        num_children == template_num_children
                        and num_seniors == template_num_seniors
                ):
                    best_match = entry
                    best_matches.append(best_match)

            if not best_matches:
                # Wenn keine passenden Templates nach Altersanforderungen gefunden wurden, wähle alle verfügbaren
                best_matches = template_data

            # Finde das beste Template mithilfe eines Scoresystems: Das Template, welches die meisten Übereinstimmungen
            # aufweist wird ausgewählt; bei Gleichstand wähle zufällig eines Derjenigen mit der höchsten Punktzahl
            best_pattern_scores = [(entry, 0) for entry in best_matches]
            for person_info in household['Personen']:
                for i, (entry, score) in enumerate(best_pattern_scores):
                    if person_info['Tag'] in entry['patterns']:
                        best_pattern_scores[i] = (entry, score + 1)

            # Finde das Template mit der höchsten Punktzahl
            max_score = max(score for (_, score) in best_pattern_scores)
            best_patterns = [entry for entry, score in best_pattern_scores if score == max_score]

            if len(best_patterns) > 1:
                if num_persons == 1:
                    best_patterns = [pattern for pattern in best_patterns if
                                         pattern['gender'][0] == person_info['Geschlecht'].capitalize()]
                best_pattern = random.choice(best_patterns)
            else:
                best_pattern = best_patterns[0]

            # print("Template for:", household['Haushalt_ID'], ", Persons:",household['Personen'],"\n", best_pattern,"\n")
            best_pattern_name = best_pattern['Template Name']
            best_pattern = {
                'Gebaeude_ID': household['Gebaeude_ID'],
                'Haushalt_ID': household['Haushalt_ID'],
                'Template Name': best_pattern_name
            }

            # if household["Gebaeude_ID"] == "ID_D2CE023A-D5E0-4E8E-A330-E161739BF000":
            #   print("Ergebnis:", best_pattern)
            # print("-----------------------------------------------------------------")

            if not best_pattern:
                print("Kein Pattern gefunden. Es wird ein zufälliges mit gleicher Personenzahl gewählt")
                random_temp = random.choice(best_matches)
                random_temp_name = random_temp[0]['Template Name']
                best_pattern = {
                    'Gebaeude_ID' : household['Gebaeude_ID'],
                    'Haushalt_ID': household['Haushalt_ID'],
                    'Template Name': random_temp_name
                }
            all_best_pattern.append(best_pattern)

    return all_best_pattern, household_data

def print_building_statistics(building_data):

    # Gibt die Anzahl der Gebäude aus die betrachtet plus Personen und Haushalte
    total_buildings = len(building_data)
    print(f"Gesamtanzahl der Gebäude: {total_buildings}")
    print("-" * 40)

    for building_id, households in sorted(building_data.items()):
        num_households = len(households)
        num_persons = sum(len(data["PersonData"]) for data in households.values())

        print(f"Gebäude ID: {building_id}")
        print(f"Anzahl der Haushalte: {num_households}")
        print(f"Anzahl der Personen: {num_persons}")
        print("-" * 30)

def LPG_sekquasens_coupling(person_presence_data_json):

    # Finde pro Haushalt das beste Template
    best_pattern, tagged_data = find_pattern(person_presence_data_json)
    building_data = {}
    # Iterieren durch die `tagged_data` und fülle die Datenstruktur auf
    for data_point in tagged_data:
        building_id = data_point["Gebaeude_ID"]
        household_id = data_point["Haushalt_ID"]
        person_data = PersonData(data_point["Alter"], data_point["Geschlecht"], data_point["Tag"], data_point["Person_ID"])

        # Wenn das Gebäude noch nicht in der Struktur existiert, fügen Sie es hinzu
        if building_id not in building_data:
            building_data[building_id] = {}

        # Wenn der Haushalt noch nicht im Gebäude existiert, fügen Sie ihn hinzu
        if household_id not in building_data[building_id]:
            building_data[building_id][household_id] = {"PersonData": [], "Template Name": None}

        # Fügen Sie die Personendaten zum Haushalt hinzu
        building_data[building_id][household_id]["PersonData"].append(person_data)

    # Iteriere durch jeden Haushalt in `best_pattern` und füge das Template hinzu
    for entry in best_pattern:
        building_id = entry["Gebaeude_ID"]
        household_id = entry["Haushalt_ID"]
        template_name = entry["Template Name"]

        # Überprüfen Sie, ob das Gebäude und der Haushalt in der Datenstruktur existieren
        if building_id in building_data and household_id in building_data[building_id]:
            # Fügen Sie das Template zum Haushalt hinzu
            building_data[building_id][household_id]["Template Name"] = template_name

    # Gibt Gebäudestatistik aus
    print_building_data = False
    if print_building_data == True:
        print_building_statistics(building_data)

    # Sammelt die Daten und erstellt die Haushalte über die HouseholdData Klasse für die LPG Simulation
    household_data = {}
    for building_id, households in building_data.items():
        if building_id not in household_data:
            household_data[building_id] = {}

        for household_id, data in households.items():
            householdref= data["Template Name"] # Template Name
            hhnamespec = HouseholdNameSpecification(householdref)
            hh_data = HouseholdData(
                HouseholdNameSpec=hhnamespec,
                UniqueHouseholdId=household_id,
                Name=household_id,
                HouseholdDataSpecification=HouseholdDataSpecificationType.ByHouseholdName,
            )
            household_data[building_id][household_id] = hh_data

    return household_data

def calculate_annual_requirement(df):

    # Annahme, dass das Datum im Format '%d.%m.%Y %H:%M' vorliegt
    df['Date'] = pd.to_datetime(df['Time'], format='%d.%m.%Y %H:%M').dt.date
    unique_days = df['Date'].nunique()
    total = df['Sum'].sum()
    average_daily_kwh = total / unique_days
    annual_energy_requirement = average_daily_kwh * 365
    return annual_energy_requirement

def resolution_to_seconds(resolution):

    h, m, s = map(int, resolution.split(':'))

    return h * 3600 + m * 60 + s

