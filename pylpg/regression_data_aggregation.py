from datetime import datetime
from pylpg import lpg_execution
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
import multiprocessing
import time
import shutil
import os
import pandas as pd
import sqlite3

def generate_households(households):
    """
    Erstellt Haushalte basierend auf den gegebenen Daten.

    Parameters:
    ----------
    households: dict
        Dictionary mit Informationen zu jedem Haushalt:
        - Template Name
        - Personenanzahl

    Returns:
    -------
    list
        Liste von HouseholdData-Objekten
    """
    household_objects = []

    for household_id, data in households.items():
        # Erstelle eine HouseholdNameSpecification
        householdref = data["Template Name"]
        hhnamespec = HouseholdNameSpecification(householdref)

        # Erstelle das HouseholdData-Objekt
        hh_data = HouseholdData(
            HouseholdNameSpec=hhnamespec,
            UniqueHouseholdId=household_id,  # Z. B. "CHR01"
            Name=household_id,  # Z. B. "CHR01"
            HouseholdDataSpecification=HouseholdDataSpecificationType.ByHouseholdName,
        )

        household_objects.append(hh_data)

    return household_objects

def simulate_building(building_id, startdate, enddate, output_folder, calc_folder, resolution, city, weather_path,
                      all_households, household_info):

    startdate = startdate.strftime("%m.%d.%Y")
    enddate = enddate.strftime("%m.%d.%Y")
    # Aktualisiere den Ausgabeordner für dieses Gebäude
    output_folder = os.path.join(output_folder, f"Results_{building_id}")
    # Directory in welchem die Berechnung durchgeführt wird; Wird nach Ende der Sim gelöscht

    # Führen Sie lpg_execution.execute_lpg_with_many_householdata für das aktuelle Gebäude durch
    df = lpg_execution.execute_lpg_with_many_householdata(
        year=2021,
        householddata=all_households,
        housetype=HouseTypes.HT20_Single_Family_House_no_heating_cooling,
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

    households_ids = []
    for idx, hhd in enumerate(all_households):
        hhd.Name = hhd.Name or f"Household_{idx + 1}"
        households_ids.append(hhd.Name)
    # Benenne Dateien um
    # rename_files(output_folder, all_households, household_info)

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

    return df


def rename_files(output_directory, household_objects, household_info):
    """
    Benennt Dateien in den Simulations-Ordnern um. Nutzt Kürzel (CHR/CHS/OR) und Personenanzahl.
    """
    directory = os.path.join(output_directory, "Results")

    # Mapping von Haushaltsnamen zu Kürzeln und Personenzahlen
    household_short_names = {hh.UniqueHouseholdId: hh.UniqueHouseholdId for hh in household_objects}
    print(household_short_names)
    household_person_counts = {hh.UniqueHouseholdId: household_info[hh.UniqueHouseholdId]["Persons"] for hh in household_objects}
    print(household_person_counts)

    for filename in os.listdir(directory):
        # Für CSV-Dateien
        if filename.startswith("SumProfiles_3600s.HH") and filename.endswith(".csv"):
            parts = filename.split('.')
            number = int(parts[1][2:])  # Extrahiere die Nummer aus HHx

            if 1 <= number <= len(household_short_names):
                short_name = list(household_short_names.values())[number - 1]
                person_count = list(household_person_counts.values())[number - 1]
                new_filename = f"SumProfiles_3600s.HH_{short_name}_{person_count}.{parts[2]}.csv"  # Ohne HHx
                os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

        # Für JSON-Dateien
        elif filename.startswith("BodilyActivityLevel.Outside.HH") and filename.endswith(".json"):
            parts = filename.split('.')
            number = int(parts[2][2:])  # Extrahiere die Nummer aus HHx

            if 1 <= number <= len(household_short_names):
                short_name = list(household_short_names.values())[number - 1]
                person_count = list(household_person_counts.values())[number - 1]
                new_filename = f"BodilyActivityLevel.Outside.HH_{short_name}_{person_count}.json"  # Ohne HHx
                os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

    for filename in os.listdir(output_directory):
        # Für TXT-Dateien
        if filename.startswith("Persons.HH") and filename.endswith(".txt"):
            parts = filename.split('.')
            number = int(parts[1][2:])  # Extrahiere die Nummer aus HHx

            if 1 <= number <= len(household_short_names):
                short_name = list(household_short_names.values())[number - 1]
                person_count = list(household_person_counts.values())[number - 1]
                new_filename = f"Persons.HH_{short_name}_{person_count}.txt"  # Ohne HHx
                os.rename(os.path.join(output_directory, filename), os.path.join(output_directory, new_filename))


def extract_short_name(household_name):
    """
    Extrahiert das Kürzel (z. B. CHR01) aus einem Haushaltsnamen.
    """
    return household_name.split()[0]


if __name__ == "__main__":
    households_all = {
        "CHR01": {"Template Name": "CHR01 Couple both at Work", "Persons": 2},
        "CHR02": {"Template Name": "CHR02 Couple, 30 - 64 age, with work", "Persons": 2},
        "CHR03": {"Template Name": "CHR03 Family, 1 child, both at work", "Persons": 3},
        "CHR04": {"Template Name": "CHR04 Couple, 30 - 64 years, 1 at work, 1 at home", "Persons": 2},
        "CHR05": {"Template Name": "CHR05 Family, 3 children, both with work", "Persons": 5},
        "CHR06": {"Template Name": "CHR06 Jak Jobless", "Persons": 1},
        "CHR07": {"Template Name": "CHR07 Single with work", "Persons": 1},
        "CHR08": {"Template Name": "CHR08 Single woman, 2 children, with work", "Persons": 3},
        "CHR09": {"Template Name": "CHR09 Single woman, 30 - 64 years, with work", "Persons": 1},
        "CHR10": {"Template Name": "CHR10 Single man, 30 - 64 age, shift worker", "Persons": 1},
        "CHR11": {"Template Name": "CHR11 Student, Female, Philosophy", "Persons": 1},
        "CHR12": {"Template Name": "CHR12 Student 2, Male, Philosophy", "Persons": 1},
        "CHR13": {"Template Name": "CHR13 Student with Work", "Persons": 1},
        "CHR14": {"Template Name": "CHR14 3 adults: Couple, 30- 64 years, both at work + Senior at home", "Persons": 3},
        "CHR15": {"Template Name": "CHR15 Multigenerational Home: working couple, 2 children, 2 seniors", "Persons": 6},
        "CHR16": {"Template Name": "CHR16 Couple over 65 years", "Persons": 2},
        "CHR17": {"Template Name": "CHR17 Shiftworker Couple", "Persons": 2},
        "CHR18": {"Template Name": "CHR18 Family, 2 children, parents without work", "Persons": 4},
        "CHR19": {"Template Name": "CHR19 Couple, 30 - 64 years, both at work, with homehelp", "Persons": 3},
        "CHR20": {"Template Name": "CHR20 one at work, one work home, 3 children", "Persons": 5},
        "CHR21": {"Template Name": "CHR21 Couple, 30 - 64 years, shift worker", "Persons": 2},
        "CHR22": {"Template Name": "CHR22 Single woman, 1 child, with work", "Persons": 2},
        "CHR23": {"Template Name": "CHR23 Single man over 65 years", "Persons": 1},
        "CHR24": {"Template Name": "CHR24 Single woman over 65 years", "Persons": 1},
        "CHR25": {"Template Name": "CHR25 Single woman under 30 years with work", "Persons": 1},
        "CHR26": {"Template Name": "CHR26 Single woman under 30 years without work", "Persons": 1},
        "CHR27": {"Template Name": "CHR27 Family both at work, 2 children", "Persons": 4},
        "CHR28": {"Template Name": "CHR28 Single man under 30 years without work", "Persons": 1},
        "CHR29": {"Template Name": "CHR29 Single man under 30 years with work", "Persons": 1},
        "CHR30": {"Template Name": "CHR30 Single, Retired Man", "Persons": 1},
        "CHR31": {"Template Name": "CHR31 Single, Retired Woman", "Persons": 1},
        "CHR32": {"Template Name": "CHR32 Couple under 30 years without work", "Persons": 2},
        "CHR33": {"Template Name": "CHR33 Couple under 30 years with work", "Persons": 2},
        "CHR34": {"Template Name": "CHR34 Couple under 30 years, one at work, one at home", "Persons": 2},
        "CHR35": {"Template Name": "CHR35 Single woman, 30 - 64 years, with work", "Persons": 1},
        "CHR36": {"Template Name": "CHR36 Single woman, 30 - 64 years, without work", "Persons": 1},
        "CHR37": {"Template Name": "CHR37 Single man, 30 - 64 years, with work", "Persons": 1},
        "CHR38": {"Template Name": "CHR38 Single man, 30 - 64 years, without work", "Persons": 1},
        "CHR39": {"Template Name": "CHR39 Couple, 30 - 64 years, with work", "Persons": 2},
        "CHR40": {"Template Name": "CHR40 Couple, 30 - 64 years, without work", "Persons": 2},
        "CHR41": {"Template Name": "CHR41 Family with 3 children, both at work", "Persons": 5},
        "CHR42": {"Template Name": "CHR42 Single man with 2 children, with work", "Persons": 3},
        "CHR43": {"Template Name": "CHR43 Single man with 1 child, with work", "Persons": 2},
        "CHR44": {"Template Name": "CHR44 Family with 2 children, 1 at work, 1 at home", "Persons": 4},
        "CHR45": {"Template Name": "CHR45 Family with 1 child, 1 at work, 1 at home", "Persons": 3},
        "CHR46": {"Template Name": "CHR46 Single woman, 1 child, without work", "Persons": 2},
        "CHR47": {"Template Name": "CHR47 Single woman, 2 children, without work", "Persons": 3},
        "CHR48": {"Template Name": "CHR48 Family with 2 children, without work", "Persons": 4},
        "CHR49": {"Template Name": "CHR49 Family with 1 child, without work", "Persons": 3},
        "CHR50": {"Template Name": "CHR50 Single woman with 3 children, without work", "Persons": 4},
        "CHR51": {"Template Name": "CHR51 Couple over 65 years II", "Persons": 2},
        "CHR52": {"Template Name": "CHR52 Student Flatsharing", "Persons": 3},
        "CHR53": {"Template Name": "CHR53 2 Parents, 1 Working, 2 Children", "Persons": 4},
        "CHR54": {"Template Name": "CHR54 Retired Couple, no work", "Persons": 2},
        "CHR55": {"Template Name": "CHR55 Couple with work around 40", "Persons": 2},
        "CHR56": {"Template Name": "CHR56 Couple with 2 children, husband at work", "Persons": 4},
        "CHR57": {"Template Name": "CHR57 Family with 2 Children, Man at work", "Persons": 4},
        "CHR58": {"Template Name": "CHR58 Retired Couple, no work, no cooking", "Persons": 2},
        "CHR59": {"Template Name": "CHR59 Family, 3 children, parents without work", "Persons": 5},
        "CHR60": {"Template Name": "CHR60 Family, 1 toddler, one at work, one at home", "Persons": 3},
        "CHR61": {"Template Name": "CHR61 Family, 1 child, both at work, early living pattern", "Persons": 3},
        "CHS01": {"Template Name": "CHS01 Couple with 2 Children, Dad Employed", "Persons": 4},
        "CHS04": {"Template Name": "CHS04 Retired Couple, no work", "Persons": 2},
        "CHS12": {"Template Name": "CHS12 Shiftworker Couple", "Persons": 2},
        "OR01": {"Template Name": "OR01 Single Person Office", "Persons": 1},
    }

    households_1 = {
        "CHR01": {"Template Name": "CHR01 Couple both at Work", "Persons": 2},
        "CHR02": {"Template Name": "CHR02 Couple, 30 - 64 age, with work", "Persons": 2},
        "CHR03": {"Template Name": "CHR03 Family, 1 child, both at work", "Persons": 3},
        "CHR04": {"Template Name": "CHR04 Couple, 30 - 64 years, 1 at work, 1 at home", "Persons": 2},
        "CHR05": {"Template Name": "CHR05 Family, 3 children, both with work", "Persons": 5},
        "CHR06": {"Template Name": "CHR06 Jak Jobless", "Persons": 1},
        "CHR07": {"Template Name": "CHR07 Single with work", "Persons": 1},
        "CHR08": {"Template Name": "CHR08 Single woman, 2 children, with work", "Persons": 3},
        "CHR09": {"Template Name": "CHR09 Single woman, 30 - 64 years, with work", "Persons": 1},
        "CHR10": {"Template Name": "CHR10 Single man, 30 - 64 age, shift worker", "Persons": 1},
    }

    households_2 = {
        "CHR21": {"Template Name": "CHR21 Couple, 30 - 64 years, shift worker", "Persons": 2},
        "CHR22": {"Template Name": "CHR22 Single woman, 1 child, with work", "Persons": 2},
        "CHR23": {"Template Name": "CHR23 Single man over 65 years", "Persons": 1},
        "CHR24": {"Template Name": "CHR24 Single woman over 65 years", "Persons": 1},
        "CHR25": {"Template Name": "CHR25 Single woman under 30 years with work", "Persons": 1},
        "CHR26": {"Template Name": "CHR26 Single woman under 30 years without work", "Persons": 1},
        "CHR27": {"Template Name": "CHR27 Family both at work, 2 children", "Persons": 4},
        "CHR28": {"Template Name": "CHR28 Single man under 30 years without work", "Persons": 1},
        "CHR29": {"Template Name": "CHR29 Single man under 30 years with work", "Persons": 1},
        "CHR30": {"Template Name": "CHR30 Single, Retired Man", "Persons": 1},
        "CHR31": {"Template Name": "CHR31 Single, Retired Woman", "Persons": 1},
        "CHR32": {"Template Name": "CHR32 Couple under 30 years without work", "Persons": 2},
        "CHR33": {"Template Name": "CHR33 Couple under 30 years with work", "Persons": 2},
        "CHR34": {"Template Name": "CHR34 Couple under 30 years, one at work, one at home", "Persons": 2},
        "CHR35": {"Template Name": "CHR35 Single woman, 30 - 64 years, with work", "Persons": 1},
        "CHR36": {"Template Name": "CHR36 Single woman, 30 - 64 years, without work", "Persons": 1},
        "CHR37": {"Template Name": "CHR37 Single man, 30 - 64 years, with work", "Persons": 1},
        "CHR38": {"Template Name": "CHR38 Single man, 30 - 64 years, without work", "Persons": 1},
        "CHR39": {"Template Name": "CHR39 Couple, 30 - 64 years, with work", "Persons": 2},
        "CHR40": {"Template Name": "CHR40 Couple, 30 - 64 years, without work", "Persons": 2},
    }

    households_3 = {
        "CHR41": {"Template Name": "CHR41 Family with 3 children, both at work", "Persons": 5},
        "CHR42": {"Template Name": "CHR42 Single man with 2 children, with work", "Persons": 3},
        "CHR43": {"Template Name": "CHR43 Single man with 1 child, with work", "Persons": 2},
        "CHR44": {"Template Name": "CHR44 Family with 2 children, 1 at work, 1 at home", "Persons": 4},
        "CHR45": {"Template Name": "CHR45 Family with 1 child, 1 at work, 1 at home", "Persons": 3},
        "CHR46": {"Template Name": "CHR46 Single woman, 1 child, without work", "Persons": 2},
        "CHR47": {"Template Name": "CHR47 Single woman, 2 children, without work", "Persons": 3},
        "CHR48": {"Template Name": "CHR48 Family with 2 children, without work", "Persons": 4},
        "CHR49": {"Template Name": "CHR49 Family with 1 child, without work", "Persons": 3},
        "CHR50": {"Template Name": "CHR50 Single woman with 3 children, without work", "Persons": 4},
        "CHR51": {"Template Name": "CHR51 Couple over 65 years II", "Persons": 2},
        "CHR52": {"Template Name": "CHR52 Student Flatsharing", "Persons": 3},
        "CHR53": {"Template Name": "CHR53 2 Parents, 1 Working, 2 Children", "Persons": 4},
        "CHR54": {"Template Name": "CHR54 Retired Couple, no work", "Persons": 2},
        "CHR55": {"Template Name": "CHR55 Couple with work around 40", "Persons": 2},
        "CHR56": {"Template Name": "CHR56 Couple with 2 children, husband at work", "Persons": 4},
        "CHR57": {"Template Name": "CHR57 Family with 2 Children, Man at work", "Persons": 4},
        "CHR58": {"Template Name": "CHR58 Retired Couple, no work, no cooking", "Persons": 2},
        "CHR59": {"Template Name": "CHR59 Family, 3 children, parents without work", "Persons": 5},
        "CHR60": {"Template Name": "CHR60 Family, 1 toddler, one at work, one at home", "Persons": 3},
    }

    households_4 = {
        "CHR61": {"Template Name": "CHR61 Family, 1 child, both at work, early living pattern", "Persons": 3},
        "CHS01": {"Template Name": "CHS01 Couple with 2 Children, Dad Employed", "Persons": 4},
        "CHS04": {"Template Name": "CHS04 Retired Couple, no work", "Persons": 2},
        "CHS12": {"Template Name": "CHS12 Shiftworker Couple", "Persons": 2},
        "OR01": {"Template Name": "OR01 Single Person Office", "Persons": 1},
    }

    households_test = {
        "CHR03": {"Template Name": "CHR41 Family with 3 children, both at work", "Persons": 5},
        "CHR04": {"Template Name": "CHR42 Single man with 2 children, with work", "Persons": 3},
        "CHR08": {"Template Name": "CHR43 Single man with 1 child, with work", "Persons": 2},
        "CHR10": {"Template Name": "CHR44 Family with 2 children, 1 at work, 1 at home", "Persons": 4},
        "CHR13": {"Template Name": "CHR45 Family with 1 child, 1 at work, 1 at home", "Persons": 3},
        "CHR16": {"Template Name": "CHR46 Single woman, 1 child, without work", "Persons": 2},
        "CHR18": {"Template Name": "CHR47 Single woman, 2 children, without work", "Persons": 3},
        "CHR20": {"Template Name": "CHR48 Family with 2 children, without work", "Persons": 4},
        "CHR25": {"Template Name": "CHR49 Family with 1 child, without work", "Persons": 3},
        "CHR27": {"Template Name": "CHR50 Single woman with 3 children, without work", "Persons": 4},
        "CHR31": {"Template Name": "CHR51 Couple over 65 years II", "Persons": 2},
        "CHR32": {"Template Name": "CHR52 Student Flatsharing", "Persons": 3},
        "CHR39": {"Template Name": "CHR53 2 Parents, 1 Working, 2 Children", "Persons": 4},
        "CHR44": {"Template Name": "CHR54 Retired Couple, no work", "Persons": 2},
        "CHR49": {"Template Name": "CHR55 Couple with work around 40", "Persons": 2},
        "CHR50": {"Template Name": "CHR56 Couple with 2 children, husband at work", "Persons": 4},
        "CHR57": {"Template Name": "CHR57 Family with 2 Children, Man at work", "Persons": 4},
        "CHR60": {"Template Name": "CHR58 Retired Couple, no work, no cooking", "Persons": 2},
        "CHS01": {"Template Name": "CHR59 Family, 3 children, parents without work", "Persons": 5},
        "CHS12": {"Template Name": "CHR60 Family, 1 toddler, one at work, one at home", "Persons": 3},
    }

    household_1 = {
        "CHR01": {"Template Name": "CHR01 Couple both at Work", "Persons": 2},
    }

    household_2 = {
        "CHR02": {"Template Name": "CHR02 Couple, 30 - 64 age, with work", "Persons": 2}
    }

    household_3 = {
        "CHR03": {"Template Name": "CHR03 Family, 1 child, both at work", "Persons": 3}
    }
    household_4 = {
        "CHR04": {"Template Name": "CHR04 Couple, 30 - 64 years, 1 at work, 1 at home", "Persons": 2}
    }
    household_5 = {
        "CHR05": {"Template Name": "CHR05 Family, 3 children, both with work", "Persons": 5}
    }
    household_6 = {
        "CHR06": {"Template Name": "CHR06 Jak Jobless", "Persons": 1}
    }
    household_7 = {
        "CHR07": {"Template Name": "CHR07 Single with work", "Persons": 1}
    }
    household_8 = {
        "CHR08": {"Template Name": "CHR08 Single woman, 2 children, with work", "Persons": 3}
    }
    household_9 = {
        "CHR09": {"Template Name": "CHR09 Single woman, 30 - 64 years, with work", "Persons": 1}
    }
    household_10 = {
        "CHR10": {"Template Name": "CHR10 Single man, 30 - 64 age, shift worker", "Persons": 1}
    }

    households = [household_1, household_2, household_3, household_4, household_5, household_6,
                  household_7, household_8, household_9, household_10]

    # Simulationsparameter
    # for hh in household_objects:
    #     print(f"ID: {hh.UniqueHouseholdId}, Name: {hh.Name}, Template: {hh.HouseholdNameSpec.HouseholdReference}")
    years = [2019, 2020, 2021, 2022, 2023]
    ticks = [1, 2]
    for k in ticks:
        for year in years:
            for i in households:
                first_key = next(iter(i))
                household_objects = generate_households(i)
                building_id = f"ID_Single_Household_{first_key}_{year}"
                lpg_dir_path = r"C:\03_Repos\sekquasens_interfaces\submodules\pylpg"
                output_folder = rf"C:\03_Repos\SimData\pylpg\LGBM_training_single_households\Results_{first_key}_{k}"
                calc_folder = os.path.join(lpg_dir_path, 'pylpg')
                city = "Berlin, Germany"
                start_date = datetime(year, 1, 1)
                end_date = datetime(year, 12, 31)
                start_time = time.time()
                resolution = "01:00:00"

                # Get Weather Data
                csv_path = rf'C:\03_Repos\sekquasens_interfaces\sekquasens_interfaces\weather\Data\Temperature_Meteostat_Berlin_{year}.csv'  # Savepath for weather data

                # Übergebe die generierten Haushaltsobjekte an die Simulation
                data = simulate_building(
                    building_id=building_id,
                    startdate=start_date,
                    enddate=end_date,
                    output_folder=output_folder,
                    calc_folder=calc_folder,
                    resolution=resolution,
                    city=city,
                    weather_path=csv_path,
                    all_households=household_objects,
                    household_info=households_test,  # Übergebe das ursprüngliche Dictionary
                )