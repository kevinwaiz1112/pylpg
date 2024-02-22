import json

# Pfad zur JSON-Datei
json_dateipfad = r'C:\\03_Repos\\sekquasens_interfaces\\Data\\presence_times_person.geojson'

# Lese die JSON-Datei
with open(json_dateipfad, 'r', encoding='utf-8') as datei:
    daten = json.load(datei)

# Ein Set, um alle einzigartigen 'status' Werte zu sammeln
status_typen = set()

# Durchlaufe alle Features im JSON
for feature in daten['features']:
    households = feature['properties']['household']
    for household in households:
        for person in household['persons']:
            # Füge den 'status' Wert zum Set hinzu
            status_typen.add(person['status'])

# Ausgabe der verschiedenen 'status' Werte und ihrer Anzahl
print(f"Anzahl unterschiedlicher 'status' Einträge: {len(status_typen)}")
print("Namen der unterschiedlichen 'status' Einträge:")
for status in status_typen:
    print(status)


import json

# Pfad zur JSON-Datei
json_dateipfad = 'C:\\03_Repos\\sekquasens_interfaces\\Data\\presence_times_person.geojson'

# Lese die JSON-Datei
with open(json_dateipfad, 'r', encoding='utf-8') as datei:
    daten = json.load(datei)

# Sets, um alle einzigartigen IDs zu sammeln
person_ids = set()
household_ids = set()
gebaeude_ids = set()

# Durchlaufe alle Features im JSON
for feature in daten['features']:
    # Füge die Gebäude-ID zum Set hinzu
    gebaeude_ids.add(feature['id'])
    households = feature['properties']['household']
    for household in households:
        # Füge die Haushalts-ID zum Set hinzu
        household_ids.add(household['household_id'])
        for person in household['persons']:
            # Füge die Personen-ID zum Set hinzu
            person_ids.add(person['person_id'])

# Ausgabe der Gesamtanzahlen
print(f"Gesamtanzahl unterschiedlicher Personen: {len(person_ids)}")
print(f"Gesamtanzahl unterschiedlicher Haushalte: {len(household_ids)}")
print(f"Gesamtanzahl unterschiedlicher Gebäude: {len(gebaeude_ids)}")

