"""
Minimalistic example for using the pylpg package
"""
from pylpg import lpg_execution, lpgdata, lpgpythonbindings
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
import utils
import random
import pandas
import visualizer
import time

# Simulate the predefined household CHR01 (couple, both employed) for the year 2022
startdate = "2023.01.01"
enddate = "2023.01.05"

"""""
p1 = PersonLivingTag(
    LivingPatternTag=LivingPatternTags.Living_Pattern_Work_From_Home_Full_Time_5_days,
    PersonName=TemplatePersons.CHR01_1_25M.PersonName,
)
p2 = PersonLivingTag(
    LivingPatternTag=LivingPatternTags.Living_Pattern_Work_From_Home_Full_Time_5_days,
    PersonName=TemplatePersons.CHR01_0_23F.PersonName,
)
personspec = HouseholdTemplateSpecification(
    [p1, p2],
    HouseholdTemplateName=HouseholdTemplates.CHR01_Couple_both_at_Work,
    ForbiddenTraitTags=[TraitTags.Food_Brunching],
)
hhdata1 = HouseholdData(
    UniqueHouseholdId="uniq",
    Name="name",
    HouseholdTemplateSpec=personspec,
    HouseholdDataSpecification=HouseholdDataSpecificationType.ByTemplateName,
    TransportationDeviceSet=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
    ChargingStationSet=ChargingStationSets.Charging_At_Home_with_11_kW,
    TravelRouteSet=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance,
)

data = lpg_execution.execute_lpg_with_householdata_with_csv_save(
    2023,
    hhdata1,
    housetype,
    startdate=startdate,
    enddate=enddate,
)

"""""

"""""
data = lpg_execution.execute_lpg_single_household(
    2023,
    lpgdata.Households.CHR01_Couple_both_at_Work,
    lpgdata.HouseTypes.HT20_Single_Family_House_no_heating_cooling,
    #lpgdata.HouseTypes.HT15_Normal_house_with_5_000_kWh_Space_heating_Continuous_Flow_Gas_Heater,
    startdate=startdate,
    enddate=enddate,
)

# Extract the generated electricity load profile
electricity_profile = data["Electricity_HH1"]
print(electricity_profile)

# Resample to 15 minute resolution
profile = electricity_profile.resample("30min").sum()

# Show a carpet plot
utils.carpet_plot(profile)
time = time.strftime("%Y-%m-%d_%H-%M-%S")
filename=r"C:\03_Repos\pylpg\Results\Results_"+str(time)
visualizer.plotdf(profile, filename=filename)

"""""
"""""
Filtert die Templates auf Basis der angegebenen Anzahl an Personen und dem Gender und wählt auf dieser Basis das Template aus.
Wird kein Template gefunden, so wird ein zufälliges passendes gewählt

Für die Filterung die LivingPatternTags nutzen mit: 
self.LivingPatternTag = value ; analog zu Alter und Gender für PersonData 

"""""
random.seed(2)
p1 = PersonData(35, "male")
p2 = PersonData(31, "female")
personspec = HouseholdDataPersonSpecification([p1, p2])
hhdata1 = HouseholdData(
    UniqueHouseholdId="uniq",
    Name="name",
    HouseholdDataPersonSpec=personspec,
    HouseholdDataSpecification=HouseholdDataSpecificationType.ByPersons,
    # TransportationDeviceSet=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
    # ChargingStationSet=ChargingStationSets.Charging_At_Home_with_11_kW,
    # TravelRouteSet=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance,
)

p3 = PersonData(65, "male")
p4 = PersonData(43, "female")
personspec = HouseholdDataPersonSpecification([p3, p4])
hhdata2 = HouseholdData(
    UniqueHouseholdId="uniq",
    Name="name",
    HouseholdDataPersonSpec=personspec,
    HouseholdDataSpecification=HouseholdDataSpecificationType.ByPersons,
    # TransportationDeviceSet=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
    # ChargingStationSet=ChargingStationSets.Charging_At_Home_with_11_kW,
    # TravelRouteSet=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance,
)

df: pandas.DataFrame = lpg_execution.execute_lpg_with_many_householdata(
    year=2020,
    householddata=[hhdata1, hhdata2],
    housetype=HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,   # HT20 für SFH
    startdate="01.01.2020",
    enddate="01.01.2020",
    simulate_transportation=False,
)
df.to_csv(r"lpgexport.csv", index=True, sep=";")
print("successfully exportet dataframe to lpgexport.csv")

#TODO: Die lpgdata.py file nutzen, um mit den Bewohnern (zunächst mit zufälligen) die Stromlastprofile für einen Tag zu erstellen
# Also ohne das Haus, oder wenn nur mit marginalem Input - ist das Haus extrahierbar ?
# Ziel: Ein Haus mit 50 Bewohnern und dem zugehörigen Stromlastprofil;
# Oder die Profile aller Personen eines Gebäudes einzeln und dann aufsummieren
# Für das Plotting eventuell die Grafiken aus CREST nutzen 