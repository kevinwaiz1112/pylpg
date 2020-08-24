import pylpg
import pandas  # type: ignore
import random
from lpgdata import *

def test_tsib_function() -> None:
    random.seed(2)
    df: pandas.DataFrame = pylpg.excute_lpg_tsib(year=2020, number_of_households=2, number_of_people_per_household=2,
                                                 startdate="01.01.2020", enddate="01.03.2020", transportation=True)
    df.to_csv(r'lpgexport.csv', index=True, sep=";")
    print("successfully exportet dataframe to lpgexport.csv")

def test_single_household_function() -> None:
    random.seed(2)
    hhnamespec = pylpg.HouseholdNameSpecification(Households.CHR04_Couple_30_64_years_1_at_work_1_at_home)
    df: pandas.DataFrame = pylpg.excute_lpg_single_household(year=2020, householdref=hhnamespec,
                                                             housetype=HouseTypes.HT02_House_with_a_5_kWh_Battery_and_a_50_m2_Photovolatic_Array_5MWh_space_heating_gas_heating,
                                                             startdate="01.01.2020", enddate="01.03.2020",
                                                             simulate_transportation=True,chargingset=ChargingStationSets.Charging_At_Home_with_11_kW,
                                                             transportation_device_set=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
                                                             travel_route_set=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance
                                                             )
    df.to_csv(r'lpgexport.csv', index=True, sep=";")
    print("successfully exportet dataframe to lpgexport.csv")

def test_householdata_function() -> None:
    random.seed(2)
    p1 = PersonData(24,"male")
    p2 = PersonData(24, "female")
    personspec = HouseholdDataPersonSpecification([p1,p2])
    hhdata1 = HouseholdData(UniqueHouseholdId="uniq",Name="name",
                            HouseholdDataPersonSpec=personspec,
                            HouseholdDataSpecification=HouseholdDataSpecificationType.ByPersons,
                            TransportationDeviceSet=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
                            ChargingStationSet=ChargingStationSets.Charging_At_Home_with_11_kW,
                            TravelRouteSet=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance                            )
    df: pandas.DataFrame = pylpg.excute_lpg_with_householdata(year=2020, householddata=hhdata1,
                                                             housetype=HouseTypes.HT23_No_Infrastructure_at_all,
                                                             startdate="01.01.2020", enddate="01.03.2020",
                                                             simulate_transportation=True)
    df.to_csv(r'lpgexport.csv', index=True, sep=";")
    print("successfully exportet dataframe to lpgexport.csv")

def test_grid_profiles_function() -> None:
    random.seed(2)
    household_sizes = [2]

    df: pandas.DataFrame = pylpg.execute_grid_calc(year=2020, household_size_list=household_sizes,
                                                             housetype=HouseTypes.HT21_Normal_House_with_15_000_kWh_Heat_Demand_Heat_Pump_with_COP3_and_Hot_Water_Storage_Tank_Heat_Pump_Electricity,
                                                             startdate="01.01.2020", enddate="12.31.2020",
                                                             simulate_transportation=True,chargingset=ChargingStationSets.Charging_At_Home_with_11_kW,
                                                             transportation_device_set=TransportationDeviceSets.Bus_and_two_30_km_h_Cars,
                                                             travel_route_set=TravelRouteSets.Travel_Route_Set_for_15km_Commuting_Distance
                                                             )
    df.to_csv(r'lpgexport_grid.csv', index=True, sep=";")
    sumcol = df.sum(axis=0)
    print(sumcol)
    print("successfully exportet dataframe to lpgexport.csv")

def test_family_maker() -> None:
    persons1:List[PersonData] = pylpg.makeReasonableFamily(1)
    printPersonsList(persons1)
    persons2: List[PersonData] = pylpg.makeReasonableFamily(2)
    printPersonsList(persons2)
    persons3: List[PersonData] = pylpg.makeReasonableFamily(3)
    printPersonsList(persons3)
    persons4: List[PersonData] = pylpg.makeReasonableFamily(4)
    printPersonsList(persons4)
    persons5: List[PersonData] = pylpg.makeReasonableFamily(5)
    printPersonsList(persons5)

def printPersonsList(persons: List[PersonData]):
    print("#############")
    print("Persons: " + str(len(persons)))
    for p in persons:
        print(personDataToString(p))

def personDataToString(pd: PersonData) -> str:
    return str(str(pd.Age) + " - " + pd.Gender)

if __name__ == "__main__":
    test_householdata_function()
    #test_single_household_function()
    
