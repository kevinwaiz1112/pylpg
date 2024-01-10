"""
Minimalistic example for using the pylpg package
"""
from pylpg import lpg_execution, lpgdata, lpgpythonbindings
from pylpg.lpgdata import *
from pylpg.lpgpythonbindings import *
from pylpg.lpg_execution import *
import utils
import random
import pandas
import visualizer
import time

# Simulate the predefined household CHR01 (couple, both employed) for the year 2022

data = lpg_execution.execute_lpg_with_many_householdata(
    2022,

    lpgdata.HouseTypes.HT22_Big_Multifamily_House_no_heating_cooling,

)

# Extract the generated electricity load profile
print(data)
