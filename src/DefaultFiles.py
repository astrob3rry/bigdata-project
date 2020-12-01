import pandas as pd
import sys, os

class DefaultFiles:
    def __init__(self, inputPath):
        self.dfCountryNames = pd.read_excel(os.path.join(inputPath, "countryNames.xlsx"))
        self.dfStateNames = pd.read_excel(os.path.join(inputPath, "stateNames.xlsx"))
        self.dfCityNames = pd.read_excel(os.path.join(inputPath, "cityNames.xlsx"))
        self.dfCountyNames = pd.read_excel(os.path.join(inputPath, "countyNames.xlsx"))
        self.dfNames = pd.read_excel(os.path.join(inputPath, "dataframeNames.xlsx"))