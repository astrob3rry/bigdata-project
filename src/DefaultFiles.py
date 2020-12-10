import pandas as pd
import sys, os

class DefaultFiles:
    def __init__(self, inputPath):
        self.dfCountryNames = pd.read_csv(os.path.join(inputPath, "countryNames.csv"), encoding = "utf-8-sig", engine = "python")
        self.dfStateNames = pd.read_csv(os.path.join(inputPath, "stateNames.csv"))
        self.dfCityNames = pd.read_csv(os.path.join(inputPath, "cityNames.csv"))
        self.dfCountyNames = pd.read_csv(os.path.join(inputPath, "countyNames.csv"))
        self.dfNames = pd.read_csv(os.path.join(inputPath, "metadata.csv"))