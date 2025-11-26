import pandas as pd
from pandarallel import pandarallel
import math
import numpy as np
pandarallel.initialize(progress_bar=True)



transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3a'", "nu3b'", "nu4a'", "nu4b'", "J'", "Ka'", "Kc'", "inv'",
                      "nu1\"", "nu2\"", "nu3a\"", "nu3b\"", "nu4a\"", "nu4b\"", "J\"", "Ka\"", "Kc\"","inv\"", "Source"]

allTransitions = pd.read_csv("ND2H-MARVEL-MAIN.txt", delim_whitespace=True, names=transitionsColumns)
# allTransitions = pd.read_csv("Transitions-H2-13C-16O.txt", delim_whitespace=True, names=transitionsColumns)
segmentsColumns = ["Segment", "Units"]
segments = pd.read_csv("segments.txt", delim_whitespace=True, names=segmentsColumns)
def splitSegment(row):
    row["Segment"] = row["Source"].split(".")[0]
    return row
allTransitions = allTransitions.parallel_apply(lambda x: splitSegment(x), result_type="expand", axis=1)
allTransitions = allTransitions.merge(segments, on="Segment")
def convertUnits(row):
    if row["Units"] == "cm-1":
        row["nu"] = row["nu"]
    else:
        row["nu"] = row["nu"]*1e6/29979245800
        row["unc1"] = row["unc1"]*1e6/29979245800
        row["unc2"] = row["unc2"]*1e6/29979245800
    return row
allTransitions = allTransitions.parallel_apply(lambda x: convertUnits(x), result_type="expand", axis=1)



def trimSourceTag(row):
    row["Source"] = row["Source"].split(".")[0]
    return row

allTransitions = allTransitions.parallel_apply(lambda x:trimSourceTag(x), result_type="expand", axis=1)

allTransitions = allTransitions.groupby(["Source"])

def findMarvelStatistics(dataFrame):
    dataFrame["MU"] = dataFrame[dataFrame["nu"] >= 0]["unc1"].median()
    dataFrame["AU"] = dataFrame[dataFrame["nu"] >= 0]["unc1"].mean()
    dataFrame["nuMax"] = abs(dataFrame["nu"]).max()
    dataFrame["nuMin"] = abs(dataFrame["nu"]).min()
    numberOfValidatedLines = len(dataFrame[dataFrame["nu"] >= 0])
    dataFrame["Validated"] = numberOfValidatedLines
    totalLines = len(dataFrame)
    dataFrame["TotalLines"] = totalLines
    return dataFrame

allTransitions = allTransitions.parallel_apply(lambda x:findMarvelStatistics(x))
allTransitions = allTransitions[["Source", "nuMin", "nuMax", "Validated", "TotalLines", "AU", "MU"]]
allTransitions = allTransitions.drop_duplicates()
allTransitions = allTransitions.sort_values(by="MU")
print("\n")
print("Displaying current table\n")
print("\n")
print(allTransitions.to_string(index=False))
# allTransitions = allTransitions.reset_index()
# print(allTransitions["AU"]["09CaDoPu"])

# latexString = "\\begin{table}"
# latexString += "\n"
latexString = "\\begin{longtable}{c c c c c c}"
latexString += "\n"
latexString += "\\caption{}"
latexString += "\n"
latexString += "\\endhead"
latexString += "\n"
latexString += "\\hline"
latexString += "\n"
latexString += "Segment & Source & Range (cm$^{-1}$) & V/T & AU & MU \\\\"
latexString += "\n"
latexString += "\\hline"

for source in allTransitions["Source"]:
    minimumFrequency = allTransitions["nuMin"][source].squeeze()
    maximumFrequency = allTransitions["nuMax"][source].squeeze()
    validatedTransitions = allTransitions["Validated"][source].squeeze()
    totalTransitions = allTransitions["TotalLines"][source].squeeze()
    averageUncertainty = "{0:1.1e}".format(round(allTransitions["AU"][source].squeeze(), 4-math.ceil(np.log10(allTransitions["AU"][source].squeeze()))))
    medianUncertainty = "{0:1.1e}".format(round(allTransitions["MU"][source].squeeze(), 4-math.ceil(np.log10(allTransitions["MU"][source].squeeze()))))
    latexString += "\n"
    latexString += f"{source} &"
    latexString += "\cite{" + source + ".NH2D} & "
    # latexString += f" {minimumFrequency} & {maximumFrequency} &"
    latexString += "{0:.6f}".format(minimumFrequency)
    latexString += " & "
    latexString += "{0:.6f}".format(maximumFrequency)
    latexString += " & "
    latexString += f" {validatedTransitions}/{totalTransitions} &"
    latexString += f" {averageUncertainty} & {medianUncertainty}"
    latexString += " \\\\"

latexString += "\n"
latexString += "\\hline"
latexString += "\n"
latexString += "\\end{longtable}"
# latexString += "\n"
# latexString += "\\caption{}"
# latexString += "\n"
# latexString += "\\end{table}"

citeAllString = "\cite{"

for source in allTransitions["Source"]:
    citeAllString += source
    citeAllString += ","
citeAllString += "}"
tableFile = "MarvelTable.tex"
with open(tableFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(latexString)

citeFile = "cite.tex"
with open(citeFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(citeAllString)