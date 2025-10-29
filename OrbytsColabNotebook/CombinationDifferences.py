import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

# marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies = pd.read_csv("22CaDiFuTa-EmpiricalLevels.txt", delim_whitespace=True)

def assignMarvelTags(row):
    row["Tag\""] = (str(row["nu1"]) + "-" + str(row["nu2"]) + "-" + str(row["nu3a"]) + "-" + str(row["nu3b"]) 
                  + "-" + str(row["nu4a"]) + "-" + str(row["nu4b"]) + "-" + str(row["J"]) + "-" + str(row["Ka"]) 
                  + "-" + str(row["Kc"]) + "-" + str(row["inv"]))
    return row

marvelEnergies = marvelEnergies.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)

print(marvelEnergies.head(20).to_string(index=False))


transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3a'", "nu3b'", "nu4a'", "nu4b'", "J'", "Ka'", "Kc'", "inv'",
                      "nu1\"", "nu2\"", "nu3a\"", "nu3b\"", "nu4a\"", "nu4b\"", "J\"", "Ka\"", "Kc\"","inv\"", "Source"]

# allTransitions = pd.read_csv("../Marvel-14NH3-2020.txt", delim_whitespace=True, names=transitionsColumns)

transitionsFiles = [
    "../completed_extractions/51WeSt/51WeSt-MARVEL.txt",
    "../completed_extractions/64LiGaDe/64LiGaDe-MARVEL.txt",
    "../completed_extractions/75DeHe/75DeHe-MARVEL.txt",
    "../completed_extractions/86CoVaHe/86CoVaHe-MARVEL.txt",
    "../completed_extractions/88FuDiJoHa/88FuDiJoHa-MARVEL.txt",
    "../completed_extractions/88KaSiJoKa/88KaSiJoKa-MARVEL.txt",
    "../completed_extractions/03SnHoQu/03SnHoQu-MARVEL.txt",
    "../completed_extractions/06SnHoQu/06SnHoQu-MARVEL.txt",
    "../completed_extractions/06EnMuBrPa/06EnMuBrPa-MARVEL.txt",
    "../completed_extractions/21MeBiDoKi/21MeBiDoKi-MARVEL.txt",
    "../completed_extractions/22CaDiFuTa/22CaDiFuTa-MARVEL.txt",
]

print("\n")
print("Reading transition files...")
allTransitions = []
for transitionFile in transitionsFiles:
    if len(allTransitions) < 1:
        allTransitions = pd.read_csv(transitionFile, delim_whitespace=True, names=transitionsColumns)
    else:
        transitionsToAdd = pd.read_csv(transitionFile, delim_whitespace=True, names=transitionsColumns)
        allTransitions = pd.concat([allTransitions, transitionsToAdd])

print("\n")
print("Done!")
allTransitions = allTransitions[allTransitions["nu"].duplicated() == False]
def removeTransitions(row, transitionsToRemove, transitionsToCorrect, transitionsToReassign, badLines, uncertaintyScaleFactor=1e1, repeatTolerance=3, maximumUncertainty=0.1):
    if row["Source"] in transitionsToRemove:
        row["nu"] = -row["nu"]
    if row["Source"] in transitionsToCorrect.keys():
        row["nu"] = transitionsToCorrect[row["Source"]]
    if row["Source"] in transitionsToReassign.keys():
        numberOfQuantumNumbers = int((len(row) - 4)/2)
        reassignment = transitionsToReassign[row["Source"]]
        upperStateReassignment = reassignment[0]
        lowerStateReassignment = reassignment[1]
        columnLabels = row.index.tolist()
        if upperStateReassignment != None:
            newUpperStateLabels = upperStateReassignment.split("-")
            for i in range(3, 3 + numberOfQuantumNumbers):
                row[columnLabels[i]] = newUpperStateLabels[i - 3]
        if lowerStateReassignment != None:
            newLowerStateLabels = lowerStateReassignment.split("-")
            for i in range(3 + numberOfQuantumNumbers, 3 + 2*numberOfQuantumNumbers):
                row[columnLabels[i]] = newLowerStateLabels[i - (3 + numberOfQuantumNumbers)]
    if row["Source"] in badLines["Line"].tolist():
        matchingBadLines = badLines[badLines["Line"] == row["Source"]]
        badLine = matchingBadLines.tail(1).squeeze()
        row["unc1"] = uncertaintyScaleFactor*badLine["Uncertainty"]
        row["unc2"] = uncertaintyScaleFactor*badLine["Uncertainty"]
        # if len(matchingBadLines) < repeatTolerance:
        #     row["unc1"] = badLine["Uncertainty'"]
        #     row["unc2"] = badLine["Uncertainty'"]
        # else:
            # if badLine["Ratio"] > uncertaintyScaleFactor:
            #     row["unc1"] = badLine["Uncertainty"]*badLine["Ratio"]
            #     row["unc2"] = badLine["Uncertainty"]*badLine["Ratio"]
            # else:
            #     row["unc1"] = uncertaintyScaleFactor*badLine["Uncertainty"]
            #     row["unc2"] = uncertaintyScaleFactor*badLine["Uncertainty"]
    # if row["unc1"] >= maximumUncertainty:
    #     # Allow transitions above 10000 cm-1 to have a larger uncertainty
    #     if row["nu"] > 0 and row["nu"] < 10000:
    #         row["nu"] = -row["nu"]
    return row

# List of transitions to be invalidated
transitionsToRemove = [
    "75DeHe.73",
    "75DeHe.77",
    "75DeHe.67",
    "75DeHe.47",
    "88FuDiJoHa.797",
    "21MeBiDoKi.38",
    "21MeBiDoKi.36",
    "21MeBiDoKi.39",
    "21MeBiDoKi.35",
    "21MeBiDoKi.23",
    "21MeBiDoKi.33",
    "21MeBiDoKi.30",
    "21MeBiDoKi.32",
    "21MeBiDoKi.29",
    "21MeBiDoKi.34",
    "21MeBiDoKi.25",
    "21MeBiDoKi.27",
    "21MeBiDoKi.31",
    "21MeBiDoKi.28",
    "21MeBiDoKi.37",
    "21MeBiDoKi.19",
    "21MeBiDoKi.26",
    "21MeBiDoKi.24",

]

transitionsToCorrect = {
    # Typos spotted from scan
    "88FuDiJoHa.342": 122.897560,
    "88FuDiJoHa.410": 143.787750,
    "88FuDiJoHa.411": 143.787750,
    "88FuDiJoHa.412": 143.983490,
    "88FuDiJoHa.386": 134.824900,
    "88FuDiJoHa.385": 134.824900,
    "88FuDiJoHa.234": 86.242410,
    "88FuDiJoHa.370": 132.683190,
    "88FuDiJoHa.389": 136.468660,
    "88FuDiJoHa.445": 151.829840,
    "88FuDiJoHa.477": 158.686810,
    "88FuDiJoHa.449": 152.289350,
    "88FuDiJoHa.528": 170.877570,
    "88FuDiJoHa.515": 168.097690,
    "88FuDiJoHa.496": 164.855240,
    "88FuDiJoHa.439": 148.352880,
    "88FuDiJoHa.756": 0.9527156,
    "88FuDiJoHa.769": 2.3209923,
    "88FuDiJoHa.818": 12.787276,
    "86CoVaHe.281": 808.31255,
    "88FuDiJoHa.214": 80.666410,
    "88FuDiJoHa.216": 80.861190,
    "88FuDiJoHa.274": 100.835910,
    "88FuDiJoHa.273": 100.835910,
    "88FuDiJoHa.294": 109.825480,
    "88FuDiJoHa.319": 118.979720,
    "88KaSiJoKa.2": 869.41080,
}

# Transitions to reassign in format (Source Tag: [New Upper State Tag, New Lower State Tag])
# Reassignments marked with a # are considered potentially dubious
transitionsToReassign = {
    # "06SnHoQu.430" : ["0-0-0-0-1-0-10-1-9-s", None],
    # "06SnHoQu.411" : ["0-0-0-0-1-0-9-1-9-s", None],
    # "06SnHoQu.108" : ["0-0-0-0-0-1-2-1-1-a", None],
    "06SnHoQu.232" : ["0-0-0-0-0-1-5-4-2-s", None],
    "06SnHoQu.233" : ["0-0-0-0-0-1-5-4-2-s", None],
    "88FuDiJoHa.409" : ["0-0-0-0-0-0-11-8-4-a", None],
    "88FuDiJoHa.752" : ["0-0-0-0-0-0-16-6-11-a", "0-0-0-0-0-0-16-5-11-s"],
    "86CoVaHe.57" : [None, "0-0-0-0-0-0-4-4-0-s"],
    "86CoVaHe.161" : [None, "0-0-0-0-0-0-4-4-0-s"],
    "86CoVaHe.36": [None, "0-0-0-0-0-0-6-2-4-s"],
    "86CoVaHe.158": [None, "0-0-0-0-0-0-5-3-3-s"],
    "88KaSiJoKa.29": [None, "0-0-0-0-0-0-6-6-1-s"],
    "86CoVaHe.793": [None, "0-0-0-0-0-0-7-2-6-s"],
    "86CoVaHe.599": ["0-1-0-0-0-0-9-6-4-a", None],
    "88KaSiJoKa.79": [None, "0-0-0-0-0-0-10-7-4-a"],
    "88KaSiJoKa.80": [None, "0-0-0-0-0-0-10-7-3-s"],
    "88KaSiJoKa.66": [None, "0-0-0-0-0-0-10-7-4-a"],
    "88KaSiJoKa.67": [None, "0-0-0-0-0-0-10-7-3-s"],
}

badLines = pd.read_csv("BadLines.txt", delim_whitespace=True)
badLines = badLines[badLines["Line"].duplicated(keep="last") == False]

print("\n")
print("Removing/Reassinging transitions...")
allTransitions = allTransitions.parallel_apply(lambda x:removeTransitions(x, transitionsToRemove, transitionsToCorrect, transitionsToReassign, badLines), axis=1, result_type="expand")
print("\n")
print("Done!")
# Filtering
Jupper = 16
transitions = allTransitions[allTransitions["nu"] > 0]
# transitions = transitions[transitions["J'"] == Jupper]
print(transitions.head(20).to_string(index=False))

def assignStateTags(row):
    row["Tag'"] = (str(row["nu1'"]) + "-" + str(row["nu2'"]) + "-" + str(row["nu3a'"]) + "-" + str(row["nu3b'"])  
                   + "-" + str(row["nu4a'"]) + "-" + str(row["nu4b'"]) + "-" + str(row["J'"]) + "-" + str(row["Ka'"]) + "-" + str(row["Kc'"]) + "-" + str(row["inv'"]))
    row["Tag\""] =  (str(row["nu1\""]) + "-" + str(row["nu2\""]) + "-" + str(row["nu3a\""]) + "-" + str(row["nu3b\""])  
                   + "-" + str(row["nu4a\""]) + "-" + str(row["nu4b\""]) + "-" + str(row["J\""]) + "-" + str(row["Ka\""]) + "-" + str(row["Kc\""]) + "-" + str(row["inv\""]))
    return row

transitions = transitions.parallel_apply(lambda x:assignStateTags(x), result_type="expand", axis=1)
print("\n")
print("Converting Units...")
segmentsColumns = ["Segment", "Units"]
segments = pd.read_csv("segments.txt", delim_whitespace=True, names=segmentsColumns)
def splitSegment(row):
    row["Segment"] = row["Source"].split(".")[0]
    return row
transitions = transitions.parallel_apply(lambda x: splitSegment(x), result_type="expand", axis=1)
transitions = transitions.merge(segments, on="Segment")
def convertUnits(row):
    if row["Units"] == "cm-1":
        row["nuCM"] = row["nu"]
    else:
        row["nuCM"] = row["nu"]*1e6/29979245800
    return row
transitions = transitions.parallel_apply(lambda x: convertUnits(x), result_type="expand", axis=1)
print("Done!")
print("\n")
print("Applying Combination Differences...")
transitions = transitions.merge(marvelEnergies, on="Tag\"")
transitions["E'"] = transitions["E\""] + transitions["nuCM"]
# print(transitions.head(20).to_string(index=False))
transitions = transitions[transitions["E\""] >= 0]
transitions = transitions.sort_values(by=["J'", "E'"])

transitionsGroupedByUpperState = transitions.groupby(["Tag'"])
def applyCombinationDifferences(transitionsToUpperState, threshold=0.1):
    transitionsToUpperState["Average E'"] = transitionsToUpperState["E'"].mean()
    transitionsToUpperState["Problem"] = abs(transitionsToUpperState["E'"] - transitionsToUpperState["Average E'"]) > threshold
    # If a problematic transition exists we mark all transitions to this upper state as those we wish to return later
    transitionsToUpperState["Return"] = False
    transitionsToUpperState["Return"] = transitionsToUpperState["Problem"].any()
    return transitionsToUpperState

# Tolerance for the combination difference test - adjust accordingly
threshold = 0.05 # cm-1
transitions = transitionsGroupedByUpperState.parallel_apply(lambda x:applyCombinationDifferences(x, threshold))
returnedTransitions = transitions[transitions["Return"]]
print("\n")
print("Done!")

print("\n Returned combination differences:")
print(returnedTransitions[["nu", "unc1", "nuCM", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

transitionsByUpperStateEnergy = transitions.sort_values(by=["E'"])
targetUpperState = 7075.641107
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[transitionsByUpperStateEnergy["E'"] > targetUpperState - 1]
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[targetUpperState + 1 > transitionsByUpperStateEnergy["E'"]]
# print(f"\n Returned upper state energies centred on {targetUpperState}: ")
# print(transitionsByUpperStateEnergy[["nu", "unc1", "nuCM", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

# For checking if transitions obey symmetry selection rules
runSelectionRulesCheck = False
if runSelectionRulesCheck:
    selectionRules = {
        "A1'": "A1\"", # Technically the nuclear spin statistical weights of the A1 states are zero
        "A1\"": "A1'",
        "A2'": "A2\"",
        "A2\"": "A2'",
        "E'": "E\"",
        "E\"": "E'",
    }

    def selectionRulesCheck(row, selectionRules):
        row["SR Broken"] = False 
        if "MAGIC" not in row["Source"]:
            if row["Gamma\""] != selectionRules[row["Gamma'"]]:
                row["SR Broken"] = True
        return row
    
    transitions = transitions.parallel_apply(lambda x:selectionRulesCheck(x, selectionRules), axis=1, result_type="expand")
    transitionsThatBreakSelectionRules = transitions[transitions["SR Broken"]]
    print("\n Printing transitions which violate selection rules: ")
    print(transitionsThatBreakSelectionRules[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

# For when matching to states file is needed
readFromStatesFile = False
if readFromStatesFile:
    statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
    states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
    states = states[states["E"] < 7000]
    states = states[states["g"] > 0]
    states = states[states["J"] == Jupper]
    states = states[states["E"] > 6500]
    print(states.to_string(index=False))

    statesList = [
        "21CaCeBeCa.1673",
        "21CaCeBeCa.1674"
    ]

    def findMatchingStates(row, states):
        matchingStates = states[states["J"] == row["J'"]]
        matchingStates = matchingStates[matchingStates["Gamma"] == row["Gamma'"]]
        matchingStates = matchingStates[matchingStates["Nb"] == row["Nb'"]]
        row["CoYuTe E'"] = matchingStates.squeeze()["E"]
        return row
    
    transitions = transitions.parallel_apply(lambda x:findMatchingStates(x, states), axis=1, result_type="expand")
    statesFromList = transitions[transitions["Source"].isin(statesList)]
    print("Selected states with CoYuTe upper state energy:")
    print(statesFromList[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "CoYuTe E'", "E\"", "Problem"]].to_string(index=False))

allTransitions = allTransitions.parallel_apply(lambda x: splitSegment(x), result_type="expand", axis=1)
allTransitions = allTransitions.merge(segments, on="Segment")
allTransitions = allTransitions.parallel_apply(lambda x: convertUnits(x), result_type="expand", axis=1)
allTransitions = allTransitions[round(allTransitions["nuCM"], 6).duplicated() == False]
# print(transitions.to_string(index=False))
allTransitions = allTransitions.sort_values(by=["nuCM"])
allTransitions = allTransitions[transitionsColumns]
allTransitions = allTransitions.to_string(index=False, header=False)
marvelFile = "../ND2H-MARVEL-MAIN.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(allTransitions)