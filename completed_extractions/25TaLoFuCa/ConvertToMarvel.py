import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

columns = ["nu", "unc", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "nu5'", "nu6'", "J'", "Ka'", "Kc'",
           "inv'", "nu1\"", "nu2\"", "nu3\"", "nu4\"", "nu5\"", "nu6\"", "J\"", "Ka\"", "Kc\"",
           "inv\""]
# df1 = pd.read_csv("25TaLoFuCa-v2.txt", delim_whitespace=True)
# df2 = pd.read_csv("25TaLoFuCa-v4a.txt", delim_whitespace=True)

df = pd.read_csv("25TaLoFuCa.txt", delim_whitespace=True, names=columns)

# df["unc"] = df["unc"]*1e6/29979245800 # Convert uncertainty to wavenumber
# df["unc2"] = df["unc"]

# inversionMapping = {
#     0: "s",
#     1: "a"
# }

# df["inv'"] = df["inv'"].map(inversionMapping)
# df["inv\""] = df["inv\""].map(inversionMapping)

# for i in range(6):
#     df[f"n{i + 1}'"] = 0
#     df[f"n{i + 1}\""] = 0

df["Source"] = [f"25TaLoFuCa.{i + 1}" for i in range(len(df))] 
df = df[["nu", "unc", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "nu5'", "nu6'", "J'", "Ka'", "Kc'",
           "inv'", "nu1\"", "nu2\"", "nu3\"", "nu4\"", "nu5\"", "nu6\"", "J\"", "Ka\"", "Kc\"",
           "inv\"", "Source"]]

df = df.sort_values(by=["nu"])
df = df.to_string(header=False, index=False)
marvelFile = "25TaLoFuCa-MARVEL.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)