import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
from decimal import Decimal, getcontext

getcontext().prec = 20
pd.set_option('display.float_format', '{:.14f}'.format)

fp = r"nd2h.lin"

# with open(fp, 'r') as file:
#     # Read the content of the file
#     file_content = file.read()
#     # Print the content
#     #print(file_content)

# lines = file_content.split('\n')

# # Filter out any lines that start with '!'
# filtered_lines = [line for line in lines if not line.startswith('!')]
# filtered_lines = [line for line in filtered_lines if line!='']


# data_rows = [line.split() for line in filtered_lines]
   
column_names = ["units", "J'", "Ka'", "Kc'", "inv'", "hyperfine1'", "hyperfine2'", "J\"", "Ka\"", "Kc\"", "inv\"", "hyperfine1\"", "hyperfine2\"", "nu", "unc"]

df = pd.read_csv(r"nd2h.lin", names=column_names, delim_whitespace=True)

def convertUnits(row):
    if row["units"] == "MHz":
        row["nu"] *= 0.0000334
        row["unc"] *= 0.0000334
    return row

df = df.parallel_apply(lambda x:convertUnits(x), axis=1, result_type="expand")

# df = pd.DataFrame(data_rows, columns=column_names)
df = df.drop_duplicates()
df["temp_label"] = [f"temp.{i + 1}" for i in range(len(df))] 
print(len(df))


df = df.drop(["hyperfine1'", "hyperfine2'", "hyperfine1\"", "hyperfine2\""], axis=1)

#df_str = df_no_hf.to_string(index=False)

#marvelFile = r"c:\Users\jazzo\Desktop\academics\work\ORBYTS\data\21MeBiDoKi\df_removed.txt"
#with open(marvelFile, "w+") as FileToWriteTo:
#    FileToWriteTo.write(df_str)

# df_output = pd.DataFrame(columns=["nu", "unc1", "unc2", "n1'", "n2'", "n3'", "n4'", "n5'", "n6'", "J'", "Ka'", "Kc'", "inv'", "n1\"", "n2\"", "n3\"", "n4\"", "n5\"", "n6\"", "J\"", "Ka\"", "Kc\"", "inv\"", "Source"])


# inversionMapping = {
#     1: "a",
#     0: "s",
#     3: "a",
#     2: "s"
# }


for label in ["n1'", "n2'", "n3'", "n4'", "n5'", "n6'", "n1\"", "n2\"", "n3\"", "n4\"", "n5\"", "n6\""]:
    df[label] = 0


# for label in ["J'", "Ka'", "Kc'", "inv'", "J\"", "Ka\"", "Kc\"", "inv\"","nu"]:
#     df_output[label] = df_no_hf[label]

df["unc1"] = df["unc"]
df["unc2"] = df["unc"]

inversionMapping = {
    1: "a",
    0: "s",
    3: "a",
    2: "s",
    5: "a",
    4: "s"
}
df["inv'"] = df["inv'"].map(inversionMapping)
df["inv\""] = df["inv\""].map(inversionMapping)

df = df.drop_duplicates()


df["Source"] = [f"21MeBiDoKi.{i + 1}" for i in range(len(df))] 


df = df.groupby(["J'", "Ka'", "Kc'", "inv'", "J\"", "Ka\"", "Kc\"", "inv\""])

def reduceHyperfine(dataFrame):
    if len(dataFrame) > 1:
        dataFrame["nu"] = dataFrame["nu"].astype(float)
        hyperfineFrequency = dataFrame["nu"].mean()
        hyperfineUncertainty = ((dataFrame["nu"] - hyperfineFrequency)**2).mean()**0.5
        dataFrame = dataFrame.head(1)
        dataFrame = dataFrame.reset_index()
        dataFrame["nu"][0] = hyperfineFrequency
        if hyperfineUncertainty > 0:
            dataFrame["unc1"][0] = hyperfineUncertainty
            dataFrame["unc2"][0] = hyperfineUncertainty
        else:
            dataFrame["unc1"][0] = dataFrame["unc"][0]
            dataFrame["unc2"][0] = dataFrame["unc"][0]
        return dataFrame
    else:
        return dataFrame


df = df.parallel_apply(lambda x:reduceHyperfine(x))
df = df.drop("index", axis=1)
# print(list(df.columns))

print("\n Dataframe output \n")
print(df.to_string(index=False))
print("\n \n")


df = df.dropna()
# columns = list(df.columns)[1:] # remove index column

df = df[["nu", "unc1", "unc2", "n1'", "n2'", "n3'", "n4'", "n5'", "n6'", "J'", "Ka'", "Kc'", "inv'",
"n1\"", "n2\"", "n3\"", "n4\"", "n5\"", "n6\"", "J\"", "Ka\"", "Kc\"", "inv\"", "Source"]]
# df = df[columns]
df = df.sort_values(by="nu")
df["Source"] = [f"21MeBiDoKi.{i + 1}" for i in range(len(df))] 
print(df.head(10))
# print(columns)
df_str = df.to_string(header=False, index=False)

marvelFile = r"21MeBiDoKi-MARVEL.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df_str)
