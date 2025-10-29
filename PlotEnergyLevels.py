import matplotlib.pyplot as plt
import pandas as pd 
import scienceplots
plt.style.use('science')

plt.rcParams['axes.linewidth'] = 2

marvelColumns = ["nu1", "nu2", "nu3a", "nu3b", "nu4a", "nu4b", "J", "Ka", "Kc", "inv", "E", "Uncertainty", "transitions"]

marvelEnergies = pd.read_csv("MarvelEnergyLevels.txt", delim_whitespace=True)
marvelEnergies["band"] = marvelEnergies["nu1"].astype(str) + marvelEnergies["nu2"].astype(str) + marvelEnergies["nu3a"].astype(str) + marvelEnergies["nu3b"].astype(str) +  marvelEnergies["nu4a"].astype(str) +   marvelEnergies["nu4b"].astype(str)

groundState = marvelEnergies[marvelEnergies["band"] == "000000"]
v1State = marvelEnergies[marvelEnergies["band"] == "100000"]
v2State = marvelEnergies[marvelEnergies["band"] == "010000"]
v3aState = marvelEnergies[marvelEnergies["band"] == "001000"]
v3bState = marvelEnergies[marvelEnergies["band"] == "000100"]
v4aState = marvelEnergies[marvelEnergies["band"] == "000010"]
v4bState = marvelEnergies[marvelEnergies["band"] == "000001"]

fontsize=40
# print(marvelEnergies.head(20).to_string(index=False))
plt.plot(groundState["J"], groundState["E"], "b.", label="g.s", markersize=10)
plt.plot(v1State["J"], v1State["E"], "r.", label=r"$\nu_1$", markersize=10)
plt.plot(v2State["J"], v2State["E"], "m.", label=r"$\nu_2$", markersize=10)
plt.plot(v3aState["J"], v3aState["E"], "y.", label=r"$\nu_{3_a}$", markersize=10)
# plt.plot(v3bState["J"], v3bState["E"], ".", "gold", label=r"$\nu_{3_b}$", markersize=10)
plt.xticks(fontsize=fontsize, weight="bold")
plt.yticks(fontsize=fontsize, weight="bold")
plt.legend(fontsize=fontsize, prop={"size": 30}, frameon=True)
plt.show()