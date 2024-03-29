# MARVEL ND2H
The purpose of this repository is to track changes throughout the application of the MARVEL procedure (Measured Active Ro-Vibrational Energy Levels) (Furtenbacher et al. 2007) to transitions of the ammonia isotopologue ND2H. The work is to be carried out with the help of sixth-form students as part of the ORBYTS programme.

The latest online version (4.1) of MARVEL is available at: https://respecth.elte.hu/MARVEL4/index.html

<!-- # Project Structure
Within the main directory we keep the segments.txt file, needed by MARVEL to specify the units used by transitions of a given segment tag. The file Marvel-14NH3-2020.txt are the transitions from the 2020 MARVEL study. The file Marvel-14NH3-Main.txt is the current updated MARVEL set.

This project also includes a number of directories. Most are divided to be staging areas for extracted MARVEL data, within which the new transitions are converted to the MARVEL 2020 format. These directeries simply follow the naming conventions for the adopted segments during MARVEL studies. 

The directory CombinationDifferencesTests includes a set of MARVEL 2020 energy levels called 14NH3-MarvelEnergies-2020.txt. These are used by the script CombinationDifferences.py, which reads the previous Marvel-14NH3-2020.txt file and appends the new transitions from their respective directories, to apply combination differences tests using the previous MARVEL energy levels as lower states. The script also contains a Python list of source tags called `transitionsToRemove`, which are transitions that we have manually found to be inconsistent through the combination differences procedure or other means of validation. These transitions are invalidated i.e. a minus sign is put in front of the transition frequency. The script concludes by printing the resulting MARVEL transition set in order of ascending transition frequency into the Marvel-14NH3-Main.txt file (it is overwritten upon each run).  -->

---


The naming convention for papers is:\
`[last two digits of year][first 2 letters of first 4 authors].pdf`

For example, a paper written in 20**15** by Alan **Ap**pleseed, John **Do**e, Peter **Pa**rker, Tony **St**ark and Bruce Wayne would be named:\
`15ApDoPaSt.pdf`

The naming convention for the final `.txt` file is:\
`[paper name]-MARVEL.txt`

---

The columns of the final `.txt` file are as follows:

|$`\nu`$|$`\delta_1`$|$`\delta_2`$|$`\nu'_1`$|$`\nu'_2`$|$`\nu'_{3a}`$|$`\nu'_{3b}`$|$`\nu'_{4a}`$|$`\nu'_{4b}`$|$`J'`$|$`K_a'`$|$`K_c'`$|$`inv'`$|$`\nu^"_1`$|$`\nu^"_2`$|$`\nu^"_{3a}`$|$`\nu^"_{3b}`$|$`\nu^"_{4a}`$|$`\nu^"_{4b}`$|$`J^"`$|$`K_a^"`$|$`K_c^"`$|$`inv^"`$|source tag|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

$`\nu`$ indicates transition frequency.

$`\delta_1=\delta_2`$ is the transition uncertainty (include twice due to quirk in MARVEL).

Single prime $`'`$ denotes the higher/upper state energy.

Double prime $`^"`$ denotes the lower state energy.

There are 10 quantum numbers for each state.

The source tag formatting is as follows: [paper].[number]

---

## References
 Furtenbacher, T; Csaszar, AG; Tennyson, J; (2007) MARVEL: measured active rotational-vibrational energy levels. J MOL SPECTROSC , 245 (2) 115 - 125  https://doi.org/10.1016/j.jms.2007.07.005
