import pandas as pd

#YELLOW
yellow_jan = pd.read_csv("yellow.csv", sep = ",")
pd.isnull(yellow_jan).sum()
yellow_jan['index'] = range(1, len(yellow_jan) + 1)
yellow_jan.to_csv("yellow_1.csv", sep = ",", index = False)

yellow_feb = pd.read_csv("yellow_feb.csv", sep = ",")
pd.isnull(yellow_feb).sum()
yellow_feb = yellow_feb.reset_index()
yellow_feb['index'] = yellow_feb.index + 1048575
yellow_feb.to_csv("yellow_2.csv", sep = ",")

yellow_mar = pd.read_csv("yellow_mar.csv", sep = ",")
pd.isnull(yellow_mar).sum()
yellow_mar = yellow_mar.reset_index()
yellow_mar['index'] = yellow_mar.index + 2097150
yellow_mar.to_csv("yellow_3.csv", sep = ",")

yellow_apr = pd.read_csv("yellow_apr.csv", sep = ",")
pd.isnull(yellow_apr).sum()
yellow_apr = yellow_apr.reset_index()
yellow_apr['index'] = yellow_apr.index + 3145725
yellow_apr.to_csv("yellow_4.csv", sep = ",")

yellow_may = pd.read_csv("yellow_may.csv", sep = ",")
pd.isnull(yellow_may).sum()
yellow_may = yellow_may.reset_index()
yellow_may['index'] = yellow_may.index + 4194300
yellow_may.to_csv("yellow_5.csv", sep = ",")

#GREEN
green_jan = pd.read_csv("green.csv", sep = ",")
green_jan['index'] = range(1, len(green_jan) +1)
pd.isnull(green_jan).sum()
green_jan.to_csv("green_1.csv", sep = ",")

green_feb = pd.read_csv("green_tripdata_2017-02.csv", sep = ",")
pd.isnull(green_feb).sum()
green_feb = green_feb.reset_index()
green_feb['index'] = green_feb.index + 1048576
green_feb.to_csv("green_2.csv", sep = ",")

green_mar = pd.read_csv("green_tripdata_2017-03.csv", sep = ",")
pd.isnull(green_mar).sum()
green_mar = green_mar.reset_index()
green_mar['index'] = green_mar.index + 2071839
green_mar.to_csv("green_3.csv", sep = ",")

green_apr = pd.read_csv("green_tripdata_2017-04.csv", sep = ",")
pd.isnull(green_apr).sum()
green_apr = green_apr.reset_index()
green_apr['index'] = green_apr.index + 3120414
green_apr.to_csv("green_4.csv", sep = ",")

green_may = pd.read_csv("green_tripdata_2017-05.csv", sep = ",")
pd.isnull(green_may).sum()
green_may = green_apr.reset_index()
green_may['index'] = green_may.index + 4168989
green_may.to_csv("green_5.csv", sep = ",")








































