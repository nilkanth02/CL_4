import pandas as pd
 
# Extract data from a CSV (you can replace with your dataset)
url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
df = pd.read_csv(url)
 
# Preview the data
print(df.head())
 
# Check for null values
print(df.isnull().sum())
 
# Feature Engineering: Add Tip % Column
df['tip_pct'] = df['tip'] / df['total_bill']
 
# Convert categorical to category type
df['sex'] = df['sex'].astype('category')
df['smoker'] = df['smoker'].astype('category')
df['day'] = df['day'].astype('category')
df['time'] = df['time'].astype('category')
 
# Summary
print(df.describe())
 

# Load into cleaned DataFrame (could be into a DB too)
cleaned_df = df.copy()
 
# Save to new CSV if needed
cleaned_df.to_csv("cleaned_tips_data.csv", index=False)
 
import seaborn as sns
import matplotlib.pyplot as plt
 
# Set theme
sns.set(style="whitegrid")
 
# 1. Tip percentage vs. total bill
plt.figure(figsize=(8, 6))
sns.scatterplot(data=cleaned_df, x="total_bill", y="tip_pct", hue="sex")
plt.title("Tip % vs Total Bill")
plt.xlabel("Total Bill ($)")
plt.ylabel("Tip %")
plt.show()
 
# 2. Distribution of Tip %
plt.figure(figsize=(8, 6))
sns.histplot(cleaned_df["tip_pct"], kde=True, color="purple")
plt.title("Distribution of Tip Percentage")
plt.show()
 

# 3. Average Tip % by Day
plt.figure(figsize=(8, 6))
sns.barplot(data=cleaned_df, x="day", y="tip_pct", hue="sex")
plt.title("Average Tip % by Day and Gender")
plt.ylabel("Tip %")
plt.show()
 

# 4. Heatmap of correlation
plt.figure(figsize=(8, 6))
sns.heatmap(cleaned_df.corr(numeric_only=True), annot=True, cmap="YlGnBu")
plt.title("Feature Correlation Heatmap")
plt.show()
