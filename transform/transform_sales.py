import pandas as pd
RAW_PATH = "data/raw/superstore_sales.csv"
def main():
	df = pd.read_csv(RAW_PATH)
	print(df.head())
if  __name__ ==  "__main__":
	main()
