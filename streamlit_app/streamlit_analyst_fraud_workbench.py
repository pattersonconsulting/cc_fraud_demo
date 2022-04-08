import streamlit as st
import pandas as pd
import numpy as np
import json

import datetime

from streamlit_folium import folium_static
import folium

from streamlit_timeline import timeline

from pypmml import Model



def color_translator (row):
	if 1 == row:
		return "'red'"
	
	else:
		return "'blue'"

print("\n\nTop of Python Script")

st.set_page_config(layout="wide")

st.header('Credit Card Fraud Detection Workbench')


# second level of layout: rows inside mid col
#top_container_mid_col = st.container()
#id_container = st.container()
#app_body_container_mid_col = st.container()


#txn_test_col, map_col = st.columns([1,2])


top_filter_select_col, top_map_col = st.columns([1,2])


mid_container_col, foo_col = st.columns([10,1])

bottom_row_col_analyze, foo2_col  = st.columns([10,1])


print("loading dataframe data...")
# data load
df_transactions = pd.read_csv( "./data/cc_fraud_merged_working_data.csv")
df_transactions = df_transactions.loc[:, ~df_transactions.columns.str.contains('^Unnamed')]

df_transactions["marker_color"] = df_transactions['TX_FRAUD'].apply(lambda x: color_translator(x))
df_transactions['TX_DATETIME'] = pd.to_datetime(df_transactions['TX_DATETIME'])



# load ML Models
print("loading ml model...")

full_model_path = "./models/xgb_cc_fraud_20220405_v2.pmml"

xgb_cc_fraud_pmml_pipeline = Model.load(full_model_path)

#print(xgb_cc_fraud_pmml_pipeline)

# now score all records

input_features = ['TERMINAL_ID_RISK_30DAY_WINDOW', 'CUST_AVG_AMOUNT_1', 'CUST_AVG_AMOUNT_30', 'TX_AMOUNT', 'TERMINAL_ID_RISK_1DAY_WINDOW', 'CUST_CNT_TX_30', 'CUST_CNT_TX_7', 'TERMINAL_ID_NB_TX_30DAY_WINDOW', 'CUST_AVG_AMOUNT_7', 'TERMINAL_ID_NB_TX_7DAY_WINDOW', 'CUST_CNT_TX_1', 'TX_DURING_WEEKEND', 'TERMINAL_ID_RISK_7DAY_WINDOW', 'TX_DURING_NIGHT']


# test fraud record 293193




# state variables

default_cust_number = 323
#current_date = "5/1/2018"
current_date = datetime.date(2018, 8, 10)
#print("Setting current date: " + current_date.strftime('%m/%d/%Y') )


def renderMainAppBody_AboutThisApp():

	print("renderMainAppBody_AboutThisApp...")


	with mid_container_col:

		st.header("About this App")
		st.subheader("Who is the user?")
		st.write("Analyst at bank")
		st.subheader("What is their function?")
		st.write("Manually reviewing the top 20 fraud cases per day")

		st.subheader("Purpose of this App")

		st.write("This prototype application is an example of how [Patterson Consulting](http://pattersonconsultingtn.com) builds Applied Insights with analytics and Machine Learning for Fortune 500 Customers")

		st.subheader("References")

		st.write("The data in this application is based on the data from the online book:")

		st.markdown("[Reproducible Machine Learning for Credit Card Fraud Detection - Practical Handbook](https://github.com/Fraud-Detection-Handbook/fraud-detection-handbook), by Le Borgne, Yann-Aël and Siblini, Wissam and Lebichot, Bertrand and Bontempi, Gianluca, 2022, Université Libre de Bruxelles")


def renderMainAppBody_Top20Fraud():

	print("renderMainAppBody_Top20Fraud...")


	with top_filter_select_col:

		st.subheader("Calendar Date")

		#print("Top20 - Setting current date: " + current_date.strftime('%m/%d/%Y'))

		current_date_select = st.date_input("Date for Fraud", value=current_date)
		#st.write('Current Date:', current_date)
		#st.write('Current current_date_select:', current_date_select)



	with top_map_col:

		st.subheader("Model Threshold Slider")

		fraud_threshold = st.select_slider('Select Threshold for Fraud', value=0.9,
		options=[1.0, 0.975, 0.95, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])



	with mid_container_col:

		st.header("Top Scored Fraudulent Transactions")
		
		#st.write("(holla holla holla)")

		cols_to_show_top20 = ["CUSTOMER_ID", "TRANSACTION_ID", "TERMINAL_ID", "TX_DATETIME", "TX_AMOUNT", "FRAUD_SCORE"]

		df_rows_found = df_transactions[ (df_transactions["TX_DATETIME"].dt.date == current_date_select) & (df_transactions["FRAUD_SCORE"] >= fraud_threshold) ][cols_to_show_top20].sort_values(by=['FRAUD_SCORE'], ascending=False)

		rows_found = len(df_rows_found.index)
		st.write('Transactions Found:', rows_found)


		# , "FRAUD_ESTIMATED_PROBABILITY"
		st.dataframe(df_rows_found)




def renderMainAppBody_CustomerAnalysis():

	print("renderMainAppBody_CustomerAnalysis")

	with top_filter_select_col:

		st.subheader("Lookup Transactions by Customer")

		#with st.expander("Customer Lookup"):
		#cust_number = st.number_input('Enter Customer ID', value=5)
		#st.write('The current customer number is ', cust_number)

		customer_id_array = df_transactions.sort_values(by=['FRAUD_SCORE'], ascending=False)["CUSTOMER_ID"].unique()

		print("Customer IDs: ")
		print(customer_id_array)

		itemindex = np.where(customer_id_array == 273)


		option_cust_id = st.selectbox(
			'Select Customer Number',
			customer_id_array, index=3
			)


		txn_filter_mode = st.radio("Filter Customer Transaction View: ",('Fraud Transactions', 'All Transactions'))



	with top_map_col:


		if txn_filter_mode == "All Transactions":

			df_cust_transactions = df_transactions[ df_transactions["CUSTOMER_ID"] == option_cust_id]

			

		else:

			df_cust_transactions = df_transactions[ (df_transactions["CUSTOMER_ID"] == option_cust_id) & (df_transactions["TX_FRAUD"] == 1)]


		st.subheader("Map of Transaction")

		

		m = folium.Map(location=[33.754278, -84.383527], zoom_start=12,tiles='openstreetmap')

		print("render map")


		for _, terminal in df_cust_transactions.iterrows():
			try:
				if 1 == terminal["TX_FRAUD"]:
					marker_div = f"""<div><svg><circle cx="10" cy="10" r="10" fill=""" + terminal['marker_color'] + """ opacity="1.0"/></svg></div>"""
				else:
					marker_div = f"""<div><svg><circle cx="6" cy="6" r="6" fill=""" + terminal['marker_color'] + """ opacity="0.5"/></svg></div>"""

				marker = folium.Marker(
					location=[terminal['lat'],terminal["long"]],
					tooltip=str( terminal['TX_DATETIME']  ),
					#icon=folium.Icon(color=terminal['marker_color'])
					icon=folium.DivIcon(html=marker_div)
				)
				marker.add_to( m )
				#print("marker" + marker_div)
			except Exception as e:
				print( "Error: ")
				print( e )
		

		    # call to render Folium map in Streamlit
		folium_static(m, height=300, width=800)



	with mid_container_col:



		if txn_filter_mode == "All Transactions":

			df_cust_transactions = df_transactions[ df_transactions["CUSTOMER_ID"] == option_cust_id]

			st.write('Transactions for Customer ', option_cust_id)

			

		else:

			df_cust_transactions = df_transactions[ (df_transactions["CUSTOMER_ID"] == option_cust_id) & (df_transactions["TX_FRAUD"] == 1)]

			st.write('Fraudulent Transactions for Customer ', option_cust_id)


		df_view_txns = df_cust_transactions.copy()

		#df_cust_transactions	

		print("\nRender scored view of data....")

		print(df_view_txns)

		if len(df_view_txns.index) == 0:

			print("No Transactions to Score")

		else:


			probs = xgb_cc_fraud_pmml_pipeline.predict( df_view_txns[ input_features ] )

			print(probs)


			#probas_ = xgb_model.predict( X )
			prediction_est_prob = probs.iloc[:, 1]  

			df_view_txns["FRAUD_ESTIMATED_PROBABILITY"] = prediction_est_prob.values

			st.dataframe(df_view_txns[["TRANSACTION_ID", "TERMINAL_ID", "TX_DATETIME", "TX_AMOUNT", "TX_FRAUD", "FRAUD_ESTIMATED_PROBABILITY", "CUST_AVG_AMOUNT_1", "CUST_AVG_AMOUNT_7", "CUST_AVG_AMOUNT_30"]])




		# load data
		#with open('./data/timeline_test_2.json', "r") as f:
		#	data = f.read()

		#print(df_transactions.info())


		json_temp = { "events": [] }

		for index, row in df_cust_transactions.iterrows():

			if 1 == row["TX_FRAUD"]:

				print("timeline fraud event: " + str(row["TRANSACTION_ID"]) + ", Amount: $" + str(row["TX_AMOUNT"]) )

				new_json = { 
					"start_date": {
						"year": row["TX_DATETIME"].year,
						"month": row["TX_DATETIME"].month,
						"day": row["TX_DATETIME"].day,
						"hour": row["TX_DATETIME"].hour,
						"minute": row["TX_DATETIME"].minute
					}, 
					"text": {
						"text": "Fraudulent Transaction Detected: " + str(row["TRANSACTION_ID"]) + ", Amount: $" + str(row["TX_AMOUNT"])
					},
					"background": {
						"color": "#FF0000"
					}

				}

				json_temp["events"].append( new_json )

			else:
				new_json = { 
					"start_date": {
						"year": row["TX_DATETIME"].year,
						"month": row["TX_DATETIME"].month,
						"day": row["TX_DATETIME"].day,
						"hour": row["TX_DATETIME"].hour,
						"minute": row["TX_DATETIME"].minute
					}, 
					"text": {
						"text": "Transaction: " + str(row["TRANSACTION_ID"]) + ", Amount: $" + str(row["TX_AMOUNT"])
					}
				}

				json_temp["events"].append( new_json )


		#print(json_temp)

		# render timeline
		timeline(json_temp, height=300)





	with bottom_row_col_analyze:


		if txn_filter_mode == "All Transactions":

			df_cust_transactions = df_transactions[ df_transactions["CUSTOMER_ID"] == option_cust_id]

			

		else:

			df_cust_transactions = df_transactions[ (df_transactions["CUSTOMER_ID"] == option_cust_id) & (df_transactions["TX_FRAUD"] == 1)]

			


		st.subheader("Analyze Transaction")

		#transaction_number = st.number_input('Enter Transaction ID to Analyze', value=0)
		option_tx_id = st.selectbox(
			'Which Transaction Do You Wish to Analyze?',
			df_cust_transactions["TRANSACTION_ID"].values
			)

		st.write('This is where we tests transactions against the saved model')

		# df_transactions[ df_transactions["CUSTOMER_ID"] == cust_number]

		st.dataframe(df_transactions[ df_transactions["TRANSACTION_ID"] == option_tx_id ][["TRANSACTION_ID", "TERMINAL_ID", "TX_DATETIME", "TX_AMOUNT", "TX_FRAUD", "CUST_AVG_AMOUNT_1", "CUST_AVG_AMOUNT_7", "CUST_AVG_AMOUNT_30"]])

		print( xgb_cc_fraud_pmml_pipeline.inputNames )

		#input_features = ['TERMINAL_ID_RISK_30DAY_WINDOW', 'CUST_AVG_AMOUNT_1', 'CUST_AVG_AMOUNT_30', 'TX_AMOUNT', 'TERMINAL_ID_RISK_1DAY_WINDOW', 'CUST_CNT_TX_30', 'CUST_CNT_TX_7', 'TERMINAL_ID_NB_TX_30DAY_WINDOW', 'CUST_AVG_AMOUNT_7', 'TERMINAL_ID_NB_TX_7DAY_WINDOW', 'CUST_CNT_TX_1', 'TX_DURING_WEEKEND', 'TERMINAL_ID_RISK_7DAY_WINDOW', 'TX_DURING_NIGHT']


		#predictions = xgb_cc_fraud_pmml_pipeline.predict( df_transactions[ df_transactions["TRANSACTION_ID"] == option_tx_id ][ input_features ] )


		

		# the pmml model wants you to only send the features it has definitions for, otherwise "boom"
		#predictions = xgb_cc_fraud_pmml_pipeline.predict( df_transactions[ df_transactions["TRANSACTION_ID"] == 293193 ][ input_features ] )


		probs = xgb_cc_fraud_pmml_pipeline.predict( df_transactions[ df_transactions["TRANSACTION_ID"] == option_tx_id ][ input_features ] )


		#probas_ = xgb_model.predict( X )
		prediction_est_prob = probs.iloc[:, 1]  

		print( "Estimated probability of fraud: " + str(prediction_est_prob[0]) )

		#st.dataframe( probs )
		#st.write( "Estimated probability of fraud: " + str( "{:.2f}".format( prediction_est_prob[0] ) ) )

		st.metric(label="Estimated probability of fraud", value=str( "{:.4f}".format( prediction_est_prob[0] ) ))



# AND in st.sidebar!
with st.sidebar:
	st.image("http://www.pattersonconsultingtn.com/images/pct_box_logo_big.png", width=100)


	st.header("Application Mode")


	#transaction_number = st.number_input('Enter Transaction ID to Analyze', value=0)
	option_app_mode = st.selectbox(
		'Select App View',
		["Fraud Case Search", "Customer Analysis", "About This Application"]
		)	


	if option_app_mode == "About This Application":

		renderMainAppBody_AboutThisApp()

	elif option_app_mode == "Fraud Case Search":

		renderMainAppBody_Top20Fraud()


	else:

		renderMainAppBody_CustomerAnalysis()



