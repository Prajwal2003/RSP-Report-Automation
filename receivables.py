from pymongo import MongoClient
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# MongoDB connection
mongo_uri = "mongodb://localhost:27017/"
client = MongoClient(mongo_uri)

# Specify the database and collection
db = client['test']
consumer_transaction_collection = db['tblConsumerTransaction']

# Fetch the current date
current_date = datetime.now()

# Define the list of names to filter by
names_to_filter = [
    "NARMADA SOLVEX PVT LTD",
    "PANDIT HEERALAL VYAS FARMERS PRODUCER",
    "RADIANT DISTRIBUTION ENTERPRISE",
    "RAHUL AGRICOM COMPANY",
    "RISHABH JAIN",
    "SACHIN INTERNATIONAL PROTEINS PRIVATE LIMITED",
    "SAMUNNATI AGRO SOLUTIONS PRIVATE LIMITED",
    "SUPERIOR MALT PVT LTD"
]

# Aggregation pipeline
pipeline = [
    {
        "$match": {
            "Status": "FinancePending"
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "BuyerId",
            "foreignField": "_id",
            "as": "user_info"
        }
    },
    {
        "$unwind": "$user_info"
    },
    {
        "$addFields": {
            "days_due": {
                "$dateDiff": {
                    "startDate": "$PaymentDate",
                    "endDate": current_date,
                    "unit": "day"
                }
            }
        }
    },
    {
        "$match": {
            "days_due": {"$gt": 0},
            "user_info.profile.CompanyName": {"$in": names_to_filter}
        }
    },
    {
        "$bucket": {
            "groupBy": "$days_due",
            "boundaries": [0, 30, 60, 90, float('inf')],
            "default": "> 90 days",
            "output": {
                "TotalPOAmount": {"$sum": {"$divide": ["$POAmount", 100000]}},
                "CompanyName": {"$first": "$user_info.profile.CompanyName"},
                "POAmounts": {"$push": {"DaysDue": "$days_due", "POAmount": {"$divide": ["$POAmount", 100000]}}}
            }
        }
    },
    {
        "$group": {
            "_id": "$CompanyName",
            "1_30_days": {
                "$sum": {"$cond": [{"$eq": ["$_id", 30]}, "$TotalPOAmount", 0]}
            },
            "31_60_days": {
                "$sum": {"$cond": [{"$eq": ["$_id", 60]}, "$TotalPOAmount", 0]}
            },
            "61_90_days": {
                "$sum": {"$cond": [{"$eq": ["$_id", 90]}, "$TotalPOAmount", 0]}
            },
            ">90_days": {
                "$sum": {"$cond": [{"$eq": ["$_id", "> 90 days"]}, "$TotalPOAmount", 0]}
            },
            "GrandTotal": {"$sum": "$TotalPOAmount"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "CompanyName": "$_id",
            "1_30_days": 1,
            "31_60_days": 1,
            "61_90_days": 1,
            ">90_days": 1,
            "GrandTotal": 1
        }
    }
]

# Execute the aggregation pipeline
result = consumer_transaction_collection.aggregate(pipeline)

# Google Sheets credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('path/to/your/credentials.json', scope)
client_gspread = gspread.authorize(creds)

# Open the Google Sheet
sheet = client_gspread.open("Your Google Sheet Name").sheet1

# Write headers to the sheet
sheet.append_row(["Company Name", "1-30 Days", "31-60 Days", "61-90 Days", ">90 Days", "Grand Total"])

# Write data to the sheet
for doc in result:
    row = [
        doc['CompanyName'],
        doc['1_30_days'],
        doc['31_60_days'],
        doc['61_90_days'],
        doc['>90_days'],
        doc['GrandTotal']
    ]
    sheet.append_row(row)

# Optionally, close the MongoDB connection
client.close()