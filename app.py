#ATS and Adzuna combined display using streamlit
#imports
import pandas as pd
import streamlit as st
import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv(override=True)
from utils import display_date_helper, load_and_label

MONGO_URI = st.secrets["mongo"]["MONGO_URI"] or os.getenv("MONGO_URI")

#page setup
st.set_page_config(page_title="Jobs", 
                    layout="wide")
st.title("Datacat Job Search")
#change selectbox border color on dropdown
st.markdown(
    """
    <style>
    /*select box*/
    div[data-baseweb="select"] span {
        transition: none !important; 
    }
    div[data-baseweb="input"] > div:focus-within, 
    div[data-baseweb="select"] > div:focus-within {
        border-color: #93FF35 !important;
        box-shadow: 0 0 0 0.2rem rgba(147, 255, 53, 0.2) !important;
    }
    div[data-baseweb="select"], 
    div[data-baseweb="input"], 
    div[data-baseweb="select"] *, 
    div[data-baseweb="input"] * {
        transition: none !important;
        animation: none !important;
    }
    [data-baseweb="base-input"] > div::after {
        display: none !important;
    }
    [data-testid="stAppViewContainer"] {
        --st-primary-color: #93FF35 !important;
    }
    li[role="option"]:hover, li[aria-selected="true"] {
        background-color: #93FF35 !important;
        color: black !important;
    }
    /*text search*/
    .stTextInput div[data-baseweb="input"] {
        border-color: transparent !important;
    }

    .stTextInput div[data-baseweb="input"]:focus-within {
        border-color: #93FF35 !important;
        box-shadow: 0 0 0 0.2rem rgba(0, 123, 255, 0.25) !important;
    }
    [data-testid="stDecoration"] {
        background-image: linear-gradient(90deg, #93FF35, #93FF35) !important;
    }
    


    </style>
    """,
    unsafe_allow_html=True
    
)


# def display_date_helper(ts):
#     if pd.isna(ts): return "Unknown"
#     now = pd.Timestamp.now(tz='UTC')
#     diff = now - ts
    
#     if diff.total_seconds() < 3600:
#         return f"{int(diff.total_seconds() // 60)} min ago"
#     elif diff.total_seconds() < 86400:
#         return f"{int(diff.total_seconds() // 3600)} hours ago"
#     else:
#         return f"{diff.days} days ago"


@st.cache_data(ttl=600) 
def load_data():
    client = MongoClient(MONGO_URI)
    db = client['all_jobs']
    
    # #adzuna datas
    # adzuna_collection = db['adzuna_jobs']
    # adzuna_data = list(adzuna_collection.find({}, {"_id":0}))
    # df_adzuna = pd.DataFrame(adzuna_data)
    # df_adzuna['source'] = 'Adzuna'
    
    # #ATS DATAS
    # #greenhouse
    # greenhouse_collection = db['greenhouse_jobs']
    # greenhouse_data = list(greenhouse_collection.find({}, {"_id":0}))
    # df_greenhouse = pd.DataFrame(greenhouse_data)
    # df_greenhouse['source'] = 'Greenhouse'
    
    # #lever
    # lever_collection = db['lever_jobs']
    # lever_data = list(lever_collection.find({}, {"_id":0}))
    # df_lever = pd.DataFrame(lever_data)
    # df_lever['source'] = 'Lever'
    
    # #ashby
    # ashby_collection = db ['ashby_jobs']
    # ashby_data = list (ashby_collection.find({}, {"_id":0}))
    # df_ashby = pd.DataFrame(ashby_data)
    # df_ashby['source'] = 'Ashby'
    
    # #combined
    # combined_df = pd.concat([df_adzuna, df_greenhouse, df_lever, df_ashby], ignore_index=True)

    sources = [
        ("adzuna_jobs", "Adzuna"),
        ("greenhouse_jobs", "Greenhouse"),
        ("lever_jobs", "Lever"),
        ("ashby_jobs", "Ashby"),
        ("workable_board_jobs", "Workable")
    ]
    dfs = [load_and_label(coll, label) for coll, label in sources]
    combined_df = pd.concat(dfs, ignore_index=True)
    
    if 'date_posted' in combined_df.columns:
        combined_df['date_posted'] = pd.to_datetime(combined_df['date_posted'], utc=True, errors='coerce')
        combined_df = combined_df.dropna(subset=['date_posted'])

    return combined_df



try:
    df = load_data().copy()
    df = df.sort_values(by='date_posted', ascending=False).reset_index(drop=True)
    df["is_remote"] = df["is_remote"].map({True: "True", False: "False"})
    df['time_since_posted'] = df['date_posted'].apply(display_date_helper)
    df["is_remote"] = df["is_remote"].fillna("Unkown")
    df["is_remote"] = df["is_remote"].replace(["None"], "Unknown")
    if not df.empty:
        column_order = [
            "job_title",
            "url",             
            "company",
            "location",
            "is_remote",            
            "salary_range",
            "salary_range_usd",
            "time_since_posted",
            "date_posted",
            "search_flag",
            "source"   
            
        ]
        #logo
        st.sidebar.image("assets/datacat-dark-side.png", 
                        width="stretch")
        #filters
        st.sidebar.header("Filters")
        #return remote only roles


            
        #filter by search profile
        PROFILE_MAP ={
            "Ryan": ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security engineer", "security analyst", "security", "information security"],
            "Mik": ["frontend", "frontend developer", "front-end", "vue", "product engineer"]
        }
        profiles = list(PROFILE_MAP.keys())
        selected_profile = st.sidebar.selectbox("Search profile", ["all"] + profiles)
        
        
        #apply profile selection filter
        if selected_profile != "all":
            target_tags = PROFILE_MAP[selected_profile]
            
            df = df[df['search_flag'].isin(target_tags)]

        
        #filter by source - multiselect
        sources = sorted(df['source'].unique().tolist())
        selected_source = st.sidebar.multiselect("Posting source", options=sources, default=[])
        
        #apply source filter
        if selected_source:
            df = df[df['source'].isin(selected_source)]
        
        #search job titles OR description for key words
        search = st.sidebar.text_input("Search text", "")        
        #apply job search
        if search:
            df = df[df['job_title'].str.contains(search, case=False, na=False) | df['description'].str.contains(search, case=False, na=False) | df['company'].str.contains(search, case=False, na=False)]
        
        #search by company only
        company_search = st.sidebar.text_input("Search by Company", "")
        if company_search:
            df = df[df['company'].str.contains(company_search, case=False, na=False)]
        
        #remote only filter
        remote_selected = st.sidebar.checkbox("Remote only",value=False, help="This will exclude 'Unknown' values.")
        
        #apply remote only filter
        if remote_selected:
            df = df[df['is_remote'] == "True"]

        
        display_df = df.copy()
        display_df['date_posted'] = display_df['date_posted'].dt.strftime('%Y-%m-%d %H:%M')


        st.write(f"Showing {len(df)} jobs found from job database.")
        
        st.dataframe(
            display_df[column_order].style.set_properties(**{'color': '#93FF35'}, subset=['url']),
            column_config={
                "url": st.column_config.LinkColumn("Url", display_text="View Job ↗"), 
                "salary_range": "Salary (Local)",
                "salary_range_usd": "Salary (USD)",
                "date_posted": "Posted On",
                "job_title": "Job Title",
                "company": "Company",
                "location": "Location",
                "time_since_posted": "Time Since Posted",
                "search_flag": "Search Flag",
                "is_remote": "Remote"
                
            },
            hide_index=True,
            width="stretch",
            height=900,
            key="job_board_v1"
        )
    else:
        st.warning("No data found in the database. Run your api calls first!")

except Exception as e:
    st.error(f"Error connecting to MongoDB: {e}")