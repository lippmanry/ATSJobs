#ATS and Adzuna combined display using streamlit
#imports
import pandas as pd
import streamlit as st
import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv(override=True)

MONGO_URI = st.secrets["mongo"]["MONGO_URI"] or os.getenv("MONGO_URI")

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

#page setup
st.set_page_config(page_title="Jobs", 
                    layout="wide")
st.title("Datacat Job Search")



@st.cache_data(ttl=600) 
def load_data():
    client = MongoClient(MONGO_URI)
    db = client['all_jobs']
    
    #adzuna datas
    adzuna_collection = db['adzuna_jobs']
    adzuna_data = list(adzuna_collection.find({}, {"_id":0}))
    df_adzuna = pd.DataFrame(adzuna_data)
    df_adzuna['source'] = 'Adzuna'
    
    #ats datas
    greenhouse_collection = db['greenhouse_jobs']
    greenhouse_data = list(greenhouse_collection.find({}, {"_id":0}))
    df_greenhouse = pd.DataFrame(greenhouse_data)
    df_greenhouse['source'] = 'Greenhouse'
    
    combined_df = pd.concat([df_adzuna, df_greenhouse], ignore_index=True)

    if 'date_posted' in combined_df.columns:
        combined_df['date_posted'] = pd.to_datetime(combined_df['date_posted'], utc=True, errors='coerce')
    return combined_df



try:
    df = load_data()
    df["is_remote"] = df["is_remote"].map({True: "True", False: "False"})
    df["is_remote"] = df["is_remote"].fillna()
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
                        use_container_width=True)
        #filters
        st.sidebar.header("Filters")

            
        #filter by search tag
        tags = sorted(df['search_flag'].unique().tolist())
        selected_tag = st.sidebar.selectbox("Search flag", ["all"] + tags)
        
        
        #apply tag filter
        if selected_tag != "all":
            df = df[df['search_flag'] == selected_tag]
        
        # #filter by source
        # sources = sorted(df['source'].unique().tolist())
        # selected_source = st.sidebar.selectbox("Posting source", ["all"] + sources)
        
        # #apply source filter
        # if selected_source != "all":
        #     df = df[df['source'] == selected_source]
        
        #filter by source - multiselect
        sources = sorted(df['source'].unique().tolist())
        selected_source = st.sidebar.multiselect("Posting source", options=sources, default=[])
        
        #apply source filter
        if selected_source:
            df = df[df['source'].isin(selected_source)]
        
        #search job titles OR description for key words
        search = st.sidebar.text_input("Search", "")        
        #apply job title search
        if search:
            df = df[df['job_title'].str.contains(search, case=False, na=False) | df['description'].str.contains(search, case=False, na=False)]

        
        df = df.sort_values(by='date_posted', ascending=False)   

        st.write(f"Showing {len(df)} jobs found from job database.")
        

        st.dataframe(
            df[column_order].style.set_properties(**{'color': '#93FF35'}, subset=['url']),
            column_config={
                "url": st.column_config.LinkColumn("Url", display_text="View Job ↗"), 
                "salary_range": "Salary (Local)",
                "salary_range_usd": "Salary (USD)",
                "date_posted": "Posted On",
                "job_title": "Job Title",
                "company": "Company",
                "location": "Location",
                "time_since_posted": "Time Since Posted",
                "search_flag": "Search Flag"
            },
            hide_index=True,
            width="stretch",
            height=900
        )
    else:
        st.warning("No data found in the database. Run your api calls first!")

except Exception as e:
    st.error(f"Error connecting to MongoDB: {e}")