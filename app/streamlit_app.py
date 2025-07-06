import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
@st.cache_resource
def get_db_connection():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    return create_engine(db_url)

def get_agendas():
    """Get list of unique agenda items"""
    with get_db_connection().connect() as conn:
        query = """
        SELECT DISTINCT title, date, confer_number, dae_number 
        FROM speeches 
        ORDER BY date DESC
        """
        return pd.read_sql(query, conn)

def get_speeches(confer_number, dae_number):
    """Get all speeches for a specific conference"""
    with get_db_connection().connect() as conn:
        query = """
        SELECT speaker, text, 
               CASE WHEN text LIKE '%찬성%' OR text LIKE '%지지%' OR text LIKE '%옳%' THEN '찬성'
                    WHEN text LIKE '%반대%' OR text LIKE '%문제%' OR text LIKE '%우려%' THEN '반대'
                    ELSE '중립' END as stance
        FROM speeches 
        WHERE confer_number = :confer_number AND dae_number = :dae_number
        """
        return pd.read_sql(text(query), conn, params={"confer_number": confer_number, "dae_number": dae_number})

def main():
    st.set_page_config(page_title="국회 발언 분석 대시보드", layout="wide")
    st.title("국회 발언 분석 대시보드")

    # Sidebar for filters
    st.sidebar.header("필터")
    
    # Get all agendas
    agendas = get_agendas()
    
    # Select an agenda
    selected_agenda = st.sidebar.selectbox(
        '안건 선택',
        options=agendas['title'].unique()
    )
    
    # Get the selected agenda's details
    selected_row = agendas[agendas['title'] == selected_agenda].iloc[0]
    
    # Display agenda details
    st.header(selected_agenda)
    st.caption(f"회의일: {selected_row['date'].strftime('%Y-%m-%d')} | 회의번호: {selected_row['confer_number']} | 대수: {selected_row['dae_number']}")
    
    # Get speeches for selected agenda
    speeches = get_speeches(selected_row['confer_number'], selected_row['dae_number'])
    
    # Display stance distribution
    st.subheader("의견 분포")
    if not speeches.empty:
        # Count stances
        stance_counts = speeches['stance'].value_counts().reset_index()
        stance_counts.columns = ['의견', '인원수']
        
        # Create pie chart
        fig = px.pie(stance_counts, values='인원수', names='의견', 
                     title='의견 분포', 
                     color='의견',
                     color_discrete_map={'찬성': '#1f77b4', '반대': '#ff7f0e', '중립': '#2ca02c'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Display speakers by stance
        st.subheader("발언자별 의견")
        for stance in ['찬성', '반대', '중립']:
            stance_speeches = speeches[speeches['stance'] == stance]
            if not stance_speeches.empty:
                with st.expander(f"{stance} ({len(stance_speeches)}명)"):
                    for _, row in stance_speeches.iterrows():
                        st.markdown(f"**{row['speaker']}**")
                        st.caption(row['text'][:200] + (row['text'][200:] and '...'))
                        st.write("---")
    else:
        st.warning("이 안건에 대한 발언이 없습니다.")

if __name__ == "__main__":
    main()
