"""
Hospital Satisfaction Dashboard

A Streamlit dashboard for visualizing hospital satisfaction survey data from PostgreSQL.
Run with: streamlit run dashboard.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from repositories.load_postgress import get_postgres_engine


# Page configuration
st.set_page_config(
    page_title="Hospital Satisfaction Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_resource
def get_connection():
    """Get PostgreSQL connection engine (cached)."""
    return get_postgres_engine()


@st.cache_data(ttl=600)
def load_data(query: str):
    """Load data from PostgreSQL with caching (10 min TTL)."""
    engine = get_connection()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_main_data():
    """Load main satisfaction data."""
    query = "SELECT * FROM satisfaction_2016_cleaned"
    return load_data(query)


def load_hospital_scores():
    """Load aggregated hospital scores."""
    query = "SELECT * FROM hospital_scores ORDER BY overall_average DESC"
    return load_data(query)


def load_question_texts():
    """Load question metadata."""
    query = "SELECT * FROM question_texts ORDER BY question_number"
    df = load_data(query)
    # Convert to standard types to avoid Arrow serialization issues
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)
    return df


def main():
    st.title("🏥 Hospital Satisfaction Dashboard")
    st.markdown("### Patient Satisfaction Survey Analysis 2016")
    
    # Sidebar
    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Choose a view:",
        ["Overview", "Hospital Comparison", "Question Analysis", "Data Explorer"]
    )
    
    # Load data
    try:
        if page in ["Overview", "Hospital Comparison", "Question Analysis"]:
            hospital_scores = load_hospital_scores()
        
        if page in ["Data Explorer"]:
            main_data = load_main_data()
        
        if page in ["Question Analysis"]:
            question_texts = load_question_texts()
    
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.info("Make sure PostgreSQL is running and the ETL has been executed.")
        st.code("bash scripts/start_postgres.sh\npython main.py")
        return
    
    # Overview Page
    if page == "Overview":
        st.header("📊 Overview")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Hospitals", len(hospital_scores))
        
        with col2:
            avg_score = hospital_scores['overall_average'].mean()
            st.metric("Average Satisfaction", f"{avg_score:.2f}")
        
        with col3:
            top_score = hospital_scores['overall_average'].max()
            st.metric("Highest Score", f"{top_score:.2f}")
        
        with col4:
            low_score = hospital_scores['overall_average'].min()
            st.metric("Lowest Score", f"{low_score:.2f}")
        
        st.markdown("---")
        
        # Overall satisfaction distribution
        st.subheader("Overall Satisfaction Score Distribution")
        fig = px.histogram(
            hospital_scores,
            x='overall_average',
            nbins=20,
            title="Distribution of Hospital Overall Satisfaction Scores",
            labels={'overall_average': 'Overall Average Score', 'count': 'Number of Hospitals'},
            color_discrete_sequence=['#1f77b4']
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, width='stretch')
        
        # Top and bottom hospitals
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Top 5 Hospitals")
            top5 = hospital_scores.head(5)[['code_hospital', 'overall_average']]
            top5['overall_average'] = top5['overall_average'].round(2)
            st.dataframe(top5, hide_index=True, width='stretch')
        
        with col2:
            st.subheader("⚠️ Bottom 5 Hospitals")
            bottom5 = hospital_scores.tail(5)[['code_hospital', 'overall_average']].sort_values('overall_average')
            bottom5['overall_average'] = bottom5['overall_average'].round(2)
            st.dataframe(bottom5, hide_index=True, width='stretch')
    
    # Hospital Comparison Page
    elif page == "Hospital Comparison":
        st.header("🏥 Hospital Comparison")
        
        # Hospital selector
        hospitals = hospital_scores['code_hospital'].tolist()
        selected_hospitals = st.multiselect(
            "Select hospitals to compare (max 5):",
            hospitals,
            default=hospitals[:3] if len(hospitals) >= 3 else hospitals,
            max_selections=5
        )
        
        if selected_hospitals:
            filtered = hospital_scores[hospital_scores['code_hospital'].isin(selected_hospitals)]
            
            # Overall average comparison
            st.subheader("Overall Average Score Comparison")
            fig = px.bar(
                filtered.sort_values('overall_average', ascending=False),
                x='code_hospital',
                y='overall_average',
                title="Overall Satisfaction Score by Hospital",
                labels={'code_hospital': 'Hospital', 'overall_average': 'Average Score'},
                color='overall_average',
                color_continuous_scale='RdYlGn',
                text='overall_average'
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, width='stretch')
            
            # Question-level comparison
            st.subheader("Question-Level Scores")
            
            # Get question columns (q* columns)
            q_cols = [col for col in filtered.columns if col.startswith('q') and col not in ['code_hospital']]
            
            if q_cols:
                selected_questions = st.multiselect(
                    "Select questions to compare:",
                    q_cols[:10],  # Show first 10 by default
                    default=q_cols[:5] if len(q_cols) >= 5 else q_cols
                )
                
                if selected_questions:
                    # Prepare data for visualization
                    comparison_data = []
                    for _, row in filtered.iterrows():
                        for q in selected_questions:
                            if pd.notna(row[q]):
                                comparison_data.append({
                                    'Hospital': str(row['code_hospital']),
                                    'Question': q,
                                    'Score': row[q]
                                })
                    
                    comp_df = pd.DataFrame(comparison_data)
                    
                    fig = px.bar(
                        comp_df,
                        x='Question',
                        y='Score',
                        color='Hospital',
                        barmode='group',
                        title="Question Scores by Hospital",
                        labels={'Score': 'Average Score'}
                    )
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, width='stretch')
        else:
            st.info("Please select at least one hospital to display comparison.")
    
    # Question Analysis Page
    elif page == "Question Analysis":
        st.header("📋 Question Analysis")
        
        # Get question columns from hospital_scores
        q_cols = [col for col in hospital_scores.columns if col.startswith('q') and col not in ['code_hospital']]
        
        if q_cols:
            # Question selector
            selected_question = st.selectbox("Select a question to analyze:", q_cols)
            
            # Get question text if available
            q_num = int(''.join(filter(str.isdigit, selected_question.split('_')[0])))
            q_text_row = question_texts[question_texts['question_number'] == q_num]
            
            if not q_text_row.empty:
                st.info(f"**Question {q_num}:** {q_text_row.iloc[0]['question_text']}")
            
            # Statistics
            col1, col2, col3, col4 = st.columns(4)
            scores = hospital_scores[selected_question].dropna()
            
            with col1:
                st.metric("Mean", f"{scores.mean():.2f}")
            with col2:
                st.metric("Median", f"{scores.median():.2f}")
            with col3:
                st.metric("Std Dev", f"{scores.std():.2f}")
            with col4:
                st.metric("Count", len(scores))
            
            st.markdown("---")
            
            # Distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Score Distribution")
                fig = px.histogram(
                    scores,
                    nbins=20,
                    title=f"Distribution of {selected_question}",
                    labels={'value': 'Score', 'count': 'Frequency'},
                    color_discrete_sequence=['#2ecc71']
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                st.subheader("Box Plot")
                fig = px.box(
                    y=scores,
                    title=f"Box Plot of {selected_question}",
                    labels={'y': 'Score'},
                    color_discrete_sequence=['#3498db']
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, width='stretch')
            
            # Top and bottom performers
            st.subheader("Hospital Performance on This Question")
            question_scores = hospital_scores[['code_hospital', selected_question]].dropna()
            question_scores = question_scores.sort_values(selected_question, ascending=False)
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Top 10 Hospitals**")
                top10 = question_scores.head(10).copy()
                top10[selected_question] = top10[selected_question].round(2)
                st.dataframe(top10, hide_index=True, width='stretch')
            
            with col2:
                st.write("**Bottom 10 Hospitals**")
                bottom10 = question_scores.tail(10).copy()
                bottom10[selected_question] = bottom10[selected_question].round(2)
                st.dataframe(bottom10, hide_index=True, width='stretch')
        else:
            st.warning("No question columns found in the hospital_scores table.")
    
    # Data Explorer Page
    elif page == "Data Explorer":
        st.header("🔍 Data Explorer")
        
        st.subheader("Raw Data Sample")
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            num_rows = st.slider("Number of rows to display:", 10, 100, 20)
        with col2:
            search_col = st.selectbox("Search in column:", [''] + list(main_data.columns))
        
        if search_col:
            search_val = st.text_input(f"Filter {search_col} (contains):")
            if search_val:
                filtered_data = main_data[main_data[search_col].astype(str).str.contains(search_val, case=False, na=False)]
                st.write(f"Showing {len(filtered_data)} matching rows")
                st.dataframe(filtered_data.head(num_rows), width='stretch')
            else:
                st.dataframe(main_data.head(num_rows), width='stretch')
        else:
            st.dataframe(main_data.head(num_rows), width='stretch')
        
        # Data info
        st.subheader("Dataset Information")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Rows", len(main_data))
        with col2:
            st.metric("Total Columns", len(main_data.columns))
        with col3:
            memory_mb = main_data.memory_usage(deep=True).sum() / 1024 / 1024
            st.metric("Memory Usage", f"{memory_mb:.2f} MB")
        
            # Column info
        with st.expander("View Column Details"):
            col_info = pd.DataFrame({
                'Column': main_data.columns,
                'Type': [str(dtype) for dtype in main_data.dtypes.values],
                'Non-Null Count': main_data.count().values,
                'Null Count': main_data.isnull().sum().values,
                'Unique Values': [main_data[col].nunique() for col in main_data.columns]
            })
            st.dataframe(col_info, width='stretch', hide_index=True)    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info(
        "**Data Source:** PostgreSQL Database\n\n"
        "**Tables:** satisfaction_2016_cleaned, hospital_scores, question_texts\n\n"
        "**Refresh:** Data is cached for 10 minutes"
    )


if __name__ == "__main__":
    main()
