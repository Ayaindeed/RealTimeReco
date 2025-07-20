#!/usr/bin/env python3
"""
Streamlit Web Interface for Real-Time Content Recommendation System
"""
import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Configuration
API_BASE_URL = "http://localhost:8000"

# Page configuration
st.set_page_config(
    page_title="Content Recommendation System",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .recommendation-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

def make_api_request(endpoint, method="GET", data=None):
    """Make API request with error handling"""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=data)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error {response.status_code}: {response.text}")
            return None
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API. Make sure the FastAPI service is running.")
        return None
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

def main():
    # Main header
    st.markdown('<h1 class="main-header">Real-Time Content Recommendation System</h1>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("Controls")
    
    # Check API status
    api_status = make_api_request("/")
    if api_status:
        st.sidebar.success("API Connected")
        st.sidebar.json(api_status)
    else:
        st.sidebar.error("API Disconnected")
        st.stop()
    
    # Navigation
    page = st.sidebar.selectbox(
        "Choose Page",
        ["Dashboard", "User Recommendations", "System Analytics", "Content Explorer", "Algorithm Comparison"]
    )
    
    if page == "Dashboard":
        show_dashboard()
    elif page == "User Recommendations":
        show_user_recommendations()
    elif page == "System Analytics":
        show_system_analytics()
    elif page == "Content Explorer":
        show_content_explorer()
    elif page == "Algorithm Comparison":
        show_algorithm_comparison()

def show_dashboard():
    """Main dashboard overview"""
    st.header("System Overview")
    
    # Get system stats
    stats = make_api_request("/stats")
    if not stats:
        return
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Users",
            value=f"{stats['total_users']:,}",
            delta="Active users in system"
        )
    
    with col2:
        st.metric(
            label="Content Items",
            value=f"{stats['total_items']:,}",
            delta="Available content"
        )
    
    with col3:
        st.metric(
            label="Matrix Sparsity",
            value=f"{stats['sparsity_percent']:.1f}%",
            delta="Typical for recommendation systems"
        )
    
    with col4:
        interactions = stats['matrix_shape'][0] * stats['matrix_shape'][1] * (100 - stats['sparsity_percent']) / 100
        st.metric(
            label="Total Interactions",
            value=f"{int(interactions):,}",
            delta="User-content interactions"
        )
    
    # System architecture diagram
    st.subheader("System Architecture")
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("""
        **Data Flow:**
        1. Kafka streams user events
        2. HBase stores user profiles
        3. Collaborative filtering engine
        4. FastAPI serves recommendations
        5. Streamlit web interface
        """)
    
    with col2:
        # Matrix visualization
        fig = go.Figure(data=go.Heatmap(
            z=[[1, 0.8, 0, 0.5], [0.3, 1, 0.7, 0], [0, 0.2, 1, 0.9], [0.6, 0, 0.4, 1]],
            colorscale='Blues',
            showscale=False
        ))
        fig.update_layout(
            title="User-Item Interaction Matrix (Sample)",
            xaxis_title="Content Items",
            yaxis_title="Users",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)

def show_user_recommendations():
    """User recommendation interface"""
    st.header("Get Personalized Recommendations")
    
    # Get users list
    users_data = make_api_request("/users?limit=50")
    if not users_data:
        return
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # User selection
        selected_user = st.selectbox(
            "Select User ID:",
            users_data['users'],
            help="Choose a user to get recommendations for"
        )
        
        # Algorithm selection
        algorithm = st.selectbox(
            "Recommendation Algorithm:",
            ["hybrid", "user_based", "item_based"],
            help="Choose the collaborative filtering method"
        )
        
        # Number of recommendations
        num_recs = st.slider(
            "Number of Recommendations:",
            min_value=1,
            max_value=20,
            value=5,
            help="How many recommendations to generate"
        )
        
        # Get recommendations button
        if st.button("Get Recommendations", type="primary"):
            get_user_recommendations(selected_user, algorithm, num_recs)
    
    with col2:
        # Show user profile
        if selected_user:
            show_user_profile(selected_user)

def get_user_recommendations(user_id, method, num_recs):
    """Fetch and display recommendations"""
    request_data = {
        "user_id": user_id,
        "method": method,
        "n_recommendations": num_recs
    }
    
    with st.spinner("Generating recommendations..."):
        recommendations = make_api_request("/recommend", "POST", request_data)
    
    if recommendations:
        st.success(f"Found {recommendations['total_found']} recommendations using {recommendations['method']} method")
        
        # Display recommendations
        for i, rec in enumerate(recommendations['recommendations'], 1):
            with st.container():
                st.markdown(f"""
                <div class="recommendation-card">
                    <h4>#{i} {rec['content_id']}</h4>
                    <p><strong>Predicted Rating:</strong> {rec['predicted_rating']:.3f}/5.0</p>
                    <p><small>{rec['recommendation_reason']}</small></p>
                </div>
                """, unsafe_allow_html=True)

def show_user_profile(user_id):
    """Display user interaction profile"""
    st.subheader(f"Profile: {user_id}")
    
    profile = make_api_request(f"/user/{user_id}/profile")
    if profile:
        st.metric("Total Interactions", profile['total_interactions'])
        
        if profile['interactions']:
            # Create DataFrame for interactions
            df = pd.DataFrame(profile['interactions'])
            
            # Interaction histogram
            fig = px.histogram(
                df, 
                x='implicit_rating', 
                title="User's Rating Distribution",
                labels={'implicit_rating': 'Implicit Rating', 'count': 'Frequency'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Recent interactions table
            st.subheader("Recent Interactions")
            st.dataframe(df, use_container_width=True)

def show_system_analytics():
    """System analytics and metrics"""
    st.header("System Analytics")
    
    # Get system stats
    stats = make_api_request("/stats")
    if not stats:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Matrix sparsity visualization
        sparsity = stats['sparsity_percent']
        density = 100 - sparsity
        
        fig = go.Figure(data=[
            go.Pie(
                labels=['Sparse (No Interaction)', 'Dense (Has Interaction)'],
                values=[sparsity, density],
                hole=.3,
                marker_colors=['lightgray', '#1f77b4']
            )
        ])
        fig.update_layout(title="User-Item Matrix Density")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # System metrics bar chart
        metrics_data = {
            'Metric': ['Users', 'Content Items', 'Interactions (K)'],
            'Count': [stats['total_users'], stats['total_items'], 
                     int((stats['matrix_shape'][0] * stats['matrix_shape'][1] * density / 100) / 1000)]
        }
        
        fig = px.bar(
            metrics_data, 
            x='Metric', 
            y='Count',
            title="System Scale Metrics",
            color='Metric'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Performance insights
    st.subheader("System Performance Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"""
        **Matrix Efficiency**
        - Sparsity: {sparsity:.1f}%
        - This is excellent for collaborative filtering
        - Typical range: 95-99.9%
        """)
    
    with col2:
        st.success(f"""
        **User Coverage**
        - {stats['total_users']:,} users with interactions
        - Rich user behavior data
        - Good for personalization
        """)
    
    with col3:
        st.warning(f"""
        **Content Catalog**
        - {stats['total_items']:,} unique content items
        - Diverse content portfolio
        - Room for content-based filtering
        """)

def show_content_explorer():
    """Content exploration interface"""
    st.header("Content Explorer")
    
    # Get sample content stats
    sample_content = ["content_0001", "content_0002", "content_0003", "content_0004", "content_0005"]
    
    selected_content = st.selectbox("Select Content ID:", sample_content)
    
    if st.button("Get Content Stats"):
        content_stats = make_api_request(f"/content/{selected_content}/stats")
        
        if content_stats:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Interactions", content_stats['total_interactions'])
                st.metric("Average Rating", f"{content_stats['average_rating']:.2f}")
            
            with col2:
                if content_stats['interacted_users']:
                    st.subheader("Sample Users Who Interacted")
                    for user in content_stats['interacted_users']:
                        st.write(f"• {user}")

def show_algorithm_comparison():
    """Compare different recommendation algorithms"""
    st.header("Algorithm Comparison")
    
    # Get users for testing
    users_data = make_api_request("/users?limit=10")
    if not users_data:
        return
    
    test_user = st.selectbox("Select Test User:", users_data['users'])
    
    if st.button("Compare Algorithms"):
        methods = ["user_based", "item_based", "hybrid"]
        results = {}
        
        # Get recommendations from all methods
        for method in methods:
            request_data = {
                "user_id": test_user,
                "method": method,
                "n_recommendations": 5
            }
            
            with st.spinner(f"Testing {method} method..."):
                result = make_api_request("/recommend", "POST", request_data)
                if result:
                    results[method] = result
        
        # Display comparison
        if results:
            st.subheader("Algorithm Comparison Results")
            
            for method, result in results.items():
                st.write(f"**{method.replace('_', ' ').title()} Method:**")
                
                # Create DataFrame for this method's results
                df = pd.DataFrame(result['recommendations'])
                
                # Display top 3 recommendations
                for i, rec in enumerate(df.head(3).itertuples(), 1):
                    st.write(f"{i}. {rec.content_id} (Score: {rec.predicted_rating:.3f})")
                
                st.write("---")
            
            # Comparison chart
            comparison_data = []
            for method, result in results.items():
                for rec in result['recommendations'][:3]:
                    comparison_data.append({
                        'Method': method.replace('_', ' ').title(),
                        'Content': rec['content_id'],
                        'Score': rec['predicted_rating']
                    })
            
            if comparison_data:
                df_comparison = pd.DataFrame(comparison_data)
                fig = px.bar(
                    df_comparison,
                    x='Content',
                    y='Score',
                    color='Method',
                    title="Top Recommendations by Algorithm",
                    barmode='group'
                )
                st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
