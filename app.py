import streamlit as st
import pandas as pd
import plotly.express as px

# --- Page Configuration ---
st.set_page_config(
    page_title="Financial Report Visualizer",
    page_icon="📊",
    layout="wide"
)

# --- Helper Functions ---
@st.cache_data
def load_data(file_path):
    """Loads the financial data from a CSV file and preprocesses it."""
    try:
        df = pd.read_csv(file_path)
        # Basic data type conversion
        df['filing_period_end_date'] = pd.to_datetime(df['filing_period_end_date'])
        df['date_filed'] = pd.to_datetime(df['date_filed'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Handle potential missing or non-string table descriptions
        df['table_description'] = df['table_description'].fillna('General').astype(str)

        df.dropna(subset=['value'], inplace=True)
        return df
    except FileNotFoundError:
        return None
    except KeyError:
        st.error("The CSV file is missing the required 'table_description' column. Please update the file.")
        return None


def calculate_q4_data(df):
    """
    Calculates Q4 data for each metric using the formula: Q4 = 10-K - (Q1+Q2+Q3).
    Appends the calculated Q4 data back to the DataFrame.
    """
    df_with_q4 = df.copy()
    new_rows = []

    # Create a temporary year column for grouping
    df['year'] = df['filing_period_end_date'].dt.year
    grouped = df.groupby(['year', 'metric'])

    for (year, metric), group in grouped:
        k_reports = group[group['form_type'] == '10-K']
        q_reports = group[group['form_type'] == '10-Q'].sort_values(by='filing_period_end_date')

        # We need a full year's worth of data: one 10-K and three 10-Qs
        if len(k_reports) == 1 and len(q_reports) == 3:
            k_report_value = k_reports['value'].iloc[0]
            q_total_value = q_reports['value'].sum()
            
            q4_value = k_report_value - q_total_value
            
            k_report_row = k_reports.iloc[0].to_dict()
            k_report_row['value'] = q4_value
            k_report_row['form_type'] = '10-Q (Calculated)' 
            
            new_rows.append(k_report_row)

    if new_rows:
        q4_df = pd.DataFrame(new_rows)
        df_with_q4 = pd.concat([df_with_q4, q4_df], ignore_index=True)
    
    # Clean up the temporary 'year' column
    df_with_q4 = df_with_q4.drop(columns=['year'], errors='ignore')
    df = df.drop(columns=['year'], errors='ignore')
    
    return df_with_q4.sort_values(by='filing_period_end_date').reset_index(drop=True)

# --- Main Application ---
st.title("📊 Financial Report Visualizer")
st.write("An interactive tool to visualize historical financial data from SEC filings.")

DATA_FILE = 'AMD_financial_data_parallel.csv'
df_initial = load_data(DATA_FILE)

if df_initial is None:
    st.warning(f"Could not load data. Please check for an error message above.")
else:
    # --- Calculate Q4 data and add it to the DataFrame ---
    df = calculate_q4_data(df_initial)

    # --- Sidebar for User Selections ---
    st.sidebar.header("Chart Controls")
    ticker = df['symbol'].iloc[0]
    st.sidebar.markdown(f"**Company:** `{ticker}`")

    # --- Report Type lter (Toggle) ---
    report_type = st.sidebar.radio(
        "Select Report Type:",
        ('All', 'Quarterly', 'Annual'),
        key='report_type_toggle'
    )

    # --- Filter data based on the selected report type ---
    if report_type == 'Quarterly':
        filtered_df = df[df['form_type'].str.contains('10-Q', na=False)].copy()
    elif report_type == 'Annual':
        filtered_df = df[df['form_type'] == '10-K'].copy()
    else: # 'All'
        filtered_df = df.copy()

    # --- Dynamic Metric Selection (Grouped by Table Description) ---
    if filtered_df.empty:
        st.warning(f"No data available for the selected report type: **{report_type}**.")
    else:
        st.sidebar.markdown("---")
        st.sidebar.subheader("Select Metrics to Plot")

        # Define some preferred metrics to be selected by default
        preferred_defaults = ['Revenues', 'CostOfGoodsAndServicesSold', 'GrossProfit', 'Assets', 'LiabilitiesAndStockholdersEquity']
        
        selected_metrics = []
        
        # Get unique table descriptions to create separate selection groups
        table_descriptions = sorted(filtered_df['table_description'].unique())

        for table_desc in table_descriptions:
            # Use an expander for each table to keep the UI clean
            with st.sidebar.expander(f"{table_desc}", expanded=True):
                metrics_in_table = sorted(filtered_df[filtered_df['table_description'] == table_desc]['metric'].unique())
                
                # Determine which of the preferred defaults apply to this table
                defaults_for_this_table = [m for m in preferred_defaults if m in metrics_in_table]
                
                # Create a multiselect for the current group of metrics
                selections = st.multiselect(
                    label=f"Metrics from {table_desc}",
                    options=metrics_in_table,
                    default=defaults_for_this_table,
                    key=f"multi_{table_desc.replace(' ', '_')}", # Unique key for each multiselect
                    label_visibility="collapsed" # Hide the label as expander title is enough
                )
                selected_metrics.extend(selections)

        # --- Data Filtering and Chart Creation ---
        if not selected_metrics:
            st.warning("Please select at least one metric from the sidebar to display the chart.")
        else:
            plot_df = filtered_df[filtered_df['metric'].isin(selected_metrics)].copy()
            
            st.header(f"{report_type} Financial Metrics Over Time")

            fig = px.bar(
                plot_df,
                x='filing_period_end_date',
                y='value',
                color='metric',
                barmode='group',
                title=f"Historical Financial Performance for {ticker}",
                labels={
                    "filing_period_end_date": "Report Period End Date",
                    "value": "Value (USD)",
                    "metric": "Financial Metric"
                },
                hover_data={
                    "value": ":$,.2f",
                    "form_type": True,
                    "date_filed": True
                }
            )

            fig.update_layout(
                legend_title_text='Metrics',
                xaxis_title="Date",
                yaxis_title="Value (in billions USD)",
                hovermode="x unified"
            )
            fig.update_yaxes(tickprefix="$", tickformat=",.2s")
            
            st.plotly_chart(fig, use_container_width=True)

            if st.checkbox("Show Raw Data for Selection"):
                st.dataframe(plot_df[['filing_period_end_date', 'table_description', 'metric', 'value', 'form_type', 'date_filed']])