# Zepto Quick Commerce Analytics Dashboard

A comprehensive data analysis and visualization project for Zepto's quick commerce operations using PySpark and Streamlit.

## � Quick Links

- **📦 GitHub Repository:** https://github.com/gsv3245-cpu/Zepto-Analysis
- **🎨 Live Dashboard:** https://zepto-analysis-g2mxwsxnubgszauqlgsmp4.streamlit.app/
- **📖 Full Documentation:** See [Running the Dashboard](#-getting-started) section

## �📊 Project Overview

This project analyzes 120,000+ orders from Zepto's quick commerce platform across 5 major Indian cities (Mumbai, Delhi, Bangalore, Hyderabad, Chennai) over a 15-day period in January 2025.

**Key Areas Analyzed:**
- 🏙️ City Performance & Delivery SLA
- 📦 Inventory & Category Demand
- 🚚 Delivery Speed & Distance Correlation
- ⏱️ Peak Hour Impact Analysis
- 💳 Payment Methods & Revenue Distribution

## 🛠️ Tech Stack

- **Data Processing:** PySpark 4.0
- **Data Manipulation:** Pandas, NumPy
- **Visualization:** Plotly, Matplotlib
- **Dashboard:** Streamlit
- **Data Format:** Excel (.xlsx)

## 📋 Project Structure

```
BDCCT_Kavita/
├── BDCCT_Individual_Final.ipynb      # Main data processing notebook
├── zepto_dashboard_app.py             # Streamlit dashboard app
├── zepto_dataset_v2.xlsx              # Dataset (120K rows)
├── requirements.txt                   # Python dependencies
├── README.md                          # This file
└── .gitignore                         # Git ignore file
```

## 📈 Data Pipeline

### Step 1-5: Setup & Data Import
- Install dependencies
- Load Excel dataset (zepto_dataset_v2.xlsx)
- Create Spark session
- Convert to Spark DataFrame

### Step 6-12: Data Exploration & Cleaning
- Schema exploration
- **Null value handling:** City-wise and category-wise median imputation
- **Outlier detection:** Remove negative prices, cap delivery times >90min, cap quantity >15
- **Duplicate removal:** Dropduplicates()
- **Data type casting:** Ensure correct types

### Step 13-25: Feature Engineering & Aggregation
- Delivery speed classification (Fast/Normal/Delayed)
- Peak hour flagging (7-9am & 6-10pm)
- Revenue bands & payment method categorization
- City-wise, category-wise, and city×category aggregations

### Step 26: Dashboard Visualization
- 5 interactive tabs with drill-down capabilities
- Real-time filtering by city, category, SLA thresholds
- KPI cards, charts, heatmaps, and raw data explorer

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Java Runtime (for PySpark)

### Installation

```bash
# Clone this repository
git clone https://github.com/yourusername/BDCCT_Kavita.git
cd BDCCT_Kavita

# Install dependencies
pip install -r requirements.txt
```

### Running the Dashboard

```bash
# Local execution
streamlit run zepto_dashboard_app.py

# Google Colab execution
!pip install streamlit plotly -q
!streamlit run zepto_dashboard_app.py &
# Then use ngrok for public URL
```

## 🌐 Deployment

### Deploy to Streamlit Cloud (Recommended)

The easiest way to deploy this dashboard for free:

1. **Create Streamlit Cloud Account**
   - Go to https://share.streamlit.io/
   - Sign up with your GitHub account

2. **Deploy Your Repository**
   - Click "New app" 
   - Select GitHub repo: `https://github.com/gsv3245-cpu/Zepto-Analysis`
   - Set main file: `zepto_dashboard_app.py`
   - Click "Deploy"

3. **Your Dashboard Is Now Live**
   ```
   https://zepto-analysis-g2mxwsxnubgszauqlgsmp4.streamlit.app/
   ```

**Note:** The dashboard auto-updates whenever you push changes to GitHub!

### Alternative Deployment Options

**Heroku / Railway / AWS / GCP:** See [Advanced Usage](#-advanced-usage) section

## 📊 Key Findings

### City Performance
- **Best Performer:** Bangalore with 14.2 min avg delivery & 3.8★ rating
- **Challenge:** Delhi with 18.5 min delivery time (+6.2 min vs SLA)
- **Revenue Leader:** Mumbai with highest revenue density

### Inventory Insights
- Staples & Dairy: 45% of total revenue
- Snacks: Highest unit volume but lower per-order value
- Opportunity: Flash discount campaigns for Snacks category

### Peak Hour Analysis
- Peak hours (7-9am, 6-10pm): ~33% of daily orders
- Delivery time increases by +4.2 min during peak
- Cancellation rates remain stable (price insensitivity)
- Recommendation: Pre-position inventory before 7am & 5pm

### Delivery Metrics
- Strong correlation between distance & delivery time
- Mumbai: Efficient dark store placement (tight clustering)
- Delhi: Operational inconsistency (wider scatter)

## 📊 Dashboard Features

### Tab 1: City Performance
- Average delivery time by city (with SLA threshold)
- Cancellation rate trends
- Customer ratings radar chart
- City × Category revenue heatmap

### Tab 2: Inventory & Demand
- Category demand visualization (Bar/Treemap/Sunburst charts)
- Discount vs Revenue bubble chart
- Payment method distribution
- Order value band analysis

### Tab 3: Delivery Deep-Dive
- Delivery speed distribution (Fast/Normal/Delayed)
- Distance vs delivery time scatter plot
- Cancellation heatmap by city & category
- Box plot analysis

### Tab 4: Time & Peak Analysis
- Hourly order volume trends
- Peak vs Non-peak metrics comparison
- Daily order pattern (15-day window)
- Peak hour insights

### Tab 5: Raw Data Explorer
- Interactive filters (status, payment, delivery time, rating)
- Download filtered data as CSV
- Quick stats on filtered dataset

## 🎨 Customization

### Adjust Thresholds
Edit in the Streamlit sidebar:
- **SLA Threshold:** Default 30 min (range: 15-45 min)
- **Cancellation Alert:** Default 15% (range: 5-25%)
- **Chart Height:** Default 360px (options: 300, 360, 420, 480)

### Toggle Options
- Show/hide value labels on charts
- Show/hide SLA reference lines
- Normalize to percentages

## 📁 Data Requirements

The dashboard expects:
- `zepto_dataset_v2.xlsx` with columns:
  - `order_id`, `customer_id`, `order_time`, `city`, `area`, `product_name`
  - `category`, `quantity`, `price_per_unit`, `discount`, `final_price`
  - `delivery_time_minutes`, `distance_km`, `order_status`, `rating`, `payment_method`
  - `peak_hour_flag`, `delivery_partner_id`, `warehouse_id`

## 🔧 Advanced Usage

### Google Colab Execution
```python
# Upload zepto_dataset_v2.xlsx to Colab

# Install and run notebook
!pip install pyspark openpyxl streamlit plotly -q

# Execute all cells sequentially (Steps 1-25)

# Run dashboard with ngrok
!pip install pyspark-streaming
!streamlit run zepto_dashboard_app.py &

from pyngrok import ngrok
public_url = ngrok.connect(8501)
print(f"Dashboard: {public_url}")
```

## 📝 Data Cleaning & Transformation

### Transformations Applied
1. **Delivery Speed:** 3-tier classification based on time buckets
2. **Revenue Bands:** Three price segments (Low/Mid/High)
3. **Peak Hours:** Binary flag for rush periods (7-9am, 6-10pm)
4. **Normalized Metrics:** Radar chart normalization (0-1 scale)

### Missing Value Strategy
- Numeric: Group-level (city/category) median imputation
- Categorical: Mode or "Unknown" placeholder

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add improvement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create a Pull Request

## 📄 License

This project is open source and available under the MIT License.

## 👨‍💼 Author

**BDCCT Individual Project - FA2**
- Dataset: Zepto Quick Commerce (120K sample orders)
- Analysis Period: Jan 1-15, 2025
- Cities: Mumbai, Delhi, Bangalore, Hyderabad, Chennai

## 🙋 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing documentation
- Review sample queries in the notebook

---

**Last Updated:** April 2025
**Status:** Production Ready ✅
