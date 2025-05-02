# DealGenie

**Automated parcel + zoning data pipeline**  
Prototype MVP for clipping county parcels to city limits and tagging them with zoning attributes.

---

## 🚀 Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/samantharoseeg/dealgenie.git
cd dealgenie

# 2. Create & activate a virtual environment
python3 -m venv venv
source venv/bin/activate      # macOS/Linux
# venv\Scripts\activate.bat   # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the pipeline
python scraper/clip_parcels.py
python scraper/overlay_zoning.py
# dealgenie