import pandas as pd
import numpy as np
import json
from typing import Dict, Any, Tuple
import warnings
warnings.filterwarnings('ignore')


class DealGenieScorer:
    """Comprehensive property scoring engine for development potential analysis"""
    
    def __init__(self, config_path: str = None):
        """Initialize scorer with default or custom configuration"""
        if config_path:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = self.get_default_config()
    
    def get_default_config(self) -> Dict[str, Any]:
        """Default scoring configuration"""
        return {
            "weights": {
                "zoning_score": 0.35,
                "lot_size_score": 0.25,
                "transit_bonus": 0.20,
                "financial_score": 0.10,
                "risk_penalty": 0.10
            },
            "zoning_scores": {
                # High density residential
                "R5": 100, "RAS4": 95, "RAS3": 92, "R4": 90,
                # Medium-high density
                "RD3": 85, "RD2.5": 82, "RD2": 80, "R3": 75, "RD1.5": 72, "RD1": 70,
                # Commercial zones
                "C4": 95, "C2": 90, "CM": 85, "C1.5": 82, "C1": 80,
                # Industrial (light)
                "M2": 75, "M1": 70, "MR1": 72, "MR2": 74,
                # Low density residential
                "R2": 50, "RD5": 48, "RD4": 45, "RD6": 43,
                "R1": 40, "RS": 38, "RE": 35, "RA": 32,
                # Special zones
                "PF": 25, "OS": 20, "A1": 15, "A2": 15
            },
            "lot_size_thresholds": [
                {"min": 20000, "max": float('inf'), "score_range": [90, 100]},
                {"min": 10000, "max": 20000, "score_range": [80, 90]},
                {"min": 7000, "max": 10000, "score_range": [70, 80]},
                {"min": 5000, "max": 7000, "score_range": [60, 70]},
                {"min": 3000, "max": 5000, "score_range": [40, 60]},
                {"min": 0, "max": 3000, "score_range": [20, 40]}
            ],
            "transit_bonuses": {
                "toc_eligible": 20,
                "high_quality_transit": 15,
                "opportunity_corridor": 10,
                "tier_1": 25,
                "tier_2": 20,
                "tier_3": 15,
                "tier_4": 10
            },
            "financial_bonuses": {
                "high_land_ratio": {"threshold": 0.70, "bonus": 10},
                "low_far": {"threshold": 0.50, "bonus": 5},
                "very_low_far": {"threshold": 0.25, "bonus": 8}
            },
            "risk_penalties": {
                "overlay_per_zone": 3,
                "methane_zone": 5,
                "methane_buffer": 3,
                "historic_zone": 8,
                "fault_zone": 4,
                "flood_zone": 3,
                "very_high_fire": 6
            },
            "investment_tiers": {
                "A": {"min": 80, "description": "Prime Development Opportunity"},
                "B": {"min": 65, "description": "Strong Development Potential"},
                "C": {"min": 50, "description": "Moderate Development Potential"},
                "D": {"min": 0, "description": "Limited Development Potential"}
            }
        }
    
    def calculate_zoning_score(self, base_zoning: str) -> float:
        """Calculate zoning score based on development potential"""
        if pd.isna(base_zoning):
            return 30.0
        
        base_zoning = str(base_zoning).upper().strip()
        
        # Direct lookup
        if base_zoning in self.config["zoning_scores"]:
            return float(self.config["zoning_scores"][base_zoning])
        
        # Try to match partial patterns
        for zone_pattern, score in self.config["zoning_scores"].items():
            if base_zoning.startswith(zone_pattern):
                return float(score)
        
        # Default score for unknown zones
        return 35.0
    
    def calculate_lot_size_score(self, lot_size_sqft: float) -> float:
        """Calculate lot size score based on development potential"""
        if pd.isna(lot_size_sqft) or lot_size_sqft <= 0:
            return 20.0
        
        for threshold in self.config["lot_size_thresholds"]:
            if threshold["min"] <= lot_size_sqft < threshold["max"]:
                # Linear interpolation within range
                score_min, score_max = threshold["score_range"]
                if threshold["max"] == float('inf'):
                    return min(100, score_max)
                
                # Calculate position within range
                range_size = threshold["max"] - threshold["min"]
                position = (lot_size_sqft - threshold["min"]) / range_size
                return score_min + (score_max - score_min) * position
        
        return 30.0
    
    def calculate_transit_bonus(self, row: pd.Series) -> float:
        """Calculate transit and development incentive bonuses"""
        bonus = 0
        bonuses = self.config["transit_bonuses"]
        
        # TOC eligibility
        if row.get('toc_eligible', False):
            bonus += bonuses["toc_eligible"]
        
        # High quality transit
        if str(row.get('high_quality_transit', '')).lower() == 'yes':
            bonus += bonuses["high_quality_transit"]
        
        # Opportunity zones
        opp_zone = str(row.get('opportunity_zone', '')).lower()
        if 'oc-' in opp_zone or 'tier' in opp_zone:
            if 'oc-1' in opp_zone or 'tier 1' in opp_zone:
                bonus += bonuses["tier_1"]
            elif 'oc-2' in opp_zone or 'tier 2' in opp_zone:
                bonus += bonuses["tier_2"]
            elif 'oc-3' in opp_zone or 'tier 3' in opp_zone:
                bonus += bonuses["tier_3"]
            else:
                bonus += bonuses["opportunity_corridor"]
        elif opp_zone not in ['not eligible', 'none', '', 'nan']:
            bonus += bonuses["opportunity_corridor"]
        
        return min(bonus, 40)  # Cap at 40 points
    
    def calculate_financial_score(self, row: pd.Series) -> float:
        """Calculate financial indicators score"""
        score = 0
        bonuses = self.config["financial_bonuses"]
        
        # Land value ratio (high = redevelopment opportunity)
        if pd.notna(row.get('assessed_land_value')) and pd.notna(row.get('total_assessed_value')):
            if row['total_assessed_value'] > 0:
                land_ratio = row['assessed_land_value'] / row['total_assessed_value']
                if land_ratio >= bonuses["high_land_ratio"]["threshold"]:
                    score += bonuses["high_land_ratio"]["bonus"]
        
        # FAR calculation (low = underbuilt)
        if pd.notna(row.get('building_size_sqft')) and pd.notna(row.get('lot_size_sqft')):
            if row['lot_size_sqft'] > 0:
                far = row['building_size_sqft'] / row['lot_size_sqft']
                if far <= bonuses["very_low_far"]["threshold"]:
                    score += bonuses["very_low_far"]["bonus"]
                elif far <= bonuses["low_far"]["threshold"]:
                    score += bonuses["low_far"]["bonus"]
        
        return min(score, 15)  # Cap at 15 points
    
    def calculate_risk_penalty(self, row: pd.Series) -> float:
        """Calculate risk penalties from constraints and overlays"""
        penalty = 0
        penalties = self.config["risk_penalties"]
        
        # Overlay zones
        overlay_count = row.get('overlay_count', 0)
        if pd.notna(overlay_count):
            penalty += overlay_count * penalties["overlay_per_zone"]
        
        # Environmental hazards
        methane = str(row.get('methane_zone', '')).lower()
        if 'methane zone' in methane:
            penalty += penalties["methane_zone"]
        elif 'buffer' in methane:
            penalty += penalties["methane_buffer"]
        
        # Historic preservation (check in specific plan or overlays)
        if pd.notna(row.get('specific_plan_area')):
            plan = str(row['specific_plan_area']).lower()
            if 'historic' in plan or 'hpoz' in plan:
                penalty += penalties["historic_zone"]
        
        # Fault zones
        if pd.notna(row.get('fault_zone')):
            fault = str(row['fault_zone']).lower()
            if fault not in ['', 'none', 'nan']:
                penalty += penalties["fault_zone"]
        
        # Flood zones
        flood = str(row.get('flood_zone', '')).lower()
        if 'flood' in flood and 'outside' not in flood:
            penalty += penalties["flood_zone"]
        
        return min(penalty, 20)  # Cap penalty at 20 points
    
    def suggest_use_case(self, row: pd.Series, total_score: float) -> str:
        """Suggest development use case based on property characteristics"""
        base_zoning = str(row.get('base_zoning', '')).upper()
        lot_size = row.get('lot_size_sqft', 0)
        toc = row.get('toc_eligible', False)
        land_ratio = 0
        
        if pd.notna(row.get('assessed_land_value')) and pd.notna(row.get('total_assessed_value')):
            if row['total_assessed_value'] > 0:
                land_ratio = row['assessed_land_value'] / row['total_assessed_value']
        
        # Large lot + high density zoning
        if lot_size >= 10000:
            if base_zoning in ['R3', 'R4', 'R5', 'RAS3', 'RAS4']:
                if toc:
                    return "Major TOC Development (100+ units)"
                return "Large Multi-Family Development"
            elif base_zoning in ['C2', 'C4', 'CM']:
                return "Mixed-Use Development"
            elif base_zoning in ['M1', 'M2']:
                return "Creative Office/Live-Work"
            elif land_ratio > 0.7:
                return "Prime Redevelopment Site"
        
        # Medium lots
        elif lot_size >= 5000:
            if base_zoning in ['R3', 'R4', 'RD2', 'RD3']:
                if toc:
                    return "TOC Multi-Family (20-50 units)"
                return "Small Multi-Family Development"
            elif base_zoning in ['C2', 'C4']:
                return "Retail/Restaurant Development"
            elif base_zoning in ['R1', 'R2'] and land_ratio > 0.65:
                return "SB9 Lot Split Opportunity"
        
        # Small lots
        elif lot_size >= 3000:
            if base_zoning in ['R3', 'RD1.5', 'RD2']:
                return "Small Lot Subdivision"
            elif base_zoning in ['C1', 'C2']:
                return "Boutique Retail/Office"
            elif base_zoning in ['R1'] and toc:
                return "ADU Development"
        
        # Zoning-specific suggestions
        if base_zoning in ['C2', 'C4', 'CM']:
            return "Commercial Development"
        elif base_zoning in ['R3', 'R4', 'R5'] and toc:
            return "Transit-Oriented Housing"
        elif land_ratio > 0.75:
            return "Teardown/Rebuild Opportunity"
        elif total_score >= 70:
            return "Development Opportunity"
        elif total_score >= 50:
            return "Value-Add Renovation"
        
        return "Hold/Income Property"
    
    def calculate_property_score(self, row: pd.Series) -> Tuple[float, Dict[str, float]]:
        """Calculate total development score for a property"""
        weights = self.config["weights"]
        
        # Calculate component scores
        scores = {
            "zoning_score": self.calculate_zoning_score(row.get('base_zoning')),
            "lot_size_score": self.calculate_lot_size_score(row.get('lot_size_sqft')),
            "transit_bonus": self.calculate_transit_bonus(row),
            "financial_score": self.calculate_financial_score(row),
            "risk_penalty": self.calculate_risk_penalty(row)
        }
        
        # Calculate weighted total (risk is subtracted)
        total_score = (
            scores["zoning_score"] * weights["zoning_score"] +
            scores["lot_size_score"] * weights["lot_size_score"] +
            scores["transit_bonus"] * weights["transit_bonus"] +
            scores["financial_score"] * weights["financial_score"] -
            scores["risk_penalty"] * weights["risk_penalty"]
        )
        
        # Normalize to 0-100 scale
        total_score = max(0, min(100, total_score))
        
        return total_score, scores
    
    def get_investment_tier(self, score: float) -> str:
        """Determine investment tier based on score"""
        for tier, criteria in self.config["investment_tiers"].items():
            if score >= criteria["min"]:
                return tier
        return "D"
    
    def score_properties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score all properties in the dataframe"""
        print("Scoring properties...")
        
        # Initialize new columns
        df['development_score'] = 0.0
        df['score_breakdown'] = ''
        df['suggested_use'] = ''
        df['investment_tier'] = ''
        
        # Score each property
        for idx, row in df.iterrows():
            score, breakdown = self.calculate_property_score(row)
            
            df.at[idx, 'development_score'] = round(score, 2)
            df.at[idx, 'score_breakdown'] = json.dumps(breakdown, default=str)
            df.at[idx, 'suggested_use'] = self.suggest_use_case(row, score)
            df.at[idx, 'investment_tier'] = self.get_investment_tier(score)
            
            if (idx + 1) % 100 == 0:
                print(f"  Scored {idx + 1}/{len(df)} properties...")
        
        print(f"✓ Scored all {len(df)} properties")
        return df


def main():
    """Main execution function"""
    print("="*100)
    print("DEALGENIE PROPERTY SCORING ENGINE")
    print("="*100)
    
    # Load cleaned data
    print("\n1. Loading cleaned property data...")
    df = pd.read_csv('clean_zimas_ready_for_scoring.csv')
    print(f"   Loaded {len(df)} properties")
    
    # Analyze property distribution
    print("\n2. Property Distribution by Zoning:")
    print("-"*50)
    zoning_dist = df['base_zoning'].value_counts().head(15)
    for zone, count in zoning_dist.items():
        print(f"   {zone}: {count} properties ({count/len(df)*100:.1f}%)")
    
    # Initialize scorer
    print("\n3. Initializing scoring engine...")
    scorer = DealGenieScorer()
    
    # Save config file
    config_file = 'scoring_config.json'
    with open(config_file, 'w') as f:
        json.dump(scorer.config, f, indent=2, default=str)
    print(f"   ✓ Config saved to {config_file}")
    
    # Score properties
    print("\n4. Calculating development scores...")
    df_scored = scorer.score_properties(df)
    
    # Export results
    output_file = 'scored_zimas_properties.csv'
    df_scored.to_csv(output_file, index=False)
    print(f"\n5. ✓ Scored results exported to {output_file}")
    
    # Generate analytics
    print("\n6. SUMMARY ANALYTICS")
    print("="*100)
    
    # Top 20 properties
    print("\nTOP 20 HIGHEST SCORING PROPERTIES:")
    print("-"*80)
    top_20 = df_scored.nlargest(20, 'development_score')[
        ['assessor_parcel_id', 'site_address', 'base_zoning', 
         'lot_size_sqft', 'development_score', 'suggested_use', 'investment_tier']
    ]
    
    for idx, row in top_20.iterrows():
        address = row['site_address'] if pd.notna(row['site_address']) else 'No Address'
        print(f"{row['development_score']:.1f} | {row['investment_tier']} | "
              f"{row['assessor_parcel_id']} | {address[:30]} | "
              f"{row['base_zoning']} | {row['lot_size_sqft']:,.0f} sqft | "
              f"{row['suggested_use']}")
    
    # Score distribution by zoning
    print("\nAVERAGE SCORES BY ZONING TYPE:")
    print("-"*50)
    zoning_scores = df_scored.groupby('base_zoning')['development_score'].agg(['mean', 'count'])
    zoning_scores = zoning_scores[zoning_scores['count'] >= 5].sort_values('mean', ascending=False).head(15)
    
    for zone, row in zoning_scores.iterrows():
        print(f"   {zone}: {row['mean']:.1f} (n={int(row['count'])})")
    
    # Score by council district
    print("\nAVERAGE SCORES BY COUNCIL DISTRICT:")
    print("-"*50)
    district_scores = df_scored.groupby('council_district')['development_score'].agg(['mean', 'count'])
    district_scores = district_scores.sort_values('mean', ascending=False)
    
    for district, row in district_scores.iterrows():
        if pd.notna(district):
            district_short = str(district)[:40]
            print(f"   {district_short}: {row['mean']:.1f} (n={int(row['count'])})")
    
    # TOC comparison
    print("\nTOC ELIGIBILITY IMPACT:")
    print("-"*50)
    toc_yes = df_scored[df_scored['toc_eligible'] == True]['development_score']
    toc_no = df_scored[df_scored['toc_eligible'] == False]['development_score']
    
    print(f"   TOC Eligible: {toc_yes.mean():.1f} average score (n={len(toc_yes)})")
    print(f"   Not TOC Eligible: {toc_no.mean():.1f} average score (n={len(toc_no)})")
    print(f"   Score Difference: +{toc_yes.mean() - toc_no.mean():.1f} points for TOC")
    
    # Investment tier distribution
    print("\nINVESTMENT TIER DISTRIBUTION:")
    print("-"*50)
    tier_dist = df_scored['investment_tier'].value_counts().sort_index()
    for tier, count in tier_dist.items():
        tier_desc = scorer.config["investment_tiers"][tier]["description"]
        print(f"   Tier {tier} ({tier_desc}): {count} ({count/len(df_scored)*100:.1f}%)")
    
    # Use case distribution
    print("\nSUGGESTED USE CASES (Top 10):")
    print("-"*50)
    use_dist = df_scored['suggested_use'].value_counts().head(10)
    for use, count in use_dist.items():
        print(f"   {use}: {count} properties")
    
    # Overall statistics
    print("\nOVERALL SCORING STATISTICS:")
    print("-"*50)
    score_stats = df_scored['development_score'].describe()
    print(f"   Mean Score: {score_stats['mean']:.1f}")
    print(f"   Median Score: {score_stats['50%']:.1f}")
    print(f"   Std Dev: {score_stats['std']:.1f}")
    print(f"   Min Score: {score_stats['min']:.1f}")
    print(f"   Max Score: {score_stats['max']:.1f}")
    print(f"   25th Percentile: {score_stats['25%']:.1f}")
    print(f"   75th Percentile: {score_stats['75%']:.1f}")
    
    # High opportunity properties
    high_opp = df_scored[df_scored['development_score'] >= 80]
    print(f"\n   High Opportunity Properties (80+): {len(high_opp)} ({len(high_opp)/len(df_scored)*100:.1f}%)")
    
    print("\n" + "="*100)
    print("✓ SCORING ENGINE COMPLETE - Ready for investment analysis!")
    print("="*100)


if __name__ == "__main__":
    main()