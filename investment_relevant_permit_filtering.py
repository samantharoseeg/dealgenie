#!/usr/bin/env python3
"""
INVESTMENT-RELEVANT PERMIT FILTERING
Filter permit data to focus only on meaningful development activity
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Any

class InvestmentPermitFilter:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        
    def get_raw_permit_dataset(self, limit: int = 200) -> List[Dict]:
        """Get raw permit dataset for filtering analysis"""
        print(f"📊 RETRIEVING RAW PERMIT DATASET")
        print("="*40)
        
        try:
            params = {
                '$where': "issue_date >= '2024-01-01T00:00:00.000'",
                '$limit': str(limit),
                '$order': 'issue_date DESC'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers={"X-App-Token": self.app_token},
                timeout=30
            )
            
            if response.status_code == 200:
                permits = response.json()
                print(f"✅ Retrieved {len(permits)} permits from 2024-2025")
                return permits
            else:
                print(f"❌ API Error: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error retrieving permits: {e}")
            return []
    
    def apply_valuation_filtering(self, permits: List[Dict]) -> Dict[str, Any]:
        """
        TASK 1: Valuation-Based Filtering
        Apply minimum thresholds to eliminate minor work
        """
        print(f"\n💰 VALUATION-BASED FILTERING")
        print("="*35)
        
        print(f"Starting with {len(permits)} total permits")
        
        # Parse and categorize by valuation
        valuation_categories = {
            'no_valuation': [],
            'under_10k': [],
            'under_100k': [],
            'over_100k': [],
            'over_500k': [],
            'over_1m': []
        }
        
        parsing_errors = 0
        
        for permit in permits:
            val_str = permit.get('valuation', '')
            
            try:
                if not val_str or val_str in ['N/A', '', 'null']:
                    valuation_categories['no_valuation'].append(permit)
                    continue
                
                # Parse valuation (handle various formats)
                val_clean = str(val_str).replace('$', '').replace(',', '').strip()
                if val_clean:
                    valuation = float(val_clean)
                    
                    if valuation >= 1000000:
                        valuation_categories['over_1m'].append(permit)
                    elif valuation >= 500000:
                        valuation_categories['over_500k'].append(permit)
                    elif valuation >= 100000:
                        valuation_categories['over_100k'].append(permit)
                    elif valuation >= 10000:
                        valuation_categories['under_100k'].append(permit)
                    else:
                        valuation_categories['under_10k'].append(permit)
                else:
                    valuation_categories['no_valuation'].append(permit)
                    
            except (ValueError, TypeError):
                parsing_errors += 1
                valuation_categories['no_valuation'].append(permit)
        
        print(f"\n📊 VALUATION BREAKDOWN:")
        total_permits = len(permits)
        
        for category, permit_list in valuation_categories.items():
            count = len(permit_list)
            percentage = (count / total_permits * 100) if total_permits > 0 else 0
            category_name = category.replace('_', ' ').title()
            
            print(f"   {category_name}: {count} permits ({percentage:.1f}%)")
            
            # Show examples for each category
            if permit_list and count > 0:
                sample = permit_list[0]
                print(f"      Example: {sample.get('primary_address', 'N/A')[:40]} - {sample.get('permit_type', 'N/A')} (${sample.get('valuation', 'N/A')})")
        
        print(f"\n🔍 FILTERING RESULTS:")
        
        # Apply minimum threshold filters
        threshold_100k = valuation_categories['over_100k'] + valuation_categories['over_500k'] + valuation_categories['over_1m']
        threshold_500k = valuation_categories['over_500k'] + valuation_categories['over_1m'] 
        
        print(f"   Original permits: {total_permits}")
        print(f"   After $100K+ filter: {len(threshold_100k)} ({len(threshold_100k)/total_permits*100:.1f}% remain)")
        print(f"   After $500K+ filter: {len(threshold_500k)} ({len(threshold_500k)/total_permits*100:.1f}% remain)")
        print(f"   Parsing errors: {parsing_errors}")
        
        return {
            'original_count': total_permits,
            'valuation_breakdown': {k: len(v) for k, v in valuation_categories.items()},
            'threshold_100k_permits': threshold_100k,
            'threshold_500k_permits': threshold_500k,
            'filtering_eliminated': total_permits - len(threshold_100k),
            'high_value_eliminated': total_permits - len(threshold_500k)
        }
    
    def apply_permit_type_filtering(self, permits: List[Dict]) -> Dict[str, Any]:
        """
        TASK 2: Permit Type Filtering
        Focus on development-relevant permit types
        """
        print(f"\n🏗️ PERMIT TYPE FILTERING")
        print("="*30)
        
        print(f"Analyzing {len(permits)} permits for type relevance...")
        
        # Define investment-relevant criteria
        investment_relevant_types = [
            'BLDG-NEW',
            'BLDG-ADDITION', 
            'BLDG-ALTER/REPAIR'  # Only if commercial or high-value
        ]
        
        investment_relevant_subtypes = [
            'APARTMENT',
            'COMMERCIAL', 
            'OFFICE',
            'RETAIL',
            'MIXED USE',
            'HOTEL',
            'WAREHOUSE'
        ]
        
        exclude_types = [
            'SWIMMING-POOL/SPA',
            'SIGN',
            'GRADING',
            'ELECTRICAL',
            'PLUMBING',
            'MECHANICAL'
        ]
        
        exclude_work_descriptions = [
            'POOL/SPA - PRIVATE',
            'DWELLING - SINGLE FAMILY',  # Unless new construction
            'ACCESSORY DWELLING UNIT'  # Unless significant value
        ]
        
        categorized_permits = {
            'highly_relevant': [],
            'moderately_relevant': [],
            'low_relevance': [],
            'excluded': []
        }
        
        for permit in permits:
            permit_type = permit.get('permit_type', '').upper()
            permit_subtype = permit.get('permit_sub_type', '').upper()  
            work_desc = permit.get('use_desc', '').upper()
            valuation = permit.get('valuation', '')
            
            # Parse valuation for context
            try:
                val_amount = float(str(valuation).replace('$', '').replace(',', '')) if valuation and valuation != 'N/A' else 0
            except:
                val_amount = 0
            
            # Exclusion criteria (lowest priority)
            if any(exc_type in permit_type for exc_type in exclude_types):
                categorized_permits['excluded'].append(permit)
                continue
            
            if any(exc_desc in work_desc for exc_desc in exclude_work_descriptions) and val_amount < 100000:
                categorized_permits['excluded'].append(permit)
                continue
            
            # High relevance criteria
            if (permit_type in ['BLDG-NEW'] or 
                any(subtype in permit_subtype for subtype in investment_relevant_subtypes) or
                val_amount >= 500000):
                categorized_permits['highly_relevant'].append(permit)
                continue
            
            # Moderate relevance criteria  
            if (permit_type in ['BLDG-ADDITION'] or
                (permit_type == 'BLDG-ALTER/REPAIR' and val_amount >= 100000)):
                categorized_permits['moderately_relevant'].append(permit)
                continue
            
            # Everything else is low relevance
            categorized_permits['low_relevance'].append(permit)
        
        print(f"\n📊 TYPE FILTERING RESULTS:")
        total_permits = len(permits)
        
        for category, permit_list in categorized_permits.items():
            count = len(permit_list)
            percentage = (count / total_permits * 100) if total_permits > 0 else 0
            category_name = category.replace('_', ' ').title()
            
            print(f"   {category_name}: {count} permits ({percentage:.1f}%)")
            
            # Show examples
            if permit_list:
                sample = permit_list[0]
                val_str = f"${sample.get('valuation', 'N/A')}" if sample.get('valuation', 'N/A') != 'N/A' else 'No valuation'
                print(f"      Example: {sample.get('permit_type', 'N/A')} - {sample.get('permit_sub_type', 'N/A')} ({val_str})")
        
        # Combined investment-relevant permits
        investment_relevant = categorized_permits['highly_relevant'] + categorized_permits['moderately_relevant']
        
        print(f"\n🎯 INVESTMENT-RELEVANT PERMITS:")
        print(f"   Combined relevant permits: {len(investment_relevant)} ({len(investment_relevant)/total_permits*100:.1f}%)")
        print(f"   Excluded permits: {len(categorized_permits['excluded'])} ({len(categorized_permits['excluded'])/total_permits*100:.1f}%)")
        
        return {
            'categorized_permits': categorized_permits,
            'investment_relevant_permits': investment_relevant,
            'total_analyzed': total_permits,
            'relevance_rate': len(investment_relevant) / total_permits * 100 if total_permits > 0 else 0
        }
    
    def test_combined_filtering(self, permits: List[Dict]) -> Dict[str, Any]:
        """
        TASK 4: Test Combined Filters
        Apply both valuation and type filtering together
        """
        print(f"\n🔬 COMBINED FILTERING TEST")
        print("="*30)
        
        print(f"Testing combined filters on {len(permits)} permits...")
        
        filtered_permits = []
        
        for permit in permits:
            # Valuation filter
            valuation = permit.get('valuation', '')
            try:
                val_amount = float(str(valuation).replace('$', '').replace(',', '')) if valuation and valuation != 'N/A' else 0
            except:
                val_amount = 0
            
            # Type filter
            permit_type = permit.get('permit_type', '').upper()
            permit_subtype = permit.get('permit_sub_type', '').upper()
            work_desc = permit.get('use_desc', '').upper()
            
            # Combined criteria: Must meet BOTH valuation AND type requirements
            meets_valuation = val_amount >= 100000
            
            meets_type = (
                permit_type in ['BLDG-NEW', 'BLDG-ADDITION'] or
                any(subtype in permit_subtype for subtype in ['APARTMENT', 'COMMERCIAL', 'OFFICE', 'RETAIL']) or
                (permit_type == 'BLDG-ALTER/REPAIR' and val_amount >= 200000)
            )
            
            excludes_minor = not (
                'POOL/SPA' in permit_type or
                'SIGN' in permit_type or
                ('DWELLING - SINGLE FAMILY' in work_desc and val_amount < 100000)
            )
            
            if meets_valuation and meets_type and excludes_minor:
                filtered_permits.append({
                    'permit': permit,
                    'valuation': val_amount,
                    'type_score': 'HIGH' if val_amount >= 500000 else 'MEDIUM'
                })
        
        # Analyze filtered results
        original_count = len(permits)
        filtered_count = len(filtered_permits)
        survival_rate = (filtered_count / original_count * 100) if original_count > 0 else 0
        
        print(f"📊 COMBINED FILTERING RESULTS:")
        print(f"   Original permits: {original_count}")
        print(f"   After combined filtering: {filtered_count}")
        print(f"   Survival rate: {survival_rate:.1f}%")
        print(f"   Eliminated: {original_count - filtered_count} permits ({100-survival_rate:.1f}%)")
        
        # Show examples of surviving permits
        if filtered_permits:
            print(f"\n✅ INVESTMENT-RELEVANT PERMITS FOUND:")
            high_value = [fp for fp in filtered_permits if fp['type_score'] == 'HIGH']
            medium_value = [fp for fp in filtered_permits if fp['type_score'] == 'MEDIUM']
            
            print(f"   High-value permits (≥$500K): {len(high_value)}")
            print(f"   Medium-value permits ($100K-$500K): {len(medium_value)}")
            
            # Show top examples
            sorted_permits = sorted(filtered_permits, key=lambda x: x['valuation'], reverse=True)
            
            print(f"\n🏆 TOP INVESTMENT-RELEVANT PERMITS:")
            for i, fp in enumerate(sorted_permits[:5], 1):
                permit = fp['permit']
                print(f"   {i}. ${fp['valuation']:,.0f} - {permit.get('primary_address', 'N/A')}")
                print(f"      Type: {permit.get('permit_type', 'N/A')} - {permit.get('permit_sub_type', 'N/A')}")
                print(f"      Date: {permit.get('issue_date', 'N/A')}")
        
        return {
            'original_count': original_count,
            'filtered_count': filtered_count,
            'survival_rate': survival_rate,
            'filtered_permits': [fp['permit'] for fp in filtered_permits],
            'high_value_permits': [fp['permit'] for fp in filtered_permits if fp['type_score'] == 'HIGH'],
            'medium_value_permits': [fp['permit'] for fp in filtered_permits if fp['type_score'] == 'MEDIUM']
        }
    
    def assess_filtered_business_value(self, filtering_results: Dict) -> Dict[str, Any]:
        """
        TASK 5: Assess Business Value After Filtering
        Determine if filtered dataset is viable for scoring
        """
        print(f"\n💼 BUSINESS VALUE ASSESSMENT")
        print("="*35)
        
        filtered_count = filtering_results['filtered_count']
        survival_rate = filtering_results['survival_rate']
        high_value_count = len(filtering_results['high_value_permits'])
        
        print(f"📊 FILTERED DATASET METRICS:")
        print(f"   Investment-relevant permits: {filtered_count}")
        print(f"   High-value permits (≥$500K): {high_value_count}")
        print(f"   Data survival rate: {survival_rate:.1f}%")
        
        # Assess viability thresholds
        viability_assessment = {
            'dataset_size': 'ADEQUATE' if filtered_count >= 50 else 'LIMITED' if filtered_count >= 20 else 'INSUFFICIENT',
            'high_value_presence': 'GOOD' if high_value_count >= 10 else 'MODERATE' if high_value_count >= 5 else 'POOR',
            'data_quality': 'HIGH' if survival_rate <= 10 else 'MEDIUM' if survival_rate <= 25 else 'LOW'
        }
        
        print(f"\n🎯 VIABILITY ASSESSMENT:")
        for metric, rating in viability_assessment.items():
            print(f"   {metric.replace('_', ' ').title()}: {rating}")
        
        # Overall recommendation
        good_ratings = sum(1 for rating in viability_assessment.values() if rating in ['ADEQUATE', 'GOOD', 'HIGH'])
        
        if good_ratings >= 2:
            overall_recommendation = "VIABLE_WITH_FILTERING"
            scoring_weight = "5-8%"
            implementation = "Use filtered permits as supplementary scoring factor"
        elif good_ratings >= 1:
            overall_recommendation = "LIMITED_VIABILITY" 
            scoring_weight = "2-3%"
            implementation = "Use only high-value permits as development indicators"
        else:
            overall_recommendation = "NOT_VIABLE"
            scoring_weight = "0%"
            implementation = "Permit data insufficient for reliable scoring"
        
        print(f"\n💡 BUSINESS RECOMMENDATIONS:")
        print(f"   Overall viability: {overall_recommendation}")
        print(f"   Recommended scoring weight: {scoring_weight}")
        print(f"   Implementation strategy: {implementation}")
        
        # Sample implementation logic
        if overall_recommendation != "NOT_VIABLE":
            print(f"\n💻 SUGGESTED IMPLEMENTATION:")
            print(f"   def calculate_permit_score(permits):")
            print(f"       score = 0")
            print(f"       for permit in permits:")
            print(f"           if permit.valuation >= 500000:")
            print(f"               score += 8  # High-value development")
            print(f"           elif permit.valuation >= 100000:")
            print(f"               score += 3  # Medium-value development") 
            print(f"       return min(score, 15)  # Cap at 15 points")
        
        return {
            'viability_metrics': viability_assessment,
            'overall_recommendation': overall_recommendation,
            'scoring_weight': scoring_weight,
            'implementation_strategy': implementation,
            'filtered_dataset_size': filtered_count,
            'high_value_permits': high_value_count
        }
    
    def run_investment_filtering_analysis(self):
        """Run complete investment-relevant permit filtering analysis"""
        print("🔍 INVESTMENT-RELEVANT PERMIT FILTERING ANALYSIS")
        print("="*60)
        print("Filtering permit data for meaningful development activity")
        print()
        
        # Get raw dataset
        raw_permits = self.get_raw_permit_dataset(200)
        if not raw_permits:
            print("❌ Failed to retrieve permit data")
            return
        
        # Apply valuation filtering
        valuation_results = self.apply_valuation_filtering(raw_permits)
        
        # Apply permit type filtering  
        type_results = self.apply_permit_type_filtering(raw_permits)
        
        # Test combined filtering
        combined_results = self.test_combined_filtering(raw_permits)
        
        # Assess business value
        business_assessment = self.assess_filtered_business_value(combined_results)
        
        # Save comprehensive results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_filename = f"investment_permit_filtering_{timestamp}.json"
        
        comprehensive_results = {
            'raw_dataset_size': len(raw_permits),
            'valuation_analysis': valuation_results,
            'type_analysis': type_results,
            'combined_filtering': combined_results,
            'business_assessment': business_assessment,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        with open(results_filename, 'w') as f:
            json.dump(comprehensive_results, f, indent=2, default=str)
        
        print(f"\n📁 FILTERING ANALYSIS COMPLETE")
        print(f"Results saved to: {results_filename}")
        
        return comprehensive_results

def main():
    """Run investment-relevant permit filtering"""
    print("💰 INVESTMENT-RELEVANT PERMIT FILTERING")
    print("="*45)
    print("Focusing permit data on meaningful development activity")
    print()
    
    filter_analyzer = InvestmentPermitFilter()
    results = filter_analyzer.run_investment_filtering_analysis()
    
    return results

if __name__ == "__main__":
    main()