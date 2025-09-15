#!/usr/bin/env python3
"""
Test Real Crime Integration - Debug and Fix HTML Generator
Critical Bug Fix: Trace and fix the data flow from coordinates to HTML display
"""

import sys
import os
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

def test_data_flow_chain():
    """Test the complete data flow chain from coordinates to HTML display."""
    print("=" * 80)
    print("🔍 TRACING COMPLETE DATA FLOW CHAIN")
    print("=" * 80)
    
    # Test coordinates that should show clear differentiation
    test_cases = [
        {
            'name': 'Downtown LA (Should show ~93.0)',
            'coordinates': {
                'latitude': 34.052235,
                'longitude': -118.243685,
                'site_address': '123 Spring Street, Los Angeles, CA 90012',
                'property_type': 'multifamily'
            },
            'expected_crime_score': 93.0
        },
        {
            'name': 'Beverly Hills (Should show ~5.6)',
            'coordinates': {
                'latitude': 34.073620,
                'longitude': -118.400356,
                'site_address': '456 Rodeo Drive, Beverly Hills, CA 90210',
                'property_type': 'residential'
            },
            'expected_crime_score': 5.6
        }
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 Testing: {test_case['name']}")
        print(f"   Coordinates: {test_case['coordinates']['latitude']}, {test_case['coordinates']['longitude']}")
        
        # Step 1: Test EnhancedDealGenieScorer directly
        print("\n   Step 1: Testing EnhancedDealGenieScorer directly...")
        try:
            from src.analysis.enhanced_dealgenie_scorer import EnhancedDealGenieScorer
            import pandas as pd
            
            scorer = EnhancedDealGenieScorer()
            df_row = pd.Series(test_case['coordinates'])
            
            overall_score, scoring_result = scorer.calculate_enhanced_property_score(df_row)
            
            # Extract crime score from scoring result
            crime_analysis = scoring_result.get('crime_analysis', {})
            crime_risk = crime_analysis.get('crime_risk', {})
            direct_crime_score = crime_risk.get('score', 'NOT_FOUND')
            
            print(f"   ✅ Scorer Result:")
            print(f"      Overall Score: {overall_score}")
            print(f"      Crime Score: {direct_crime_score}")
            print(f"      Crime Analysis Keys: {list(crime_analysis.keys())}")
            print(f"      Crime Risk Keys: {list(crime_risk.keys()) if crime_risk else 'None'}")
            
        except Exception as e:
            print(f"   ❌ Scorer Error: {e}")
            direct_crime_score = None
        
        # Step 2: Test Enhanced Explainer 
        print("\n   Step 2: Testing Enhanced Explainer...")
        try:
            from src.reporting.enhanced_explainer import EnhancedPropertyExplainer
            
            explainer = EnhancedPropertyExplainer()
            explain_result = explainer.generate_enhanced_explain_json(
                features=test_case['coordinates'],
                scoring_result=scoring_result if 'scoring_result' in locals() else {}
            )
            
            # Handle result format
            if isinstance(explain_result, dict):
                explain_data = explain_result
            else:
                import json
                explain_data = json.loads(explain_result)
            
            # Extract crime score from explain JSON
            explain_crime_score = None
            for section in explain_data.get('sections', []):
                if section.get('section') == 'crime_intelligence':
                    content = section.get('content', {})
                    explain_crime_score = content.get('crime_score', 'NOT_FOUND')
                    break
            
            print(f"   ✅ Explainer Result:")
            print(f"      Sections: {[s.get('section') for s in explain_data.get('sections', [])]}")
            print(f"      Crime Score in Explain: {explain_crime_score}")
            
        except Exception as e:
            print(f"   ❌ Explainer Error: {e}")
            explain_crime_score = None
        
        # Step 3: Test HTML Generator
        print("\n   Step 3: Testing HTML Generator...")
        try:
            from src.reporting.html_generator import EnhancedHTMLReportGenerator
            
            generator = EnhancedHTMLReportGenerator()
            html_content = generator.generate_comprehensive_report(test_case['coordinates'])
            
            # Extract crime score from HTML
            html_crime_score = extract_crime_score_from_html(html_content)
            
            print(f"   ✅ HTML Generator Result:")
            print(f"      HTML Size: {len(html_content):,} chars")
            print(f"      Crime Score in HTML: {html_crime_score}")
            
            # Save HTML for inspection
            filename = f"debug_{test_case['name'].lower().replace(' ', '_').replace('(', '').replace(')', '')}.html"
            with open(filename, 'w') as f:
                f.write(html_content)
            print(f"      Saved HTML: {filename}")
            
        except Exception as e:
            print(f"   ❌ HTML Generator Error: {e}")
            html_crime_score = None
        
        # Step 4: Compare Results
        print(f"\n   📊 COMPARISON:")
        print(f"      Expected Score: {test_case['expected_crime_score']}")
        print(f"      Direct Scorer Score: {direct_crime_score}")
        print(f"      Explain JSON Score: {explain_crime_score}")
        print(f"      HTML Display Score: {html_crime_score}")
        
        # Check consistency
        scores = [direct_crime_score, explain_crime_score, html_crime_score]
        valid_scores = [s for s in scores if s is not None and s != 'NOT_FOUND']
        
        if len(set(valid_scores)) <= 1 and valid_scores:
            consistency = "✅ CONSISTENT"
        else:
            consistency = "❌ INCONSISTENT"
        
        print(f"      Data Consistency: {consistency}")
        print(f"      Expected vs Actual: {'✅ MATCH' if html_crime_score == test_case['expected_crime_score'] else '❌ MISMATCH'}")

def extract_crime_score_from_html(html_content: str) -> float:
    """Extract crime score from HTML content."""
    import re
    
    # Look for crime score in score-text div
    pattern = r'<div class="score-text">(\d+\.?\d*)/100</div>'
    matches = re.findall(pattern, html_content)
    
    if matches:
        try:
            return float(matches[0])
        except ValueError:
            pass
    
    return None

def debug_html_generator_initialization():
    """Debug why HTML generator might not be using real scorer."""
    print("\n🔧 DEBUGGING HTML GENERATOR INITIALIZATION")
    print("-" * 60)
    
    try:
        from src.reporting.html_generator import EnhancedHTMLReportGenerator
        
        generator = EnhancedHTMLReportGenerator()
        
        print(f"Scorer available: {generator.scorer is not None}")
        print(f"Explainer available: {generator.explainer is not None}")
        
        if generator.scorer:
            print(f"Scorer type: {type(generator.scorer)}")
        else:
            print("❌ Scorer is None - this is why mock data is used!")
            
        if generator.explainer:
            print(f"Explainer type: {type(generator.explainer)}")
        else:
            print("❌ Explainer is None - this is why mock data is used!")
            
    except Exception as e:
        print(f"❌ Debug Error: {e}")

def test_manual_integration():
    """Manually test the integration with exact coordinates."""
    print("\n🎯 MANUAL INTEGRATION TEST WITH EXACT COORDINATES")
    print("-" * 60)
    
    # Exact coordinates that should show differentiation
    downtown_coords = {
        'latitude': 34.052235,
        'longitude': -118.243685,
        'site_address': '123 Spring Street, Los Angeles, CA 90012',
        'property_type': 'multifamily'
    }
    
    beverly_coords = {
        'latitude': 34.073620,
        'longitude': -118.400356,
        'site_address': '456 Rodeo Drive, Beverly Hills, CA 90210',
        'property_type': 'residential'
    }
    
    print(f"Testing Downtown LA: {downtown_coords['latitude']}, {downtown_coords['longitude']}")
    print(f"Testing Beverly Hills: {beverly_coords['latitude']}, {beverly_coords['longitude']}")
    
    try:
        # Import and test scorer directly
        from src.analysis.enhanced_dealgenie_scorer import EnhancedDealGenieScorer
        import pandas as pd
        
        scorer = EnhancedDealGenieScorer()
        
        # Test Downtown LA
        print(f"\n🏢 Downtown LA Results:")
        df_row = pd.Series(downtown_coords)
        score1, result1 = scorer.calculate_enhanced_property_score(df_row)
        crime1 = result1.get('crime_analysis', {}).get('crime_risk', {}).get('score', 0)
        print(f"   Overall Score: {score1}")
        print(f"   Crime Score: {crime1}")
        
        # Test Beverly Hills
        print(f"\n🏡 Beverly Hills Results:")
        df_row = pd.Series(beverly_coords)
        score2, result2 = scorer.calculate_enhanced_property_score(df_row)
        crime2 = result2.get('crime_analysis', {}).get('crime_risk', {}).get('score', 0)
        print(f"   Overall Score: {score2}")
        print(f"   Crime Score: {crime2}")
        
        # Calculate differentiation
        if crime1 and crime2:
            diff = abs(crime1 - crime2)
            print(f"\n📊 Crime Score Differentiation: {diff:.1f} points")
            print(f"    Downtown LA: {crime1}")
            print(f"    Beverly Hills: {crime2}")
            
            if diff > 50:
                print("✅ SIGNIFICANT DIFFERENTIATION FOUND!")
            else:
                print("❌ Limited differentiation")
        else:
            print("❌ No valid crime scores obtained")
            
    except Exception as e:
        print(f"❌ Manual test error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main execution function."""
    print("🚨 CRITICAL BUG FIX: HTML Generator Real Crime Integration")
    print("Testing complete data flow from coordinates to HTML display...\n")
    
    # Debug initialization first
    debug_html_generator_initialization()
    
    # Test manual integration
    test_manual_integration()
    
    # Test complete data flow
    test_data_flow_chain()

if __name__ == "__main__":
    main()