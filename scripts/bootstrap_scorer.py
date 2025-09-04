#!/usr/bin/env python3
"""
DealGenie Bootstrap Scorer
Generates scoring reports for top properties as part of Phase 6 testing
"""

import sys
import json
import os
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scoring.engine import calculate_score
from features.feature_matrix import get_feature_matrix

# Test APNs - use known APNs from database
TOP_APNS = [
    '5306050014',  # 321 E 3RD ST
    '5309100014',  # 555 W 7TH ST  
    '5309130032',  # 888 S MAIN ST
    '5544300055',  # 999 WILSHIRE BLVD
    '4355060064',  # 777 FIGUEROA ST
    '4371310002',  # 111 HOPE ST
    '5559110020',  # 222 GRAND AVE
    '5465240010',  # 123 S SPRING ST
    '5431200006',  # 456 W 5TH ST
    '5149021900',  # 789 S BROADWAY
]

def generate_bootstrap_report(apn: str, index: int) -> None:
    """Generate HTML and JSON reports for a property"""
    
    # Get features and calculate score
    features = get_feature_matrix(apn)
    score_result = calculate_score(features, 'multifamily')
    
    # Save JSON report
    json_path = f"out/report_{apn}.json"
    with open(json_path, 'w') as f:
        json.dump(score_result, f, indent=2)
    
    # Generate HTML report
    html_path = f"out/report_{apn}.html"
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DealGenie Report - {apn}</title>
    <style>
        body {{
            font-family: 'Arial', sans-serif;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }}
        .report {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }}
        .header {{
            border-bottom: 3px solid #667eea;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        h1 {{
            color: #2d3748;
            margin: 0;
        }}
        .meta {{
            color: #718096;
            font-size: 14px;
            margin-top: 10px;
        }}
        .score-display {{
            text-align: center;
            padding: 30px;
            background: linear-gradient(135deg, #f6f6f6 0%, #e9e9e9 100%);
            border-radius: 10px;
            margin: 20px 0;
        }}
        .score-value {{
            font-size: 64px;
            font-weight: bold;
            color: #667eea;
            margin: 0;
        }}
        .score-label {{
            font-size: 18px;
            color: #4a5568;
            margin-top: 10px;
        }}
        .tier {{
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            background: #667eea;
            color: white;
            font-weight: bold;
            margin-left: 10px;
        }}
        .components {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .component {{
            background: #f7fafc;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            text-align: center;
        }}
        .component-name {{
            font-size: 12px;
            color: #718096;
            text-transform: uppercase;
            margin-bottom: 5px;
        }}
        .component-score {{
            font-size: 24px;
            font-weight: bold;
            color: #4a5568;
        }}
        .explanation {{
            background: #edf2f7;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            line-height: 1.6;
            color: #2d3748;
        }}
        .recommendations {{
            list-style: none;
            padding: 0;
        }}
        .recommendations li {{
            padding: 15px;
            margin: 10px 0;
            background: white;
            border-left: 4px solid #48bb78;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-radius: 4px;
        }}
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
            text-align: center;
            color: #718096;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="report">
        <div class="header">
            <h1>DealGenie Property Analysis Report #{index + 1}</h1>
            <div class="meta">
                <strong>APN:</strong> {apn} | 
                <strong>Template:</strong> {score_result.get('template', 'multifamily')} | 
                <strong>Generated:</strong> {score_result.get('timestamp', 'N/A')[:19]}
            </div>
        </div>
        
        <div class="score-display">
            <div class="score-value">{score_result['score']:.1f}</div>
            <div class="score-label">
                Investment Score
                <span class="tier">{'Premium' if score_result['score'] >= 8 else 'Good' if score_result['score'] >= 6.5 else 'Average'}</span>
            </div>
        </div>
        
        <h2>Component Analysis</h2>
        <div class="components">
            {''.join(f'<div class="component"><div class="component-name">{name.replace("_", " ").title()}</div><div class="component-score">{score:.1f}</div></div>' for name, score in score_result.get('component_scores', {}).items())}
        </div>
        
        <h2>Investment Analysis</h2>
        <div class="explanation">
            {score_result.get('explanation', 'No analysis available')}
        </div>
        
        <h2>Recommendations</h2>
        <ul class="recommendations">
            {''.join(f'<li>{rec}</li>' for rec in score_result.get('recommendations', []))}
        </ul>
        
        <div class="footer">
            DealGenie AI Scoring Engine | Phase 6 Bootstrap Test | Property {index + 1} of {len(TOP_APNS)}
        </div>
    </div>
</body>
</html>
"""
    
    with open(html_path, 'w') as f:
        f.write(html_content)
    
    print(f"  [{index+1:2d}/10] APN {apn}: Score {score_result['score']:.1f}/10 - Reports generated ✓")


def main():
    """Main bootstrap process"""
    print("Starting bootstrap scoring process...")
    print(f"Generating reports for {len(TOP_APNS)} properties...")
    print("")
    
    # Ensure output directory exists
    os.makedirs('out', exist_ok=True)
    
    # Generate reports for each property
    for i, apn in enumerate(TOP_APNS):
        try:
            generate_bootstrap_report(apn, i)
        except Exception as e:
            print(f"  [{i+1:2d}/10] APN {apn}: Error - {e}")
    
    print("")
    print(f"✅ Bootstrap complete! Generated reports in out/")
    print(f"   - {len(TOP_APNS)} HTML reports")
    print(f"   - {len(TOP_APNS)} JSON reports")
    
    # Generate summary
    summary_path = "out/bootstrap_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("DealGenie Bootstrap Summary\n")
        f.write("===========================\n\n")
        f.write(f"Total Properties Analyzed: {len(TOP_APNS)}\n")
        f.write(f"Report Types: HTML, JSON\n")
        f.write(f"Scoring Template: Multifamily\n\n")
        f.write("Properties Scored:\n")
        for i, apn in enumerate(TOP_APNS, 1):
            f.write(f"{i:2d}. APN {apn}\n")
    
    print(f"   - Summary saved to {summary_path}")


if __name__ == '__main__':
    main()