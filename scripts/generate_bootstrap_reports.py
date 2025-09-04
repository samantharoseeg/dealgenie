#!/usr/bin/env python3
"""
Bootstrap HTML Report Generator
Standalone script to generate HTML reports for the bootstrap pipeline.
"""

import sys
import os
sys.path.append('.')

from features.feature_matrix import get_feature_matrix
from scoring.engine import calculate_score
import json
import time
from datetime import datetime

def generate_html_report(apn, template, score_result, features=None):
    """Generate professional HTML report with address information."""
    
    # Get features if not provided
    if features is None:
        features = get_feature_matrix(apn)
    
    # Extract address information
    property_address = features.get('site_address', 'Address not available')
    property_city = features.get('site_city', 'Los Angeles')
    property_zip = features.get('site_zip', '')
    property_zoning = features.get('zoning', 'Unknown')
    property_lot_size = features.get('lot_size_sqft', 0)
    
    # Format full address
    full_address = f"{property_address}, {property_city}"
    if property_zip:
        full_address += f" {property_zip}"
    
    # Get grade styling
    def get_grade_class(score):
        if score >= 8.0: return 'grade-a'
        elif score >= 6.5: return 'grade-b'
        elif score >= 5.0: return 'grade-c'
        else: return 'grade-d'
    
    def get_grade(score):
        if score >= 8.0: return 'A'
        elif score >= 6.5: return 'B'  
        elif score >= 5.0: return 'C'
        else: return 'D'
    
    # Generate component HTML
    components_html = ''
    for name, score in score_result.get('component_scores', {}).items():
        components_html += f'''
        <div class="component">
            <div class="component-name">{name.replace('_', ' ').title()}</div>
            <div class="component-score">{score:.1f}</div>
        </div>
        '''
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DealGenie Score Report - APN {apn}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            background: white;
            border-radius: 15px;
            padding: 40px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .logo {{
            font-size: 28px;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }}
        .subtitle {{
            color: #718096;
            font-size: 16px;
        }}
        h1 {{
            color: #2d3748;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin: 30px 0;
        }}
        .score-display {{
            text-align: center;
            margin: 40px 0;
        }}
        .score-value {{
            font-size: 72px;
            font-weight: bold;
            color: #667eea;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }}
        .score-label {{
            font-size: 24px;
            color: #718096;
            margin-top: 10px;
        }}
        .grade {{
            display: inline-block;
            padding: 8px 20px;
            border-radius: 25px;
            font-weight: bold;
            margin-left: 15px;
            font-size: 20px;
        }}
        .grade-a {{ background: #48bb78; color: white; }}
        .grade-b {{ background: #4299e1; color: white; }}
        .grade-c {{ background: #ed8936; color: white; }}
        .grade-d {{ background: #f56565; color: white; }}
        .section {{
            margin: 30px 0;
            padding: 25px;
            background: #f7fafc;
            border-radius: 12px;
            border-left: 5px solid #667eea;
        }}
        .section h2 {{
            color: #2d3748;
            margin-top: 0;
            font-size: 20px;
            margin-bottom: 15px;
        }}
        .property-info {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .info-item {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
        }}
        .info-label {{
            font-size: 12px;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 5px;
        }}
        .info-value {{
            font-size: 16px;
            font-weight: 600;
            color: #2d3748;
        }}
        .explanation {{
            line-height: 1.8;
            color: #4a5568;
            font-size: 16px;
            margin: 15px 0;
        }}
        .recommendations {{
            list-style: none;
            padding: 0;
        }}
        .recommendations li {{
            padding: 15px;
            margin: 12px 0;
            background: white;
            border-radius: 8px;
            border-left: 4px solid #48bb78;
            color: #2d3748;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .recommendations li:before {{
            content: "✓ ";
            color: #48bb78;
            font-weight: bold;
            font-size: 18px;
            margin-right: 10px;
        }}
        .components {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .component {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border: 1px solid #e2e8f0;
        }}
        .component-name {{
            font-size: 12px;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 8px;
            font-weight: 600;
        }}
        .component-score {{
            font-size: 28px;
            font-weight: bold;
            color: #667eea;
        }}
        .metadata {{
            margin-top: 40px;
            padding-top: 25px;
            border-top: 2px solid #e2e8f0;
            color: #718096;
            font-size: 14px;
            background: #f8fafc;
            padding: 25px;
            border-radius: 8px;
        }}
        .timestamp {{
            text-align: right;
            font-style: italic;
            margin-top: 15px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">🏠 DealGenie</div>
            <div class="subtitle">AI-Powered Real Estate Development Scoring</div>
        </div>
        
        <h1>Property Investment Analysis</h1>
        
        <div class="score-display">
            <div class="score-value">{score_result['score']:.1f}</div>
            <div class="score-label">
                Investment Score
                <span class="grade {get_grade_class(score_result['score'])}">{get_grade(score_result['score'])}</span>
            </div>
        </div>
        
        <div class="section">
            <h2>📍 Property Information</h2>
            <div class="property-info">
                <div class="info-item">
                    <div class="info-label">Property Address</div>
                    <div class="info-value">{full_address}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Assessor Parcel Number</div>
                    <div class="info-value">{apn}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Zoning</div>
                    <div class="info-value">{property_zoning}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Lot Size</div>
                    <div class="info-value">{property_lot_size:,.0f} sq ft</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Development Template</div>
                    <div class="info-value">{template.title()}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Analysis Date</div>
                    <div class="info-value">{datetime.now().strftime('%B %d, %Y')}</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Analysis Summary</h2>
            <div class="explanation">{score_result['explanation']}</div>
        </div>
        
        <div class="section">
            <h2>💡 Investment Recommendations</h2>
            <ul class="recommendations">
                {''.join(f'<li>{rec}</li>' for rec in score_result.get('recommendations', []))}
            </ul>
        </div>
        
        <div class="section">
            <h2>🔍 Component Analysis</h2>
            <div class="components">
                {components_html}
            </div>
        </div>
        
        <div class="metadata">
            <strong>Report Details:</strong><br>
            Generated by DealGenie AI Scoring System<br>
            Data Source: LA County Real Property Records (369,703 parcels)<br>
            Analysis Engine: Week 1 Foundation with Real Data Integration<br>
            <div class="timestamp">
                Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S PST')}
            </div>
        </div>
    </div>
</body>
</html>'''
    
    return html_content

def main():
    """Generate bootstrap HTML reports."""
    
    print("📊 Generating bootstrap HTML reports...")
    
    # Read sample APNs
    sample_apns = []
    apn_files = ['diverse_apns.txt', 'sample_apns.txt']
    
    for apn_file in apn_files:
        if os.path.exists(apn_file):
            with open(apn_file, 'r') as f:
                apns = [line.strip() for line in f.readlines() if line.strip()]
                sample_apns.extend(apns)
            break
    
    if not sample_apns:
        print('❌ No sample APNs available for report generation')
        sys.exit(1)
    
    # Generate reports for different templates
    templates = ['multifamily', 'residential', 'commercial', 'industrial', 'retail']
    reports_generated = 0
    target_reports = min(15, len(sample_apns))
    
    print(f"📊 Generating {target_reports} sample reports...")
    
    for i in range(target_reports):
        try:
            apn = sample_apns[i % len(sample_apns)]
            template = templates[i % len(templates)]
            
            # Get features and calculate score
            features = get_feature_matrix(apn)
            score_result = calculate_score(features, template)
            
            # Generate HTML report
            html_content = generate_html_report(apn, template, score_result, features)
            
            # Save report
            filename = f'dealgenie_report_{apn}_{template}.html'
            filepath = os.path.join('out', filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            reports_generated += 1
            
            print(f'   ✓ Report {reports_generated:2d}/{target_reports}: APN {apn} ({template}) → {filename}')
            
            # Brief pause to avoid overwhelming the system
            time.sleep(0.1)
            
        except Exception as e:
            print(f'   ⚠️  Failed to generate report for APN {apn}: {e}')
            continue
    
    print(f'✓ Generated {reports_generated} HTML reports in ./out/')
    return reports_generated

if __name__ == "__main__":
    main()