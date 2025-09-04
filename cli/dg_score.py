#!/usr/bin/env python3
"""
DealGenie Scoring CLI
Command-line interface for property scoring and report generation.
"""

import argparse
import json
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scoring.engine import calculate_score
from features.feature_matrix import get_feature_matrix


def generate_html_report(score_result: dict, apn: str, output_path: str) -> None:
    """Generate HTML report from scoring results."""
    html_content = f"""
<!DOCTYPE html>
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
        h1 {{
            color: #2d3748;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-bottom: 30px;
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
        .section {{
            margin: 30px 0;
            padding: 20px;
            background: #f7fafc;
            border-radius: 10px;
            border-left: 4px solid #667eea;
        }}
        .section h2 {{
            color: #2d3748;
            margin-top: 0;
            font-size: 20px;
        }}
        .explanation {{
            line-height: 1.8;
            color: #4a5568;
            font-size: 16px;
        }}
        .recommendations {{
            list-style: none;
            padding: 0;
        }}
        .recommendations li {{
            padding: 12px;
            margin: 10px 0;
            background: white;
            border-radius: 8px;
            border-left: 3px solid #48bb78;
            color: #2d3748;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .recommendations li:before {{
            content: "✓ ";
            color: #48bb78;
            font-weight: bold;
            margin-right: 8px;
        }}
        .components {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .component {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .component-name {{
            font-size: 12px;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 5px;
        }}
        .component-score {{
            font-size: 24px;
            font-weight: bold;
            color: #667eea;
        }}
        .metadata {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
            color: #718096;
            font-size: 14px;
        }}
        .grade {{
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            margin-left: 20px;
        }}
        .grade-a {{ background: #48bb78; color: white; }}
        .grade-b {{ background: #4299e1; color: white; }}
        .grade-c {{ background: #ed8936; color: white; }}
        .grade-d {{ background: #f56565; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>DealGenie Property Score Report</h1>
        
        <div class="score-display">
            <div class="score-value">{score_result['score']:.1f}</div>
            <div class="score-label">
                Investment Score
                <span class="grade {get_grade_class(score_result['score'])}">{get_grade(score_result['score'])}</span>
            </div>
        </div>
        
        <div class="section">
            <h2>Analysis Summary</h2>
            <p class="explanation">{score_result['explanation']}</p>
        </div>
        
        <div class="section">
            <h2>Investment Recommendations</h2>
            <ul class="recommendations">
                {''.join(f'<li>{rec}</li>' for rec in score_result.get('recommendations', []))}
            </ul>
        </div>
        
        <div class="section">
            <h2>Component Scores</h2>
            <div class="components">
                {generate_component_html(score_result.get('component_scores', {}))}
            </div>
        </div>
        
        <div class="metadata">
            <strong>Property APN:</strong> {apn}<br>
            <strong>Template:</strong> {score_result.get('template', 'multifamily')}<br>
            <strong>Report Generated:</strong> {score_result.get('timestamp', 'N/A')}
        </div>
    </div>
</body>
</html>
"""
    
    with open(output_path, 'w') as f:
        f.write(html_content)
    print(f"HTML report saved to: {output_path}")


def get_grade(score: float) -> str:
    """Get letter grade for score."""
    if score >= 8.0:
        return "A"
    elif score >= 6.5:
        return "B"
    elif score >= 5.0:
        return "C"
    else:
        return "D"


def get_grade_class(score: float) -> str:
    """Get CSS class for grade."""
    grade = get_grade(score)
    return f"grade-{grade.lower()}"


def generate_component_html(components: dict) -> str:
    """Generate HTML for component scores."""
    html = ""
    for name, score in components.items():
        html += f"""
        <div class="component">
            <div class="component-name">{name.replace('_', ' ').title()}</div>
            <div class="component-score">{score:.1f}</div>
        </div>
        """
    return html


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DealGenie Property Scoring CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Score command
    score_parser = subparsers.add_parser('score', help='Score a property')
    score_parser.add_argument(
        '--template',
        choices=['multifamily', 'commercial', 'residential'],
        default='multifamily',
        help='Scoring template to use'
    )
    score_parser.add_argument(
        '--apn',
        required=True,
        help='Assessor Parcel Number'
    )
    score_parser.add_argument(
        '--format',
        choices=['json', 'html'],
        default='json',
        help='Output format'
    )
    score_parser.add_argument(
        '--output',
        help='Output file path (for HTML format)'
    )
    
    args = parser.parse_args()
    
    if args.command == 'score':
        # Get property features
        features = get_feature_matrix(args.apn)
        
        # Calculate score
        score_result = calculate_score(features, args.template)
        
        if args.format == 'json':
            # Output JSON
            print(json.dumps(score_result, indent=2))
        elif args.format == 'html':
            # Generate HTML report
            output_path = args.output or f"score_report_{args.apn}.html"
            generate_html_report(score_result, args.apn, output_path)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()