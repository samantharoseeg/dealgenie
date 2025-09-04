#!/usr/bin/env python3
"""
DealGenie Multi-Template CLI Interface v1.2

Updated CLI interface supporting both single-template and multi-template outputs
with JSON and HTML formats based on feature flags.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scoring.multi_template_scorer import score_parcel_multi_template
from scoring.result_formatter import format_multi_template_result
from scoring.engine import calculate_score
from database.multi_template_db import store_multi_template_result, get_multi_template_result

logger = logging.getLogger(__name__)

class MultiTemplateCLI:
    """Enhanced CLI with multi-template support"""
    
    def __init__(self):
        """Initialize CLI with feature flags"""
        self.feature_flags = {
            'MULTI_TEMPLATE_ENABLED': True,
            'HTML_OUTPUT_ENABLED': True,
            'DATABASE_STORAGE_ENABLED': True
        }
    
    def create_parser(self) -> argparse.ArgumentParser:
        """Create command line argument parser"""
        parser = argparse.ArgumentParser(
            description='DealGenie Property Scoring CLI v1.2',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Single template scoring (v1 compatibility)
  python cli/multi_template_cli.py --apn 5529-025-001 --template retail
  
  # Multi-template scoring (new v2 functionality)  
  python cli/multi_template_cli.py --apn 5529-025-001 --multi-template
  
  # Output formats
  python cli/multi_template_cli.py --apn 5529-025-001 --multi-template --format html
  python cli/multi_template_cli.py --apn 5529-025-001 --multi-template --format json
  
  # Database operations
  python cli/multi_template_cli.py --apn 5529-025-001 --retrieve
  python cli/multi_template_cli.py --stats
            """
        )
        
        # Input options
        parser.add_argument('--apn', required=True, help='Property APN to score')
        parser.add_argument('--template', choices=['retail', 'office', 'multifamily', 'residential', 'commercial', 'industrial'], 
                          help='Single template to score (v1 compatibility)')
        parser.add_argument('--multi-template', action='store_true',
                          help='Enable multi-template scoring (v2 functionality)')
        parser.add_argument('--force-multi', action='store_true',
                          help='Force multi-template even if triggers not met')
        
        # Property features (optional - can use defaults)
        features = parser.add_argument_group('Property Features (optional)')
        features.add_argument('--zoning', default='R1', help='Property zoning code')
        features.add_argument('--lot-size', type=int, default=7500, help='Lot size in sq ft')
        features.add_argument('--transit-score', type=int, default=50, help='Transit accessibility score')
        features.add_argument('--population-density', type=int, default=6000, help='Population density per sq mile')
        features.add_argument('--median-income', type=int, default=65000, help='Area median income')
        features.add_argument('--price-per-sqft', type=int, default=550, help='Price per square foot')
        features.add_argument('--crime-factor', type=float, default=1.0, help='Crime factor (1.0 = average)')
        
        # Output options
        parser.add_argument('--format', choices=['json', 'html', 'summary'], default='summary',
                          help='Output format')
        parser.add_argument('--output', help='Output file path (default: stdout)')
        parser.add_argument('--pretty', action='store_true', help='Pretty print JSON output')
        
        # Database options
        parser.add_argument('--store', action='store_true', help='Store result in database')
        parser.add_argument('--retrieve', action='store_true', help='Retrieve existing result from database')
        parser.add_argument('--stats', action='store_true', help='Show database statistics')
        
        # Debug options
        parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
        parser.add_argument('--debug', action='store_true', help='Debug logging')
        
        return parser
    
    def setup_logging(self, verbose: bool = False, debug: bool = False):
        """Setup logging configuration"""
        if debug:
            level = logging.DEBUG
        elif verbose:
            level = logging.INFO
        else:
            level = logging.WARNING
            
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def build_features_dict(self, args: argparse.Namespace) -> Dict[str, Any]:
        """Build features dictionary from command line arguments"""
        return {
            'apn': args.apn,
            'zoning': args.zoning,
            'lot_size_sqft': args.lot_size,
            'transit_score': args.transit_score,
            'population_density': args.population_density,
            'median_income': args.median_income,
            'price_per_sqft': args.price_per_sqft,
            'crime_factor': args.crime_factor,
            'flood_risk': False  # Default
        }
    
    def score_single_template(self, features: Dict[str, Any], template: str) -> Dict[str, Any]:
        """Score using single template (v1 compatibility)"""
        result = calculate_score(features, template)
        
        # Convert to v2-like format for consistency
        return {
            'version': '1.1',
            'parcel_id': features['apn'],
            'scoring_method': 'single_template',
            'template': template,
            'score': result['score'],
            'explanation': result.get('explanation', ''),
            'recommendations': result.get('recommendations', []),
            'component_scores': result.get('component_scores', {}),
            'timestamp': result.get('timestamp', '')
        }
    
    def score_multi_template(
        self, 
        features: Dict[str, Any], 
        force_multi: bool = False
    ) -> Dict[str, Any]:
        """Score using multi-template logic (v2 functionality)"""
        scoring_result = score_parcel_multi_template(features, force_multi)
        formatted_result = format_multi_template_result(scoring_result)
        return formatted_result
    
    def format_summary_output(self, result: Dict[str, Any]) -> str:
        """Format result as human-readable summary"""
        lines = []
        lines.append(f"DealGenie Property Analysis")
        lines.append("=" * 40)
        lines.append(f"Property APN: {result.get('parcel_id', 'Unknown')}")
        lines.append(f"Scoring Method: {result.get('scoring_method', 'Unknown')}")
        lines.append(f"Analysis Version: {result.get('version', 'Unknown')}")
        lines.append("")
        
        # Single template result
        if result.get('scoring_method') == 'single_template':
            lines.append(f"Template: {result.get('template')}")
            lines.append(f"Score: {result.get('score', 0):.1f}/10")
            lines.append(f"Explanation: {result.get('explanation', '')}")
        
        # Multi-template result
        else:
            primary = result.get('primary_recommendation')
            if primary:
                lines.append(f"PRIMARY RECOMMENDATION:")
                lines.append(f"  Template: {primary['template']}")
                lines.append(f"  Score: {primary['score']:.1f}/10")
                lines.append(f"  Confidence: {primary.get('confidence', 0):.3f}")
                lines.append(f"  Reasoning: {primary.get('reasoning', '')}")
                lines.append("")
            
            # Alternative templates
            viable_uses = result.get('viable_uses', [])
            if len(viable_uses) > 1:
                lines.append("ALSO VIABLE:")
                for use in viable_uses[1:]:  # Skip primary (first)
                    lines.append(f"  {use['template']}: {use['score']:.1f}/10 (confidence: {use.get('confidence', 0):.3f})")
                lines.append("")
            
            # Key factors
            if primary and primary.get('risk_factors'):
                lines.append("RISK FACTORS:")
                for risk in primary['risk_factors']:
                    lines.append(f"  • {risk}")
                lines.append("")
            
            if primary and primary.get('opportunities'):
                lines.append("OPPORTUNITIES:")
                for opp in primary['opportunities']:
                    lines.append(f"  • {opp}")
        
        return "\n".join(lines)
    
    def format_html_output(self, result: Dict[str, Any]) -> str:
        """Format result as HTML"""
        html_parts = []
        html_parts.append('<!DOCTYPE html>')
        html_parts.append('<html><head>')
        html_parts.append('<title>DealGenie Property Analysis</title>')
        html_parts.append('<style>')
        html_parts.append('body { font-family: Arial, sans-serif; margin: 20px; }')
        html_parts.append('.primary { background: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }')
        html_parts.append('.alternatives { background: #f0f8ff; padding: 15px; margin: 10px 0; border-radius: 5px; }')
        html_parts.append('.score { font-size: 1.2em; font-weight: bold; color: #2e7d32; }')
        html_parts.append('</style>')
        html_parts.append('</head><body>')
        
        html_parts.append(f'<h1>DealGenie Property Analysis</h1>')
        html_parts.append(f'<p><strong>Property APN:</strong> {result.get("parcel_id", "Unknown")}</p>')
        html_parts.append(f'<p><strong>Analysis Version:</strong> {result.get("version", "Unknown")}</p>')
        
        # Primary recommendation
        primary = result.get('primary_recommendation')
        if primary:
            html_parts.append('<div class="primary">')
            html_parts.append('<h2>Primary Recommendation</h2>')
            html_parts.append(f'<p class="score">{primary["template"].title()}: {primary["score"]:.1f}/10</p>')
            html_parts.append(f'<p><strong>Confidence:</strong> {primary.get("confidence", 0):.1%}</p>')
            html_parts.append(f'<p>{primary.get("reasoning", "")}</p>')
            html_parts.append('</div>')
        
        # Also viable alternatives
        viable_uses = result.get('viable_uses', [])
        if len(viable_uses) > 1:
            html_parts.append('<div class="alternatives">')
            html_parts.append('<h2>Also Viable</h2>')
            html_parts.append('<ul>')
            for use in viable_uses[1:]:  # Skip primary
                html_parts.append(f'<li><strong>{use["template"].title()}:</strong> {use["score"]:.1f}/10 '
                                f'(confidence: {use.get("confidence", 0):.1%})</li>')
            html_parts.append('</ul>')
            html_parts.append('</div>')
        
        html_parts.append('</body></html>')
        return '\n'.join(html_parts)
    
    def output_result(self, result: Dict[str, Any], format_type: str, output_path: Optional[str] = None, pretty: bool = False):
        """Output result in specified format"""
        if format_type == 'json':
            output = json.dumps(result, indent=2 if pretty else None)
        elif format_type == 'html':
            output = self.format_html_output(result)
        else:  # summary
            output = self.format_summary_output(result)
        
        if output_path:
            with open(output_path, 'w') as f:
                f.write(output)
            print(f"Output written to {output_path}")
        else:
            print(output)
    
    def run(self, args: argparse.Namespace):
        """Main CLI execution"""
        # Database-only operations
        if args.stats:
            from database.multi_template_db import MultiTemplateDB
            db = MultiTemplateDB()
            try:
                stats = db.get_stats()
                print(f"Multi-Template Database Statistics:")
                print(f"  Total results: {stats.get('total_results', 0)}")
                print(f"  Database available: {stats.get('database_available', False)}")
                if stats.get('version_distribution'):
                    print(f"  Version distribution: {stats['version_distribution']}")
                if stats.get('scoring_method_distribution'):
                    print(f"  Scoring methods: {stats['scoring_method_distribution']}")
            finally:
                db.close()
            return
        
        if args.retrieve:
            result = get_multi_template_result(args.apn)
            if result:
                self.output_result(result, args.format, args.output, args.pretty)
            else:
                print(f"No stored result found for APN {args.apn}")
            return
        
        # Build features
        features = self.build_features_dict(args)
        
        # Score property
        if args.multi_template and self.feature_flags['MULTI_TEMPLATE_ENABLED']:
            print(f"Scoring APN {args.apn} using multi-template analysis...")
            result = self.score_multi_template(features, args.force_multi)
        elif args.template:
            print(f"Scoring APN {args.apn} using {args.template} template...")
            result = self.score_single_template(features, args.template)
        else:
            print("Error: Must specify either --template or --multi-template")
            return 1
        
        # Store in database if requested
        if args.store and self.feature_flags['DATABASE_STORAGE_ENABLED']:
            if store_multi_template_result(args.apn, result):
                print(f"Result stored in database for APN {args.apn}")
            else:
                print(f"Failed to store result in database for APN {args.apn}")
        
        # Output result
        self.output_result(result, args.format, args.output, args.pretty)
        return 0

def main():
    """Main entry point"""
    cli = MultiTemplateCLI()
    parser = cli.create_parser()
    args = parser.parse_args()
    
    cli.setup_logging(args.verbose, args.debug)
    
    try:
        exit_code = cli.run(args)
        sys.exit(exit_code or 0)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        if args.debug:
            import traceback
            traceback.print_exc()
        else:
            print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()