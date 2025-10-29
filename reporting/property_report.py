"""
Property Report Generator

Generates HTML reports for development opportunities including:
- Property details
- Envelope parameters
- Risk analysis (Monte Carlo) - placeholder card

Uses Jinja2 templates for rendering.
"""

import psycopg2
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from config.settings import get_settings


def get_mc_snapshot(apn: str, template: str) -> Optional[Dict[str, Any]]:
    """
    Get latest Monte Carlo input snapshot for a parcel.

    Args:
        apn: Assessor Parcel Number
        template: Development template

    Returns:
        Dictionary with snapshot data or None if not found
    """
    settings = get_settings()

    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        cur = conn.cursor()

        cur.execute("""
            SELECT apn, template, inputs_hash, inputs_json, sim_version, created_at
            FROM sim_inputs_snapshot
            WHERE apn = %s AND template = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """, (apn, template))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        return {
            'apn': row[0],
            'template': row[1],
            'inputs_hash': row[2],
            'inputs_json': row[3],
            'sim_version': row[4],
            'created_at': row[5]
        }
    except Exception as e:
        print(f"Warning: Could not fetch MC snapshot: {e}")
        return None


def render_risk_card(apn: str, template: str, mc_data: Optional[Dict[str, Any]]) -> str:
    """
    Render the Monte Carlo risk card HTML.

    Args:
        apn: Assessor Parcel Number
        template: Development template
        mc_data: Monte Carlo snapshot data or None

    Returns:
        HTML string for the risk card
    """
    try:
        from jinja2 import Template
    except ImportError:
        return """
        <div style="color: red; padding: 20px; border: 1px solid red;">
            Error: jinja2 not installed. Run: pip install jinja2
        </div>
        """

    # Load template
    template_path = Path(__file__).parent / 'components' / 'risk_card_mc.html'

    try:
        with open(template_path, 'r') as f:
            template_content = f.read()
    except FileNotFoundError:
        return f"""
        <div style="color: red; padding: 20px; border: 1px solid red;">
            Error: Template not found at {template_path}
        </div>
        """

    # Render template
    template = Template(template_content)
    html = template.render(
        apn=apn,
        template=template,
        mc_data=mc_data
    )

    return html


def generate_property_report(
    apn: str,
    template: str,
    property_data: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate full HTML property report.

    Args:
        apn: Assessor Parcel Number
        template: Development template
        property_data: Optional property details (envelope, zoning, etc.)

    Returns:
        Complete HTML report as string

    Example:
        >>> html = generate_property_report('2249003017', 'multifamily', {})
        >>> print('Risk card included:', 'Risk Analysis (Monte Carlo)' in html)
        Risk card included: True
    """
    # Get MC snapshot data
    mc_data = get_mc_snapshot(apn, template)

    # Render risk card
    risk_card_html = render_risk_card(apn, template, mc_data)

    # Build full report
    # For now, just a simple HTML structure with the risk card
    # Future: Add property details, envelope parameters, etc.
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Property Report - APN {apn}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 900px;
            margin: 40px auto;
            padding: 20px;
            background: #fafafa;
        }}
        .report-container {{
            background: white;
            border-radius: 8px;
            padding: 40px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .report-header {{
            margin-bottom: 40px;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 20px;
        }}
        .report-header h1 {{
            margin: 0 0 8px 0;
            color: #333;
            font-size: 28px;
        }}
        .report-header .meta {{
            color: #666;
            font-size: 14px;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .section h2 {{
            color: #444;
            font-size: 20px;
            margin: 0 0 16px 0;
        }}
    </style>
</head>
<body>
    <div class="report-container">
        <div class="report-header">
            <h1>Development Opportunity Report</h1>
            <div class="meta">
                APN: <strong>{apn}</strong> |
                Template: <strong>{template}</strong> |
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
            </div>
        </div>

        <!-- Property Details Section (placeholder) -->
        <div class="section">
            <h2>📋 Property Details</h2>
            <p style="color: #999; font-style: italic;">
                Property details will appear here when envelope data is integrated.
            </p>
        </div>

        <!-- Monte Carlo Risk Card -->
        <div class="section">
            {risk_card_html}
        </div>

        <!-- Future sections -->
        <div class="section">
            <h2>📍 Location & Zoning</h2>
            <p style="color: #999; font-style: italic;">
                Coming soon: Interactive map, zoning details, nearby comparables
            </p>
        </div>
    </div>
</body>
</html>
    """

    return html
