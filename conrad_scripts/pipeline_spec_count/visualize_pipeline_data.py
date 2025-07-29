#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

# Color palette
PRIMARY = '#4CA6B8'
PRIMARY_LIGHT = '#EFFBFF'
ACCENT = '#0E2D40'
ACCENT_MEDIUM = '#486B80'
ACCENT_LIGHT = '#96BBD1'

def load_data(filename='pipeline_spec_analysis.json'):
    """Load the pipeline analysis data from JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)

def calculate_statistics(data):
    """Calculate summary statistics from the pipeline data."""
    pipeline_files = data['pipeline_files']

    # Filter out files with errors
    valid_files = [f for f in pipeline_files if 'error' not in f]

    total_pipelines = len(valid_files)
    step_counts = [f['pipeline_steps'] for f in valid_files]

    total_steps = sum(step_counts)
    avg_steps = total_steps / total_pipelines if total_pipelines > 0 else 0
    max_steps = max(step_counts) if step_counts else 0

    return {
        'total_pipelines': total_pipelines,
        'total_steps': total_steps,
        'avg_steps': avg_steps,
        'max_steps': max_steps
    }

def group_by_month(data):
    """Group pipeline data by month."""
    pipeline_files = data['pipeline_files']
    valid_files = [f for f in pipeline_files if 'error' not in f]

    monthly_data = defaultdict(list)

    for file_info in valid_files:
        # Parse the creation date
        creation_date = datetime.fromisoformat(file_info['creation_date'].replace('Z', '+00:00'))
        month_key = creation_date.strftime('%Y-%m')
        monthly_data[month_key].append(file_info['pipeline_steps'])

    return monthly_data

def create_statistics_boxes(ax, stats):
    """Create visually appealing statistics boxes."""
    # Define box positions and dimensions
    box_width = 0.22
    box_height = 0.80
    y_pos = 0.15
    x_positions = [0.05, 0.28, 0.51, 0.74]

    stats_data = [
        ('Total Pipelines', f"{stats['total_pipelines']:,}", PRIMARY),
        ('Total Steps', f"{stats['total_steps']:,}", ACCENT_MEDIUM),
        ('Avg Steps', f"{stats['avg_steps']:.1f}", PRIMARY),
        ('Max Steps', f"{stats['max_steps']:,}", ACCENT)
    ]

    for i, (label, value, color) in enumerate(stats_data):
        x_pos = x_positions[i]

        # Create background box
        box = plt.Rectangle((x_pos, y_pos), box_width, box_height,
                           facecolor=color, alpha=0.1, edgecolor=color, linewidth=2)
        ax.add_patch(box)

        # Add value text (large)
        ax.text(x_pos + box_width/2, y_pos + box_height*0.65, value,
                ha='center', va='center', fontsize=18, fontweight='bold', color=color)

        # Add label text (smaller)
        ax.text(x_pos + box_width/2, y_pos + box_height*0.25, label,
                ha='center', va='center', fontsize=11, color=ACCENT)

def create_monthly_charts(monthly_data, stats):
    """Create charts showing pipelines per month and average steps per month with statistics."""
    # Sort months chronologically
    sorted_months = sorted(monthly_data.keys())

    # Calculate data for charts
    months = []
    pipeline_counts = []
    avg_steps_per_month = []

    for month in sorted_months:
        months.append(datetime.strptime(month, '%Y-%m'))
        pipeline_counts.append(len(monthly_data[month]))
        avg_steps_per_month.append(np.mean(monthly_data[month]) if monthly_data[month] else 0)

    # Create figure with custom layout
    fig = plt.figure(figsize=(14, 10))

    # Title
    fig.suptitle('Laminar Pipelines', fontsize=20, fontweight='bold',
                 color=ACCENT, y=0.95)

    # Statistics area (top)
    ax_stats = plt.subplot2grid((10, 1), (0, 0), rowspan=2)
    ax_stats.set_xlim(0, 1)
    ax_stats.set_ylim(0, 1)
    ax_stats.axis('off')
    create_statistics_boxes(ax_stats, stats)

    # Chart 1: Pipelines per month
    ax1 = plt.subplot2grid((10, 1), (3, 0), rowspan=3)
    ax1.bar(months, pipeline_counts, color=PRIMARY, alpha=0.8, edgecolor=ACCENT, linewidth=1)
    ax1.set_title('Number of Pipelines Created Per Month', fontsize=14, color=ACCENT, pad=20)
    ax1.set_ylabel('Number of Pipelines', fontsize=12, color=ACCENT)
    ax1.grid(True, alpha=0.3, color=ACCENT_LIGHT)
    ax1.set_facecolor(PRIMARY_LIGHT)

    # Format x-axis for first chart - quarterly labels
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # Chart 2: Average steps per month
    ax2 = plt.subplot2grid((10, 1), (7, 0), rowspan=3)
    ax2.plot(months, avg_steps_per_month, color=PRIMARY, marker='o', linewidth=3,
             markersize=6, markerfacecolor=ACCENT, markeredgecolor=PRIMARY)
    ax2.fill_between(months, avg_steps_per_month, alpha=0.3, color=PRIMARY)
    ax2.set_title('Average Pipeline Steps Per Month', fontsize=14, color=ACCENT, pad=20)
    ax2.set_xlabel('Month', fontsize=12, color=ACCENT)
    ax2.set_ylabel('Average Steps', fontsize=12, color=ACCENT)
    ax2.grid(True, alpha=0.3, color=ACCENT_LIGHT)
    ax2.set_facecolor(PRIMARY_LIGHT)

    # Format x-axis for second chart - quarterly labels
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    # Adjust layout
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)

    return fig


def main():
    """Main function to generate the PDF report."""
    # Load data
    print("Loading pipeline analysis data...")
    data = load_data()

    # Calculate statistics
    print("Calculating statistics...")
    stats = calculate_statistics(data)

    # Group data by month
    print("Grouping data by month...")
    monthly_data = group_by_month(data)

    # Create charts
    print("Creating charts...")
    fig = create_monthly_charts(monthly_data, stats)

    # Generate PDF
    output_filename = 'laminar_pipeline_analysis.pdf'
    print(f"Generating PDF report: {output_filename}")

    with PdfPages(output_filename) as pdf:
        # Single page with statistics and charts
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)

    print(f"Report saved as: {output_filename}")
    print("\nSummary:")
    print(f"  - Total pipelines: {stats['total_pipelines']:,}")
    print(f"  - Average steps: {stats['avg_steps']:.2f}")
    print(f"  - Max steps: {stats['max_steps']:,}")

if __name__ == '__main__':
    main()
