"""
Generate Sample Report Script (Snakemake rule: generate_sample_report)
Generate HTML report for individual sample
"""

import csv
import html
import json
from pathlib import Path

from rgi_json_to_csv import escape_html

report_file = snakemake.output.html_report
metrics_file = snakemake.input.assembly_metrics
card_file = snakemake.input.card
blast_file = snakemake.input.blast
mlst_file = snakemake.input.mlst
mobile_element_finder_file = snakemake.input.mobile_element_finder

Path(report_file).parent.mkdir(parents=True, exist_ok=True)


def json_to_html_table(rgi_data):
	if not rgi_data:
		return "<table></table>"
	column_names = list(next(iter(rgi_data.values())).keys())

	html = "<table>\n"

	# header
	html += "  <tr><th></th>"
	for column_name in column_names:
		html += f"<th>{escape_html(column_name)}</th>"
	html += "</tr>\n"

	# rows
	for output_row, row_values in rgi_data.items():
		html += f"  <tr><th>{escape_html(output_row)}</th>"
		for column_name in column_names:
			html += f"<td>{escape_html(row_values.get(column_name, ''))}</td>"
		html += "</tr>\n"

	html += "</table>"
	return html


def _first_csv_row(input_path):
	if not Path(input_path).exists():
		return {}
	with open(input_path, newline="") as file_handle:
		return next(csv.DictReader(file_handle), {})


def _csv_to_html_table(input_path):
	if not Path(input_path).exists():
		return "<p>No data available.</p>"
	with open(input_path, newline="") as file_handle:
		table_rows = {
			row_index: csv_row for row_index, csv_row in enumerate(csv.DictReader(file_handle))
		}
	if not table_rows:
		return "<p>No data available.</p>"
	return json_to_html_table(table_rows)


def _novelty_to_html_table(input_path):
	"""Parse the tab-separated hit table out of evaluate_novelty.py's text report."""
	if not Path(input_path).exists():
		return "<p>No data available.</p>"
	with open(input_path) as file_handle:
		report_lines = file_handle.read().splitlines()
	header = "Gene\tAntibiotic\tIdentity_pct\tCoverage_pct\tNovel_flag\tNotes"
	if header not in report_lines:
		return "<pre>" + html.escape("\n".join(report_lines)) + "</pre>"
	columns = header.split("\t")
	data_rows = [
		report_line
		for report_line in report_lines[report_lines.index(header) + 1 :]
		if report_line.strip()
	]
	if not data_rows:
		return "<p>No resistance gene hits.</p>"
	table_rows = {
		row_index: dict(zip(columns, report_line.split("\t")))
		for row_index, report_line in enumerate(data_rows)
	}
	return json_to_html_table(table_rows)


def _mef_to_html_table(input_path):
	"""Render the MobileElementFinder per-type counts from me_summary.csv."""
	if not input_path or not Path(input_path).exists():
		return "<p>Mobile-element analysis not available.</p>"
	with open(input_path, newline="") as file_handle:
		table_rows = {
			row_index: csv_row for row_index, csv_row in enumerate(csv.DictReader(file_handle))
		}
	if not table_rows:
		return "<p>No mobile genetic elements detected.</p>"
	return json_to_html_table(table_rows)


def _mlst_to_html_table(input_path):
	if not Path(input_path).exists():
		return "<p>No data available.</p>"
	with open(input_path) as file_handle:
		json_data = json.load(file_handle)
	if not json_data:
		return "<p>No data available.</p>"
	return json_to_html_table({0: json_data})


assembly_metrics = _first_csv_row(metrics_file)
mlst_result_data = {}
if Path(mlst_file).exists():
	with open(mlst_file) as file_handle:
		mlst_result_data = json.load(file_handle)

summary_row = dict(assembly_metrics)
summary_row["species"] = mlst_result_data.get("species", "N/A")
summary_row["sequence_type"] = mlst_result_data.get("st", "N/A")
summary_table = json_to_html_table({0: summary_row}) if summary_row else "<p>No data available.</p>"

html_content = f"""
<!DOCTYPE html>
<html>
<head>
<style>
.tabs {{
	width: 600px;
  font-family: Arial, sans-serif;
}}

.tabs input[type="radio"] {{
	display: none;
}}

.labels label {{
	display: inline-block;
  padding: 10px 15px;
  background: #ddd;
  cursor: pointer;
  border: 1px solid #ccc;
}}

.content {{
	display: none;
  padding: 20px;
}}

table tr td {{
  border: 1px solid #000;
}}

#summary:checked ~ .contents #summary-content,
#bvbrc:checked ~ .contents #bvbrc-content,
#blast:checked ~ .contents #blast-content,
#mlst:checked ~ .contents #mlst-content,
#mef:checked ~ .contents #mef-content,
#card:checked ~ .contents #card-content {{
	display: block;
}}

#summary:checked ~ .labels label[for="summary"],
#bvbrc:checked ~ .labels label[for="bvbrc"],
#blast:checked ~ .labels label[for="blast"],
#mlst:checked ~ .labels label[for="mlst"],
#mef:checked ~ .labels label[for="mef"],
#card:checked ~ .labels label[for="card"] {{
	background: white;
  border-bottom: 1px solid white;
}}
</style>
</head>

<body>

<div class="tabs">

  <input type="radio" id="summary" name="tabs" checked>
  <input type="radio" id="bvbrc" name="tabs">
  <input type="radio" id="blast" name="tabs">
  <input type="radio" id="mlst" name="tabs">
  <input type="radio" id="mef" name="tabs">
  <input type="radio" id="card" name="tabs">

  <div class="labels">
    <label for="summary">Summary</label>
    <label for="bvbrc">BV-BRC</label>
    <label for="blast">BLAST</label>
    <label for="mlst">MLST</label>
    <label for="mef">MEF</label>
    <label for="card">CARD</label>
  </div>

  <div class="contents">

    <div id="summary-content" class="content">
      <h2>Summary</h2>
      {summary_table}
    </div>

    <div id="bvbrc-content" class="content">
      <h2>BV-BRC</h2>
      {_csv_to_html_table(metrics_file)}
    </div>

    <div id="blast-content" class="content">
      <h2>BLAST</h2>
      {_csv_to_html_table(blast_file)}
    </div>

    <div id="mlst-content" class="content">
      <h2>MLST</h2>
      {_mlst_to_html_table(mlst_file)}
    </div>

    <div id="mef-content" class="content">
      <h2>Mobile Genetic Elements</h2>
      {_mef_to_html_table(mobile_element_finder_file)}
    </div>

    <div id="card-content" class="content">
      <h2>CARD</h2>
      {_novelty_to_html_table(card_file)}
    </div>

  </div>

</div>

</body>
</html>
"""

with open(report_file, "w") as file_handle:
	file_handle.write(html_content)

print(f"✓ Sample report generated: {report_file}")
