"""Turning pipeline values into report cells, and cells into markup.

The presentation half of the report split: `report_io` reads the tool output and
writes the files, this renders what goes between. Two kinds of thing live here,
and both are deliberately ignorant of which service produced the value:

  value formatters   number / percent / fraction_as_percent, each rendering an
                     absent value as MISSING rather than as a zero.
  markup builders    table / kv_table / support_bar and friends, which escape
                     their own text and hand back a fragment.

A renderer that has to know one service's data shape -- which JSON keys an RGI
hit uses, how PubMLST nests its linked-data values -- is not general and stays
with the panel that reads it.

This is a leaf module: it imports nothing from the pipeline, so any script run
as a Snakemake `script:` can import it without dragging in the workflow package.
"""

import html

# A value the upstream output did not carry. Distinct from a zero: rendering a
# missing identity as "0.0%" states that the aligner found no similarity, which
# is a claim about the call rather than an admission that we do not have it.
MISSING = "—"


def escape_html(value):
	"""Escape a value for the HTML report. The report is served under a CSP that
	blocks scripts outright, but this is what keeps tool-derived text -- contig
	deflines, gene names -- from breaking the markup around it."""
	return html.escape(str(value), quote=True)


def number(value, decimals=0):
	"""Thousands-separated, the way BV-BRC prints counts in its own report."""
	if value is None or value == "":
		return MISSING
	try:
		return f"{float(value):,.{decimals}f}"
	except (TypeError, ValueError):
		return escape_html(value)


def percent(value, decimals=2):
	if value is None or value == "":
		return MISSING
	try:
		return f"{float(value):.{decimals}f}%"
	except (TypeError, ValueError):
		return escape_html(value)


def fraction_as_percent(value, decimals=1):
	"""mefinder reports identity and coverage as fractions (0.919), not percents."""
	if value is None or value == "":
		return MISSING
	try:
		return f"{float(value) * 100:.{decimals}f}%"
	except (TypeError, ValueError):
		return escape_html(value)


def float_or_none(value):
	try:
		return round(float(value), 2)
	except (TypeError, ValueError):
		return None


def empty(message):
	return f'<p class="empty">{escape_html(message)}</p>'


def table(headers, rows, caption=None, table_class=""):
	"""One table. rows is a list of lists of already-escaped HTML cells."""
	if not rows:
		return empty("No data available.")
	parts = [f'<div class="scroll"><table class="{table_class}">']
	if caption:
		parts.append(f"<caption>{escape_html(caption)}</caption>")
	parts.append("<thead><tr>")
	parts.extend(f"<th>{escape_html(header)}</th>" for header in headers)
	parts.append("</tr></thead><tbody>")
	for row in rows:
		parts.append("<tr>")
		parts.extend(f"<td>{cell}</td>" for cell in row)
		parts.append("</tr>")
	parts.append("</tbody></table></div>")
	return "".join(parts)


def generic_table(rows, caption=None):
	"""Fallback for a CSV whose columns aren't the ones a panel expects -- a
	result directory written by an older version of the pipeline, say. The panels
	name their columns deliberately (that is the point: they mirror the
	service's own), but a name that isn't there must degrade to showing the data
	as it stands, not to a column of "None" per field we hoped for.
	"""
	if not rows:
		return empty("No data available.")
	headers = list(rows[0].keys())
	return table(
		headers,
		[[escape_html(row.get(header, "")) for header in headers] for row in rows],
		caption=caption,
	)


def kv_table(pairs, caption=None):
	"""BV-BRC's kv-table: bold key column, value column, no cell grid."""
	if not pairs:
		return empty("No data available.")
	parts = ['<div class="scroll"><table class="kv">']
	if caption:
		parts.append(f"<caption>{escape_html(caption)}</caption>")
	parts.append("<tbody>")
	for key, value in pairs:
		parts.append(f'<tr><th scope="row">{escape_html(key)}</th><td>{value}</td></tr>')
	parts.append("</tbody></table></div>")
	return "".join(parts)


def support_bar(value):
	"""PubMLST draws the Predicted-taxa support column as a filled bar, not a
	bare number; mirror it so a 100% call reads at a glance the way it does on
	the hosted rMLST results page."""
	if value is None or value == "":
		return MISSING
	try:
		pct = max(0.0, min(100.0, float(value)))
	except (TypeError, ValueError):
		return escape_html(value)
	return (
		f'<span class="support-bar"><span class="support-fill" style="width:{pct:.0f}%"></span>'
		f'<span class="support-label">{pct:.0f}%</span></span>'
	)


def seq_box(sequence):
	"""A long nucleotide/amino-acid string in a scrollable monospace box."""
	if not sequence:
		return MISSING
	return f'<div class="seqbox">{escape_html(sequence)}</div>'
