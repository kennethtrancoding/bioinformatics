#!/bin/bash
# Render a per-sample HTML report from results you already have, and open it.
#
#   ./preview-report.sh                       # richest sample under results/, opened
#   ./preview-report.sh results/JOB/SAMPLE    # that one
#   ./preview-report.sh --list                # what is available, ranked
#   ./preview-report.sh --in-place            # overwrite the pipeline's report.html
#   ./preview-report.sh --no-open             # just write it
#
# This calls generate_sample_report.py directly rather than going through
# Snakemake, which on a stale or partial tree would try to rebuild every upstream
# rule -- re-fetching raw reads that the upload already released to S3. The report
# rule and this path read the same inputs, so what you get here is what the
# pipeline would write.
#
# Output defaults to summary/report.preview.html, not summary/report.html: the
# latter is the pipeline's own output, and a preview built from a tree that
# predates a rule's current inputs is not the same artifact. Pass --in-place when
# you do mean to replace it.
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RESULTS_DIR="${RESULTS_DIR:-results}"
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"
REPORT_SCRIPT="workflow/scripts/generate_sample_report.py"

# The inputs the report rule declares. Used only to rank candidates: the script
# itself tolerates every one of them being absent and renders the gaps as
# em-dashes, so a thin tree still produces a report -- just a quieter one.
inputs_for() {
	local dir="$1" sample
	sample="$(basename "$dir")"
	printf '%s\n' \
		"$dir/02_assembly/genome_metrics.csv" \
		"$dir/03_resistance/novelty_report.txt" \
		"$dir/03_resistance/rgi_results.csv" \
		"$dir/03_resistance/rgi_results.json" \
		"$dir/04_blast/blast_results.csv" \
		"$dir/05_mlst/mlst_results.json" \
		"$dir/05_mlst/rmlst_raw.json" \
		"$dir/06_mobile_elements/me_summary.csv" \
		"$dir/06_mobile_elements/$sample.csv" \
		"$dir/06_mobile_elements/${sample}_arg_mge_colocation.json" \
		"$dir/06_mobile_elements/${sample}_arg_mge_colocation.csv"
}

score_of() {
	local dir="$1" found=0 path
	while IFS= read -r path; do
		[ -f "$path" ] && found=$((found + 1))
	done < <(inputs_for "$dir")
	printf '%s' "$found"
}

# Every results/<JOB>/<SAMPLE> that has at least one declared input, richest
# first. Ranking by input count rather than by mtime because "the data I already
# have" is rarely uniform: an interrupted job leaves sample directories holding
# anything from one file to all eleven, and the fullest one is the one worth
# looking at.
list_candidates() {
	local dir score
	for dir in "$RESULTS_DIR"/*/*/; do
		[ -d "$dir" ] || continue
		dir="${dir%/}"
		score="$(score_of "$dir")"
		[ "$score" -gt 0 ] && printf '%s\t%s\n' "$score" "$dir"
	done | sort -rn -k1,1
}

SAMPLE_DIR=""
DO_OPEN=1
IN_PLACE=0
for arg in "$@"; do
	case "$arg" in
	--list | -l)
		printf 'inputs  sample\n'
		candidates="$(list_candidates || true)"
		if [ -z "$candidates" ]; then
			echo "(none -- no results/<JOB>/<SAMPLE> holds any report input)" >&2
			exit 1
		fi
		printf '%s\n' "$candidates" | while IFS=$'\t' read -r score dir; do
			printf '%2s/11   %s\n' "$score" "$dir"
		done
		exit 0
		;;
	--no-open) DO_OPEN=0 ;;
	--in-place) IN_PLACE=1 ;;
	-h | --help)
		sed -n '2,8p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	-*)
		echo "unknown option: $arg" >&2
		exit 2
		;;
	*) SAMPLE_DIR="${arg%/}" ;;
	esac
done

if [ -z "$SAMPLE_DIR" ]; then
	best="$(list_candidates | head -1 || true)"
	if [ -z "$best" ]; then
		echo "No renderable sample found under $RESULTS_DIR/." >&2
		echo "Expected results/<JOB_ID>/<SAMPLE>/ holding at least one report input." >&2
		exit 1
	fi
	SAMPLE_DIR="$(printf '%s' "$best" | cut -f2)"
fi

if [ ! -d "$SAMPLE_DIR" ]; then
	echo "Not a directory: $SAMPLE_DIR" >&2
	exit 1
fi

if [ "$IN_PLACE" -eq 1 ]; then
	OUT="$SAMPLE_DIR/summary/report.html"
else
	OUT="$SAMPLE_DIR/summary/report.preview.html"
fi

echo "Sample:  $SAMPLE_DIR  ($(score_of "$SAMPLE_DIR")/11 inputs present)"
"$PYTHON" "$REPORT_SCRIPT" "$SAMPLE_DIR" -o "$OUT"

# Name the gaps rather than letting them read as findings: an absent input is a
# blank panel, which looks identical to a panel whose tool genuinely found
# nothing. Only the missing ones are worth printing.
missing=""
while IFS= read -r path; do
	[ -f "$path" ] || missing="$missing  ${path#"$SAMPLE_DIR"/}"$'\n'
done < <(inputs_for "$SAMPLE_DIR")
if [ -n "$missing" ]; then
	echo "Not in this tree, so those panels render empty:"
	printf '%s' "$missing"
fi

if [ "$DO_OPEN" -eq 1 ]; then
	if command -v open >/dev/null 2>&1; then
		open "$OUT"
	elif command -v xdg-open >/dev/null 2>&1; then
		xdg-open "$OUT"
	else
		echo "No opener found; the report is at: $OUT"
	fi
fi
