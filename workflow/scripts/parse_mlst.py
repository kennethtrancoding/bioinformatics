"""
Parse MLST output and resolve species (Snakemake rule: mlst_analysis).

Two-part species logic:
  1. The `mlst` CLI only reports a SCHEME + ST. Several schemes (e.g. 'ecloacae',
     'cfreundii') cover a whole species *complex* and cannot resolve the species
     from the scheme name alone, so the scheme-based label is only a fallback.
  2. For the authoritative species we query PubMLST's rMLST Species-ID API (53
     ribosomal genes) with the assembly — the same engine native PubMLST uses.
     This distinguishes e.g. Enterobacter mori from E. cloacae within the ECC.

If PubMLST is unreachable or returns no confident species call, we fall back to
the scheme-based (complex) label. This step never fails the pipeline.

Usage: parse_mlst.py <mlst_results.txt> <mlst_json_out> <sample_id> <assembly_fasta> <rmlst_raw_out>
"""

import sys
import json
import base64
import urllib.request
from pathlib import Path

# An mlst SCHEME is not a species. Map known schemes to a label, marking
# species-complex schemes explicitly (they need rMLST for species-level ID).
SCHEME_TO_SPECIES = {
    "ecloacae": "Enterobacter cloacae complex",
    "ecloacae_2": "Enterobacter cloacae complex",
    "cfreundii": "Citrobacter freundii complex",
    "kpneumoniae": "Klebsiella pneumoniae",
    "saureus": "Staphylococcus aureus",
    "paeruginosa": "Pseudomonas aeruginosa",
    "ecoli_achtman_4": "Escherichia coli",
}

RMLST_URL = "https://rest.pubmlst.org/db/pubmlst_rmlst_seqdef_kiosk/schemes/1/sequence"
# Minimum support (%) to trust an rMLST species call.
MIN_SUPPORT = 70.0


def rmlst_species(assembly_fasta, raw_output_path):
    """Return every SPECIES-rank candidate from PubMLST rMLST, sorted by support
    descending (highest-confidence match first), or [] on failure or if the top
    match doesn't meet MIN_SUPPORT.

    Always writes the full API response (or an {"error": ...} placeholder on
    failure) to raw_out_path -- the caller gets every species candidate here
    too, but the full response also carries allele match detail that's
    otherwise discarded once this function returns.
    """
    try:
        with open(assembly_fasta, "rb") as file_handle:
            assembly_fasta_bytes = file_handle.read()
        payload = json.dumps(
            {"base64": True, "details": True,
             "sequence": base64.b64encode(assembly_fasta_bytes).decode()}
        ).encode()
        request_object = urllib.request.Request(
            RMLST_URL, data=payload, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request_object, timeout=180) as response:
            rmlst_result = json.load(response)
        with open(raw_output_path, "w") as file_handle:
            json.dump(rmlst_result, file_handle, indent=2)
        species_matches = []
        for taxon in rmlst_result.get("taxon_prediction", []) or []:
            if str(taxon.get("rank", "")).upper() != "SPECIES":
                continue
            taxon_name = taxon.get("taxon")
            if not taxon_name:
                continue
            species_matches.append({"species": taxon_name, "support": float(taxon.get("support") or 0)})
        species_matches.sort(key=lambda species_match: species_match["support"], reverse=True)
        if species_matches and species_matches[0]["support"] >= MIN_SUPPORT:
            return species_matches
    except Exception as exception:  # network/parse errors must not fail the pipeline
        print(f"rMLST species lookup failed ({exception}); using scheme-based label",
              file=sys.stderr)
        with open(raw_output_path, "w") as file_handle:
            json.dump({"error": str(exception)}, file_handle, indent=2)
    return []


def main():
    mlst_txt, json_out, sample_id, assembly_fasta, rmlst_raw_out = sys.argv[1:6]

    with open(mlst_txt) as file_handle:
        mlst_lines = [mlst_line for mlst_line in file_handle.read().splitlines() if mlst_line.strip()]
    # The real result line is tab-separated; fall back to the last non-empty line.
    result_line = next((mlst_line for mlst_line in mlst_lines if "\t" in mlst_line), mlst_lines[-1] if mlst_lines else "")
    result_columns = result_line.split("\t")

    mlst_results = {"sample": sample_id}
    if len(result_columns) >= 3:
        scheme = result_columns[1]
        scheme_label = SCHEME_TO_SPECIES.get(scheme, scheme)
        mlst_results.update(
            filename=result_columns[0],
            scheme=scheme,
            st=result_columns[2],
            alleles=result_columns[3:],
            scheme_species=scheme_label,
        )
        # Authoritative species from rMLST, with scheme label as fallback.
        # rMLST can return several candidate species (ambiguous within a
        # complex) -- keep all of them, not just the top hit.
        species_matches = rmlst_species(assembly_fasta, rmlst_raw_out)
        if species_matches:
            mlst_results["species"] = species_matches[0]["species"]
            mlst_results["species_method"] = "rMLST"
            mlst_results["species_support"] = species_matches[0]["support"]
            mlst_results["species_matches"] = species_matches
        else:
            mlst_results["species"] = scheme_label
            mlst_results["species_method"] = "mlst_scheme"

    # rmlst_raw_out is a declared Snakemake output, so it must always exist --
    # rmlst_species() only runs (and writes it) when mlst produced a usable
    # result line above.
    if not Path(rmlst_raw_out).exists():
        with open(rmlst_raw_out, "w") as file_handle:
            json.dump({"error": "mlst produced no usable result line; rMLST was not queried"}, file_handle, indent=2)

    with open(json_out, "w") as file_handle:
        json.dump(mlst_results, file_handle, indent=2)

    print(f"✓ MLST parsed: {mlst_results.get('species', 'N/A')} "
          f"(scheme={mlst_results.get('scheme', 'N/A')}, ST={mlst_results.get('st', 'N/A')}, "
          f"via {mlst_results.get('species_method', 'N/A')})")


if __name__ == "__main__":
    main()
