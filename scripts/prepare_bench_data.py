#!/usr/bin/env python3
import argparse
import gzip
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

DEFAULT_INPUT = "data/raw/uniprot_sprot_human.xml.gz"
DEFAULT_OUTPUT = "data/bench/bench_small.xml.gz"
DEFAULT_SIDECAR = "data/bench/bench_sidecar.fasta.gz"
DEFAULT_TARGET = 1_000


def extract_entries(input_path: Path, output_path: Path, target: int) -> tuple[int, bool]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    truncated = False
    saw_uniprot_close = False

    def open_output(path: Path):
        if path.suffix == ".gz":
            return gzip.open(path, "wt", encoding="utf-8")
        return path.open("w", encoding="utf-8")

    with gzip.open(input_path, "rt", encoding="utf-8") as f_in, open_output(
        output_path
    ) as f_out:
        for line in f_in:
            f_out.write(line)

            if "</uniprot>" in line:
                saw_uniprot_close = True

            if "</entry>" in line:
                count += line.count("</entry>")
                if count >= target:
                    truncated = True
                    break

        if truncated and not saw_uniprot_close:
            f_out.write("\n</uniprot>\n")

    return count, truncated


def open_xml_input(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def build_sidecar(xml_path: Path, sidecar_path: Path) -> int:
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    written = 0

    def open_output(path: Path):
        if path.suffix == ".gz":
            return gzip.open(path, "wt", encoding="utf-8")
        return path.open("w", encoding="utf-8")

    with open_xml_input(xml_path) as f_in, open_output(sidecar_path) as f_out:
        context = ET.iterparse(f_in, events=("end",))
        for _, elem in context:
            if strip_ns(elem.tag) != "entry":
                continue

            seq_elem = None
            for child in elem:
                if strip_ns(child.tag) == "sequence":
                    seq_elem = child
                    break

            if seq_elem is None:
                elem.clear()
                continue

            seq = "".join(seq_elem.itertext()).replace(" ", "").replace("\n", "").strip()
            if not seq:
                elem.clear()
                continue

            for isoform in elem.iter():
                if strip_ns(isoform.tag) != "isoform":
                    continue
                iso_id = None
                for child in isoform:
                    if strip_ns(child.tag) == "id" and child.text:
                        iso_id = child.text.strip()
                        break
                if not iso_id or iso_id in seen:
                    continue

                f_out.write(f">{iso_id}\n")
                for i in range(0, len(seq), 60):
                    f_out.write(seq[i : i + 60] + "\n")
                seen.add(iso_id)
                written += 1

            elem.clear()

    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a small UniProt XML fixture for benchmarks."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to .xml.gz input")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to output XML file")
    parser.add_argument(
        "--entries", type=int, default=DEFAULT_TARGET, help="Number of entries to extract"
    )
    parser.add_argument(
        "--sidecar",
        default=DEFAULT_SIDECAR,
        help="Output FASTA sidecar path (for isoform sequences)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    print(f"Extracting {args.entries} entries from {input_path}...")
    count, truncated = extract_entries(input_path, output_path, args.entries)

    if count < args.entries:
        print(f"Warning: only extracted {count} entries (file ended early).")

    if truncated:
        print(f"Done! Wrote {count} entries to {output_path}.")
    else:
        print(f"Done! Copied full file to {output_path}.")

    if args.sidecar:
        sidecar_path = Path(args.sidecar)
        print(f"Building sidecar FASTA at {sidecar_path}...")
        written = build_sidecar(output_path, sidecar_path)
        print(f"Sidecar entries written: {written}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
